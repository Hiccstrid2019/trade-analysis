
from datetime import datetime, timedelta

import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS

from DAL.AnalyticRepository import AnalyticRepository
from DAL.ConfigRepository import ConfigRepository
from exporter.Repository import Repository
from scraper.TelegramParser import TelegramParser
from tasks import get_task_status, stop_task, save_tg_posts, save_yt_videos, save_data_by_source

app = Flask(__name__)
cors = CORS(app)


sources = [
    {'moex': 'Котировки акций с Московской биржи'},
    {'spbex': 'Котировки акций с СПб биржи'},
    {'cbr': 'Курсы валют ЦБ'},
    {'finmarket': 'Публикации с портала Finmarket'}
]

tasks = []


@app.route('/tasks', methods=['POST', 'GET'])
def run_task():
    if request.method == 'POST':
        source = request.json['source']
        title = request.json['title']
        print(source)
        if source == 'telegram':
            task = save_tg_posts.delay()
        elif source == 'youtube':
            task = save_yt_videos.delay()
        else:
            start_period = datetime.fromisoformat(request.json['startPeriod'][:-1]).date() + timedelta(days=2)
            print(start_period)
            end_period = datetime.fromisoformat(request.json['endPeriod'][:-1]).date()
            task = save_data_by_source.delay(source, start_period, end_period)
            tasks.append({"id": task.id, "title": title, "start": start_period.strftime("%Y.%m.%d"),
                          "end": end_period.strftime("%Y.%m.%d")})

        return jsonify({"id": task.id, "title": title})
    elif request.method == 'GET':
        return jsonify(tasks)


@app.route('/tasks/<task_id>', methods=['GET', 'DELETE'])
def get_status(task_id):
    if request.method == 'GET':
        task_result = get_task_status(task_id)
        return jsonify({"id": task_id, "status": task_result.status})
    elif request.method == 'DELETE':
        stop_task(task_id)
        return jsonify({"id": task_id})


@app.route('/period', methods=['GET'])
def get_period():
    args = request.args
    source = args.get('source')
    repository = Repository()
    start, end = repository.get_data_range(source)
    return jsonify({'start': start.strftime("%Y.%m.%d"), 'end': end.strftime("%Y.%m.%d")})


@app.route('/periods', methods=['GET'])
def get_all_periods():
    args = request.args
    ischannels = args.get('channels', type=bool, default=False)

    repository = Repository()
    data = []

    if ischannels:
        config_repository = ConfigRepository()
        channels_info = config_repository.get_all_tg_channels()

        tg_channels = []
        for channel in channels_info:
            start, end = repository.get_data_range_for_tg(channel[1])
            if start is not None:
                tg_channels.append({"id": channel[0], "name": channel[2],
                         'period': f'{start.strftime("%d.%m.%Y")} - {end.strftime("%d.%m.%Y")}'})
        channels_info = config_repository.get_all_youtube_channels()
        yt_channels = []
        for channel in channels_info:
            start, end = repository.get_data_range_for_yt(channel[1])
            if start is not None:
                yt_channels.append({"id": channel[0], "name": channel[2],
                                    'period': f'{start.strftime("%d.%m.%Y")} - {end.strftime("%d.%m.%Y")}'})
        return jsonify({"telegram": tg_channels, "youtube": yt_channels})
    else:
        for source in sources:
            start, end = repository.get_data_range(list(source)[0])
            data.append({"id": sources.index(source), "name": source[list(source)[0]], 'period': f'{start.strftime("%d.%m.%Y")} - {end.strftime("%d.%m.%Y")}'})

        return jsonify(data)


@app.route('/channels', methods=['GET', 'POST'])
def tg_channels():
    repository = ConfigRepository()
    if request.method == 'GET':
        channels = repository.get_all_tg_channels()
        data = []
        for channel in channels:
            data.append({"id": channel[0], "username": channel[1], "name": channel[2], "subscribers": channel[3]})
        return jsonify(data)
    elif request.method == 'POST':
        new_channel = request.json['link']
        if 't.me' in new_channel:
            new_channel = new_channel.split('/')[-1]

        parser = TelegramParser()

        chat = parser.get_info(new_channel)
        print(chat)
        if chat is not None:
            id = repository.add_tg_channel(chat.username, chat.title, chat.members_count, chat.photo.small_file_id)
            return jsonify({"id": id, "username": chat.username, "name": chat.title, "subscribers": chat.members_count})
        else:
            return jsonify({"error": "Телеграм канал с таким username-ом не найден"}), 404


@app.route('/stat', methods=['GET'])
def stat():
    args = request.args
    year = args.get('year', type=int, default=datetime.now().year)

    source = args.get('source')
    print(source)

    if source == 'moex':
        df = pd.read_csv(f'./moex/{year}.csv', sep=';')
        df['date'] = pd.to_datetime(df['TRADEDATE'])
        df = df[df['date'].dt.year == year]
        aggregate = df['date'].groupby(df['date']).agg('count')
    # elif source in sources:
    #     with open(f'./tg/{source}.json', 'r', encoding='utf-8') as f:
    #         data = json.loads(re.sub(r',\s*\]', ']', f.read()))
    #     df = pd.json_normalize(data)
    #     df['date'] = pd.to_datetime(df['date'])
    #     aggregate = df[((df['text'].notnull()) | (df['caption'].notnull())) & (df['date'].dt.year == year)].sort_values('date')['date'].groupby(df['date'].dt.date).agg('count')
    #     aggregate.index = pd.to_datetime(aggregate.index)
    else:
        return {'error': 'source not found'}

    if aggregate.empty:
        return {'error': 'year not found'}

    df = pd.DataFrame({"date": aggregate.index.strftime('%Y-%m-%d'), "count": aggregate.values})
    data = {
        'source': source,
        'data': df.to_dict(orient='records'),
        'min': int(df['count'].min()),
        'max': int(df['count'].max())
    }
    return jsonify(data)


@app.route('/stocks', methods=['GET'])
def stocks():
    args = request.args
    id_company = args.get('id', type=int)
    id_market = args.get('market', type=int)
    start = args.get('start')
    end = args.get('end')

    repository = AnalyticRepository()
    data = repository.get_company_data(id_company, id_market, start, end)

    return jsonify(data)


@app.route('/exchanges', methods=['GET'])
def companies():
    repository = AnalyticRepository()
    companies_moex = repository.get_all_company_exchange(1)
    companies_spbex = repository.get_all_company_exchange(2)
    return jsonify({
        'exchanges': [
            {'name': 'Московская биржа', 'value': 'moex', 'companies': companies_moex},
            {'name': 'СПб биржа', 'value': 'spbex', 'companies': companies_spbex}
        ]
    })


@app.route('/corr', methods=['GET'])
def corr():
    args = request.args
    id_company = args.get('id', type=int)
    id_market = args.get('market', type=int)
    date = args.get('date')

    repository = AnalyticRepository()
    data = repository.get_prices(id_company, id_market, date)

    return jsonify({"date": date, "corr": data})


@app.route('/posts', methods=['GET'])
def get_posts():
    args = request.args
    id_company = args.get('id', type=int)
    id_market = args.get('market', type=int)
    date = args.get('date')

    repository = AnalyticRepository()
    data = repository.get_posts_mention_company(id_company, id_market, date)

    return jsonify(data)


# def get_post(post_id, chat_id):
#     df = pd.read_csv('msg.csv', sep=';')
#     return df[(df['id'] == post_id) & (df['chat.id'] == chat_id)][['chat.title', 'text', 'date']].to_dict('records')[0]
#
#
# @app.route('/posts', methods=['POST'])
# def posts():
#     if request.method == 'POST':
#         data = request.json['data']
#         posts = []
#         for d in data:
#             post = get_post(d['id'], d['chat.id'])
#             post['id'] = d['id']
#             post['chat.id'] = d['chat.id']
#             posts.append(post)
#         return jsonify(posts)


if __name__ == '__main__':
    app.run(debug=True)
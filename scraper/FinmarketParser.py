import csv
from datetime import date, timedelta
from pathlib import Path

from bs4 import BeautifulSoup

from scraper.base.Parser import Parser


class FinmarketParser(Parser):
    def get_day_news(self, url: str):
        html = self.get_html(url, encoding='windows-1251')
        bs = BeautifulSoup(html, 'lxml')
        news_block = bs.find('div', class_="news_content")
        date_published = news_block.find('span').text
        title = news_block.find('h1').text
        body = news_block.find('div', class_='body')
        if body is None or not body.text:
            text = ''
            announce = news_block.find('div', class_='announce')
            if not announce:
                text = ' '.join(news_block.find('h1').find_next_sibling('div').text.split())
            else:
                try_text = announce.find_next_sibling('div')
                if try_text.text:
                    text = ' '.join(try_text.text.split())
        else:
            text = ' '.join(body.text.split())
            announce = news_block.find('div', class_='announce')

        if announce:
            announce = ' '.join(announce.text.split())
        else:
            announce = ' '
        news = {
            'date': date_published,
            'title': title,
            'announce': announce,
            'text': text
        }
        return news

    def save_for_period(self, start_date: date, end_date: date):
        # filename = f'finam-{start_date.strftime("%Y.%m.%d")}-{end_date.strftime("%Y.%m.%d")}.csv'
        data = []
        print(end_date-start_date > timedelta(days=31))
        if end_date - start_date > timedelta(days=31):
            delta = timedelta(days=31)
            while start_date <= end_date:
                url = f"http://www.finmarket.ru/analytics/?srch=&nt=0&df={start_date.strftime('%d.%m.%Y')}&dt={(start_date + delta).strftime('%d.%m.%Y')}"
                html = self.get_html(url)
                bs = BeautifulSoup(html, 'lxml')
                links = bs.find('div', class_='center_column').find_all('div', class_='title')
                total_pages = int(links[-1].text.split()[-1])
                for p in range(2, total_pages + 1):
                    for link in links[:-1]:
                        print("http://www.finmarket.ru" + link.find('a').get('href'))
                        news = self.get_day_news("http://www.finmarket.ru" + link.find('a').get('href'))
                        data.append(news)
                    url = f"http://www.finmarket.ru/analytics/?srch=&nt=0&df={start_date.strftime('%d.%m.%Y')}&dt={(start_date + delta).strftime('%d.%m.%Y')}&pg={p}"
                    html = self.get_html(url)
                    bs = BeautifulSoup(html, 'lxml')
                    links = bs.find('div', class_='center_column').find_all('div', class_='title')
                print(f"{start_date.strftime('%Y-%m-%d')} completed")
                start_date += delta
        else:
            url = f"http://www.finmarket.ru/analytics/?srch=&nt=0&df={start_date.strftime('%d.%m.%Y')}&dt={end_date.strftime('%d.%m.%Y')}"
            print(url)
            html = self.get_html(url)
            bs = BeautifulSoup(html, 'lxml')
            links = bs.find('div', class_='center_column').find_all('div', class_='title')
            total_pages = int(links[-1].text.split()[-1])
            for p in range(2, total_pages + 1):
                for link in links[:-1]:
                    print("http://www.finmarket.ru" + link.find('a').get('href'))
                    news = self.get_day_news("http://www.finmarket.ru" + link.find('a').get('href'))
                    data.append(news)
                url = f"http://www.finmarket.ru/analytics/?srch=&nt=0&df={start_date.strftime('%d.%m.%Y')}&dt={end_date.strftime('%d.%m.%Y')}&pg={p}"
                html = self.get_html(url)
                bs = BeautifulSoup(html, 'lxml')
                links = bs.find('div', class_='center_column').find_all('div', class_='title')
            print(f"{start_date.strftime('%Y-%m-%d')} completed")

        Path("./data/finmarket").mkdir(parents=True, exist_ok=True)
        with open('./data/finmarket/finmarket.csv', "w", encoding='utf8', newline='') as file:
            dict_writer = csv.DictWriter(file, data[0].keys(), delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(data)


if __name__ == '__main__':
    # history(04.09.2003, 25.04.2023)
    pass

import csv
import json
import re
from datetime import timedelta, date
from pathlib import Path

from scraper.base.Parser import Parser


class MoExParser(Parser):
    def __init__(self):
        super().__init__()
        self.url = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/boardgroups/57/securities.jsonp'

    def __get_total_count(self, day: date) -> int:
        params = {
            'iss.meta': 'off',
            'iss.json': 'extended',
            'lang': 'ru',
            'security_collection': '3',
            'date': day.strftime("%Y-%m-%d"),
            'start': 0,
            'limit': 1,
            'sort_column': 'VALUE',
            'sort_order': 'desc',
        }
        result = self.session.get(self.url, params=params).text
        total_count_str = re.search(r'"TOTAL":\s\d{1,4}', result).group()
        total_count = int(re.search(r'\d{1,4}', total_count_str).group())
        return total_count

    def __get_stock_day(self, day: date) -> list:
        total_count = self.__get_total_count(day)
        day_list = []
        for offset in range(0, total_count, 100):
            list_history = self.__get_json(day, offset)
            day_list.extend(list_history)
        return day_list

    def __get_json(self, day: date, offset: int) -> str:
        params = {
            'iss.meta': 'off',
            'iss.json': 'extended',
            'lang': 'ru',
            'security_collection': '3',
            'date': day.strftime("%Y-%m-%d"),
            'start': offset,
            'limit': offset + 100,
            'sort_column': 'VALUE',
            'sort_order': 'desc',
        }
        response = self.session.get(self.url, params=params)
        json_response = json.loads(response.text)
        return json_response[1]['history']

    def get_data(self, start_date: date, end_date: date) -> list[dict]:
        data = []
        delta = timedelta(days=1)
        while start_date <= end_date:
            day = self.__get_stock_day(start_date)
            data.extend(day)
            print(f"{start_date.strftime('%Y-%m-%d')} completed")
            start_date += delta
        return data

    def save_for_period(self, start_date: date, end_date: date):
        data = self.get_data(start_date, end_date)
        # with open(f'{start_date.strftime("%Y.%m.%d")}-{end_date.strftime("%Y.%m.%d")}.csv', 'w', encoding='utf8',
        #           newline='') as file:
        Path("./data/moex").mkdir(parents=True, exist_ok=True)
        with open(f'./data/moex/moex.csv', 'w', encoding='utf8', newline='') as file:
            dict_writer = csv.DictWriter(file, data[0].keys(), delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerows(data)


if __name__ == '__main__':
    mp = MoExParser()
    mp.get_data(date(2022,1,1), date(2022, 1, 4))
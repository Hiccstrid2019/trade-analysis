import csv
import locale
from datetime import date, timedelta
from pathlib import Path

from bs4 import BeautifulSoup

from scraper.base.Parser import Parser


class CBRParser(Parser):
    def save_daily_currency(self, day: date) -> list[dict]:
        daily = []
        date_format = day.strftime("%d.%m.%Y")
        url = f"https://cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={date_format}"
        html = self.get_html(url)
        bs = BeautifulSoup(html, 'lxml')
        table = bs.find("table", {"class": "data"})
        rows = table.find_all('tr')
        for row in rows[1:]:
            values = row.find_all('td')
            currency = {
                "date": day.strftime("%Y-%m-%d"),
                "num_code": values[0].text,
                "let_code": values[1].text,
                "units": int(values[2].text),
                "name": values[3].text,
                "currency": locale.atof(values[4].text.replace(' ', ''))
            }
            daily.append(currency)
        return daily

    def save_for_period(self, start_date: date, end_date: date):
        delta = timedelta(days=1)
        # with open(f'../data/cbr/currencyCBR-{start_date.strftime("%Y.%m.%d")}-{end_date.strftime("%Y.%m.%d")}.csv',
        # "a", encoding='utf8', newline='') as file:
        print(start_date)
        Path("./data/cbr").mkdir(parents=True, exist_ok=True)
        with open('./data/cbr/currency.csv', "w", encoding='utf8', newline='') as file:
            data = self.save_daily_currency(start_date)
            dict_writer = csv.DictWriter(file, data[0].keys(), delimiter=';')
            dict_writer.writeheader()

            while start_date <= end_date:
                data = self.save_daily_currency(start_date)
                dict_writer.writerows(data)
                print(f"{start_date.strftime('%Y-%m-%d')} completed")
                start_date += delta

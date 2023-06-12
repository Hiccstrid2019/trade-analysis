import csv

from exporter.Exporter import Exporter


class SpbexExporter(Exporter):
    def export(self):
        command = "INSERT INTO spbex (date, short_name, full_name, isin, ticker, low, high, average, currency) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
        with open('./data/spbex/spbex.csv', newline='', encoding='utf8') as file:
            reader = csv.DictReader(file, delimiter=';')
            for row in reader:
                short_name = row['Краткое наименование ценной бумаги']
                full_name = row['Наименование эмитента']
                isin = row['ISIN']
                ticker = row['Код ценной бумаги']
                low = row['(Осн. сессия) Минимальная цена, в валюте цены'].replace(',', '.').replace(' ', '')
                high = row['(Осн. сессия) Максимальная цена, в валюте цены'].replace(',', '.').replace(' ', '')
                average = row['(Осн. сессия) Средневзвешен ная цена, в валюте цены (ставка репо в %)'] \
                    .replace(',', '.').replace(' ', '')
                currency = row['Валюта цены']
                if ticker:
                    self.cursor.execute(command, (row['date'], short_name, full_name, isin,
                                                  ticker, low or None, high or None, average or None, currency))
        self.connection.commit()


if __name__ == '__main__':
    mx = SpbexExporter()
    mx.export()

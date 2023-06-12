import csv

from exporter.Exporter import Exporter


class MoexExporter(Exporter):
    def export(self):
        command = "INSERT INTO moex (date, short_name, ticker, open, low, high, close, average) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"#" ON CONFLICT DO NOTHING"
        with open('./data/moex/moex.csv', newline='', encoding='utf8') as file:
            reader = csv.DictReader(file, delimiter=';')
            for row in reader:
                self.cursor.execute(command, (row['TRADEDATE'], row['SHORTNAME'], row['SECID'],
                                              row['OPEN'] or None, row['LOW'] or None, row['HIGH'] or None,
                                              row['LEGALCLOSEPRICE'] or None, row['WAPRICE'] or None))
        self.connection.commit()


if __name__ == '__main__':
    mx = MoexExporter()
    mx.export()
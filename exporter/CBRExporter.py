import csv

from exporter.Exporter import Exporter


class CBRExporter(Exporter):
    def export(self):
        command = "INSERT INTO cbr_currency (date, num_code, letter_code, units, name, currency) " \
                  "VALUES (%s, %s, %s, %s, %s, %s)"
        with open('./data/cbr/currency.csv', newline='', encoding='utf8') as file:
            reader = csv.DictReader(file, delimiter=';')
            for row in reader:
                try:
                    num_code = int(row['num_code'])
                    units = int(row['units'])
                    currency = float(row['currency'])
                    self.cursor.execute(command, (row['date'], num_code, row['let_code'],
                                                  units, row['name'], currency))
                except ValueError:
                    print("Bad row: ", row)
        self.connection.commit()


if __name__ == '__main__':
    cbre = CBRExporter()
    cbre.export()
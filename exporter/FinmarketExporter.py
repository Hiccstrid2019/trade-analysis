import csv
from exporter.Exporter import Exporter
from datetime import datetime
import locale
locale.setlocale(
    category=locale.LC_ALL,
    locale="Russian"
)


months = {
    'января': 'январь',
    'февраля': 'февраль',
    'марта': 'март',
    'апреля': 'апрель',
    'мая': 'май',
    'июня': 'июнь',
    'июля': 'июль',
    'августа': 'август',
    'сентября': 'сентябрь',
    'октября': 'октябрь',
    'ноября': 'ноябрь',
    'декабря': 'декабрь',
}


class FinmarketExporter(Exporter):
    def export(self):
        command = "INSERT INTO finmarket (timestamp, title, announce, text) " \
                  "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
        with open('./data/finmarket/finmarket.csv', newline='', encoding='utf8') as file:
            reader = csv.DictReader(file, delimiter=';')
            for row in reader:
                date = row['date'].split()
                timestamp = datetime.strptime(f"{date[0]} {months[date[1]]} {date[2]} {date[4]}", "%d %B %Y %H:%M")
                self.cursor.execute(command, (timestamp, row['title'], row['announce'] or None, row['text']))
        self.connection.commit()


if __name__ == '__main__':
    mx = FinmarketExporter()
    mx.export()

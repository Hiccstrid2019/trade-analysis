import csv
from exporter.Exporter import Exporter
from datetime import datetime
import locale
locale.setlocale(
    category=locale.LC_ALL,
    locale="Russian"
)


class YouTubeExporter(Exporter):
    def export(self, channels: list):
        command = "INSERT INTO youtube (date, username, channel, views, caption, pinned_comment, url) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        for username in channels:
            with open(f"./data/youtube/{username}.csv", newline='', encoding='utf8') as file:
                reader = csv.DictReader(file, delimiter=';')
                for row in reader:
                    if 'мая' in row['date']:
                        date = datetime.strptime(row['date'].replace('мая', 'май'), "%d %b %Y г.").date()
                    else:
                        if 'февр.' in row['date']:
                            row['date'] = row['date'].replace('февр.', 'фев.')
                        elif 'нояб.' in row['date']:
                            row['date'] = row['date'].replace('нояб.', 'ноя.')
                        elif 'сент.' in row['date']:
                            row['date'] = row['date'].replace('сент.', 'сен.')
                        date = datetime.strptime(row['date'], "%d %b. %Y г.").date()
                    views = int(''.join(row['views'].split()[:-1]))
                    if username == '':
                        channel = 'Smirnova Capital'
                    else:
                        channel = 'Finversia'

                    self.cursor.execute(command, (date, username, channel, views,
                                                  row['description'], row['pinned_comment'] or None,
                                                  row['url']))
            self.connection.commit()


if __name__ == '__main__':
    mx = YouTubeExporter()
    mx.export()

import json
import re
from os import walk

from exporter.Exporter import Exporter


class TelegramExporter(Exporter):
    def export(self, channels: list):
        command = "INSERT INTO telegram VALUES (DEFAULT, %s)"
        for channel in channels:
            with open(f'./data/tg/{channel}.json', 'r', encoding='utf-8') as f:
                data = json.loads(re.sub(r',\s*\]', ']', f.read()))
                for row in data:
                    self.cursor.execute(command, (json.dumps(row),))
            self.connection.commit()

        # filenames = next(walk("../data/history_tg"), (None, None, []))[2]
        # for file in filenames:
        #     with open(f'../data/history_tg/{file}', 'r', encoding='utf-8') as f:
        #         data = json.loads(re.sub(r',\s*\]', ']', f.read()))
        #         for row in data:
        #             self.cursor.execute(command, (json.dumps(row),))
        #     self.connection.commit()


if __name__ == '__main__':
    mx = TelegramExporter()
    mx.export()
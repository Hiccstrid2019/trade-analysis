from datetime import datetime
from transformer.Transformer import Transformer


class CurrencyTransformer(Transformer):

    def transform(self):
        command = "SELECT letter_code, date, name, units, currency, added FROM rowdata.public.cbr_currency " \
                  "WHERE added > %s"
        last_updated = self._get_datetime_updated('currency_updated')
        self.raw_cursor.execute(command, (last_updated,))
        data = self.raw_cursor.fetchall()
        print(len(data))
        command = "INSERT INTO analytics.public.currency (letter_code, date, name, units, currency) " \
                  "VALUES (%s, %s, %s, %s, %s)"
        for row in data:
            self.analytic_cursor.execute(command, (row['letter_code'], row['date'], row['name'],
                                                   row['units'], row['currency']))
        self.analytic_connection.commit()
        self._set_datetime_updated('currency_updated', data[0]['added'])


if __name__ == '__main__':
    mx = CurrencyTransformer()
    mx.transform()
    
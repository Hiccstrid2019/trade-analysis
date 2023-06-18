from transformer.Transformer import Transformer
from datetime import date


class StockTransformer(Transformer):
    def _add_ticker(self, market_id: int, company_id: int, ticker: str):
        command = "INSERT INTO analytics.public.ticker (ticker, id_company, id_stock_market) " \
                  "VALUES (%s, %s, %s)"
        self.analytic_cursor.execute(command, (ticker, company_id, market_id))
        self.analytic_connection.commit()

    def _add_company(self, market_id: int,  short_name: str, full_name: str) -> int:
        command = "INSERT INTO analytics.public.company (stock_market_id, short_name, full_name) " \
                  "VALUES (%s, %s, %s) RETURNING id"
        self.analytic_cursor.execute(command, (market_id, short_name, full_name))
        self.analytic_connection.commit()
        return self.analytic_cursor.fetchone()[0]

    def _get_ticker(self, ticker: str, id_stock_market: int):
        command = "SELECT ticker, id_company, id_stock_market FROM analytics.public.ticker " \
                  "WHERE ticker = %s AND id_stock_market = %s"
        self.analytic_cursor.execute(command, (ticker, id_stock_market))
        ticker = self.analytic_cursor.fetchone()
        return ticker

    def _get_currency(self, date: date, currency_code: str):
        command = "SELECT date, letter_code FROM analytics.public.currency WHERE date = %s AND letter_code = %s"
        self.analytic_cursor.execute(command, (date, currency_code))
        return self.analytic_cursor.fetchone()

    def _add_currency(self, date: date, currency_code: str, name: str = "Российский рубль"):
        command = "INSERT INTO analytics.public.currency (letter_code, date, name, units, currency) " \
                  "VALUES (%s, %s, %s, %s, %s)"
        self.analytic_cursor.execute(command, (currency_code, date, name, 1, 1))

    def transform_moex(self):
        command = "SELECT date, short_name, ticker, open, low, high, close, average, added FROM rowdata.public.moex " \
                  "WHERE added > %s"
        last_updated = self._get_datetime_updated('moex_updated')
        self.raw_cursor.execute(command, (last_updated, ))
        data = self.raw_cursor.fetchall()

        self.analytic_cursor.execute("SELECT id FROM analytics.public.stock_market WHERE name = 'Московская биржа'")
        market_id = self.analytic_cursor.fetchone()[0]

        command = "INSERT INTO analytics.public.stock " \
                  "(ticker, id_company, id_stock_market, date, code_currency, open, low, high, close, average) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        for row in data:
            ticker = self._get_ticker(row['ticker'], market_id)
            if ticker is None:
                short_name = row['short_name']
                company_id = self._add_company(market_id, short_name, short_name)
                ticker = row['ticker']
                self._add_ticker(market_id, company_id, ticker)
                ticker = self._get_ticker(ticker, market_id)
            currency = self._get_currency(row['date'], "RUB")
            print(ticker)
            print(currency)
            if currency is None:
                self._add_currency(row['date'], "RUB")
                currency = self._get_currency(row['date'], "RUB")
            self.analytic_cursor.execute(command, (ticker['ticker'], ticker['id_company'], ticker['id_stock_market'],
                                                   currency['date'], currency['letter_code'],
                                                   row['open'], row['low'], row['high'], row['close'], row['average']))
        self.analytic_connection.commit()
        self._set_datetime_updated('moex_updated', data[0]['added'])

    def transform_spbex(self):
        command = "SELECT date, short_name, full_name, ticker, low, high, average, currency, added FROM rowdata.public.spbex " \
                  "WHERE added > %s"
        last_updated = self._get_datetime_updated('spbex_updated')
        self.raw_cursor.execute(command, (last_updated,))
        data = self.raw_cursor.fetchall()

        command = "INSERT INTO analytics.public.stock " \
                  "(ticker, id_company, id_stock_market, date, code_currency, low, high, average) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

        self.analytic_cursor.execute("SELECT id FROM analytics.public.stock_market WHERE name = 'СПб биржа'")
        market_id = self.analytic_cursor.fetchone()[0]

        for row in data:
            ticker = self._get_ticker(row['ticker'], market_id)

            if ticker is None:
                short_name = row['short_name']
                full_name = row['full_name']
                company_id = self._add_company(market_id, short_name, full_name)
                ticker = row['ticker']
                self._add_ticker(market_id, company_id, ticker)
                ticker = self._get_ticker(ticker, market_id)
            currency = self._get_currency(row['date'], row['currency'])

            if currency is None:
                if row['currency'] == 'RUB':
                    self._add_currency(row['date'], "RUB")
                else:
                    self._add_currency(row['date'], row['currency'], name="Unknown")
                currency = self._get_currency(row['date'], row['currency'])
            print(ticker)
            print(currency)
            self.analytic_cursor.execute(command, (ticker['ticker'], ticker['id_company'], ticker['id_stock_market'],
                                                   currency['date'], currency['letter_code'],
                                                   row['low'], row['high'], row['average']))
        self.analytic_connection.commit()
        self._set_datetime_updated('spbex_updated', data[0]['added'])


if __name__ == '__main__':
    mx = StockTransformer()
    mx.transform_spbex()

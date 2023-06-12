from transformer.Transformer import Transformer
from datetime import date


class StockMoExTransformer(Transformer):
    def _add_ticker(self, market_id: int, company_id: int, ticker: str):
        command = "INSERT INTO analytics.public.ticker (ticker, id_company, id_stock_market) " \
                  "VALUES (%s, %s, %s)"
        self.analytic_cursor.execute(command, (ticker, company_id, market_id))
        self.analytic_connection.commit()

    def _add_company(self, market_id: int,  short_name: str) -> int:
        command = "INSERT INTO analytics.public.company (stock_market_id, short_name, full_name) " \
                  "VALUES (%s, %s, %s) RETURNING id"
        self.analytic_cursor.execute(command, (market_id, short_name, short_name))
        self.analytic_connection.commit()
        return self.analytic_cursor.fetchone()[0]

    def _get_ticker(self, ticker: str):
        command = "SELECT ticker, id_company, id_stock_market FROM analytics.public.ticker " \
                  "WHERE ticker = %s"
        self.analytic_cursor.execute(command, (ticker,))
        ticker = self.analytic_cursor.fetchone()
        return ticker

    def _get_currency(self, date: date, currency_code: str):
        command = "SELECT date, letter_code FROM analytics.public.currency WHERE date = %s AND letter_code = %s"
        self.analytic_cursor.execute(command, (date, currency_code))
        return self.analytic_cursor.fetchone()

    def _add_currency(self, date: date, currency_code: str):
        command = "INSERT INTO analytics.public.currency (letter_code, date, name, units, currency) " \
                  "VALUES (%s, %s, %s, %s, %s)"
        self.analytic_cursor.execute(command, (currency_code, date, "Российский рубль", 1, 1))

    def transform(self):
        command = "SELECT date, short_name, ticker, open, low, high, close, average FROM rowdata.public.moex"
        self.row_cursor.execute(command)
        data = self.row_cursor.fetchall()

        self.analytic_cursor.execute("SELECT id FROM analytics.public.stock_market WHERE name = 'Московская биржа'")
        market_id = self.analytic_cursor.fetchone()[0]

        command = "INSERT INTO analytics.public.stock " \
                  "(ticker, id_company, id_stock_market, date, code_currency, open, low, high, close, average) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        for row in data:
            ticker = self._get_ticker(row[2])
            print(ticker)
            if ticker is None:
                short_name = row[1]
                company_id = self._add_company(market_id, short_name)
                self._add_ticker(market_id, company_id, row[2])
                ticker = self._get_ticker(row[2])
            currency = self._get_currency(row[0], "RUB")
            print(currency)
            if currency is None:
                self._add_currency(row[0], "RUB")
                currency = self._get_currency(row[0], "RUB")
            self.analytic_cursor.execute(command, (ticker[0], ticker[1], ticker[2], currency[0], currency[1],
                                                   row[3], row[4], row[5], row[6], row[7]))
        self.analytic_connection.commit()


class StockSPbExTransformer(Transformer):
    def transform(self):
        command = "SELECT date, short_name, full_name, ticker, low, high, average, currency FROM rowdata.public.spbex"
        self.row_cursor.execute(command)
        data = self.row_cursor.fetchall()
        print(len(data))


if __name__ == '__main__':
    mx = StockSPbExTransformer()
    mx.transform()

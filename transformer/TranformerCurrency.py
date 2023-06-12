from transformer.Transformer import Transformer


class TransformerCurrency(Transformer):
    def transform(self):
        command = "SELECT letter_code, date, name, units, currency FROM rowdata.public.cbr_currency"
        self.row_cursor.execute(command)
        data = self.row_cursor.fetchall()
        command = "INSERT INTO analytics.public.currency (letter_code, date, name, units, currency) " \
                  "VALUES (%s, %s, %s, %s, %s)"
        for row in data:
            self.analytic_cursor.execute(command, row)
        self.analytic_connection.commit()


if __name__ == '__main__':
    mx = TransformerCurrency()
    mx.transform()
    
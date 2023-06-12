from transformer.Transformer import Transformer
from utils.TextLemmatizer import TextLemmatizer


class FinmarketTransformer(Transformer):
    def transform(self):
        command = "SELECT timestamp, title, text FROM rowdata.public.finmarket"
        self.row_cursor.execute(command)
        data = self.row_cursor.fetchall()

        lemmatizer = TextLemmatizer()

        command = "INSERT INTO analytics.public.publication_finmarket (date, title, text, norm_text) " \
                  "VALUES (%s, %s, %s, %s)"
        for row in data:
            norm_text = lemmatizer.lemmatize_text(row[2])
            self.analytic_cursor.execute(command, (row[0], row[1], row[2], norm_text))
        self.analytic_connection.commit()
        print("END")


if __name__ == '__main__':
    mx = FinmarketTransformer()
    mx.transform()

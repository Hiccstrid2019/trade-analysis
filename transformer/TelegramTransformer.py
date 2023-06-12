from transformer.Transformer import Transformer
from utils.TextLemmatizer import TextLemmatizer


class TelegramTransformer(Transformer):
    def transform(self):
        command = "SELECT publication->'chat'->>'username', publication->'chat'->>'title',  " \
                  "(publication->'chat'->>'id')::bigint, (publication->>'date')::timestamp," \
                  "CONCAT(publication->>'caption', ' ', publication->>'text') as text " \
                  "FROM rowdata.public.telegram " \
                  "WHERE publication->>'caption' is not NULL OR publication->>'text' IS NOT NULL"
        self.row_cursor.execute(command)
        data = self.row_cursor.fetchall()

        lemmatizer = TextLemmatizer()

        command = "INSERT INTO analytics.public.publication_tg (id_channel, datetime, text, norm_text)" \
                  "VALUES (%s, %s, %s, %s)"

        adding_tg_command = "INSERT INTO analytics.public.tg_channel (id, username, name) VALUES (%s, %s, %s)"
        for row in data:
            self.analytic_cursor.execute("SELECT id FROM analytics.public.tg_channel WHERE username = %s", (row[0],))
            id = self.analytic_cursor.fetchone()['id']
            if id is None:
                id = row[2]
                self.analytic_cursor.execute(adding_tg_command, (id, row[0], row[1]))
                self.analytic_connection.commit()
            norm_text = lemmatizer.lemmatize_text(row[4])
            self.analytic_cursor.execute(command, (id, row[3], row[4], norm_text))
        self.analytic_connection.commit()


            # norm_text = lemmatizer.lemmatize_text(row[2])
            # self.analytic_cursor.execute(command, (row[0], row[1], row[2], norm_text))
        # self.analytic_connection.commit()
        # print("END")


if __name__ == '__main__':
    mx = TelegramTransformer()
    mx.transform()

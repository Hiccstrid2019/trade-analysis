import re

from transformer.Transformer import Transformer
from utils.TextLemmatizer import TextLemmatizer


class TelegramTransformer(Transformer):
    def transform(self):
        query = "SELECT publication->'chat'->>'username' as username, publication->'chat'->>'title' as title,  " \
                "(publication->'chat'->>'id')::bigint as chat_id, (publication->>'date')::timestamp as timestamp," \
                "CONCAT(publication->>'caption', ' ', publication->>'text') as text, added " \
                "FROM rowdata.public.telegram " \
                "WHERE publication->>'caption' is not NULL OR publication->>'text' IS NOT NULL AND added > %s"
        last_updated = self._get_datetime_updated('telegram_updated')
        self.raw_cursor.execute(query, (last_updated,))
        data = self.raw_cursor.fetchall()

        lemmatizer = TextLemmatizer()
        companies = self._get_all_companies()

        insert_post = "INSERT INTO analytics.public.publication_tg (id_channel, datetime, text, norm_text)" \
                      "VALUES (%s, %s, %s, %s) RETURNING id"

        insert_reference = "INSERT INTO analytics.public.reference " \
                           "(ticker, id_company, id_stock_market, id_publication_tg) " \
                           "VALUES (%s, %s, %s, %s)"

        adding_tg_command = "INSERT INTO analytics.public.tg_channel (id, username, name) VALUES (%s, %s, %s)"
        for row in data:
            self.analytic_cursor.execute("SELECT id FROM analytics.public.tg_channel WHERE username = %s",
                                         (row['username'],))
            id = self.analytic_cursor.fetchone()['id']
            if id is None:
                id = row['chat_id']
                self.analytic_cursor.execute(adding_tg_command, (id, row['username'], row['title']))
                self.analytic_connection.commit()
            norm_text = lemmatizer.lemmatize_text(row['text'])
            self.analytic_cursor.execute(insert_post, (id, row['timestamp'], row['text'], norm_text))
            self.analytic_connection.commit()
            id_post = self.analytic_cursor.fetchone()[0]
            for company in companies:
                name = self.clean_name(company['short_name'])
                if company['ticker'].isdigit():
                    name = name.split()[0]
                    if len(name) < 3 or 'and' in name:
                        continue
                    if re.search(fr"\b{name}\b", norm_text, flags=re.IGNORECASE) is not None:
                        print(f"{name}: in {norm_text}")
                        self.analytic_cursor.execute(insert_reference, (company['ticker'], company['id_company'],
                                                                        company['id_stock_market'], id_post))
                else:
                    if len(name) < 3 or 'and' in name or len(company['ticker']) < 3:
                        continue
                    if re.search(fr"\b{name}\b", norm_text, flags=re.IGNORECASE) is not None \
                            or re.search(fr"\b{company['ticker']}\b", norm_text, flags=re.IGNORECASE) is not None:
                        print(f"{name}:{company['ticker']} in {norm_text}")
                        self.analytic_cursor.execute(insert_reference, (company['ticker'], company['id_company'],
                                                                        company['id_stock_market'], id_post))
        self.analytic_connection.commit()
        self._set_datetime_updated('telegram_updated', data[0]['added'])


if __name__ == '__main__':
    mx = TelegramTransformer()
    mx.transform()

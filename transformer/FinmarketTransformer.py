import re

from transformer.Transformer import Transformer
from utils.TextLemmatizer import TextLemmatizer


class FinmarketTransformer(Transformer):
    def transform(self):
        query = "SELECT timestamp, title, text, added FROM rowdata.public.finmarket WHERE added > %s"
        last_updated = self._get_datetime_updated('finmarket_updated')
        self.raw_cursor.execute(query, (last_updated,))
        data = self.raw_cursor.fetchall()

        lemmatizer = TextLemmatizer()

        companies = self._get_all_companies()

        insert_finmarket = "INSERT INTO analytics.public.publication_finmarket (date, title, text, norm_text) " \
                           "VALUES (%s, %s, %s, %s) RETURNING id"

        insert_reference = "INSERT INTO analytics.public.reference " \
                           "(ticker, id_company, id_stock_market, id_publiaction_finmarket) " \
                           "VALUES (%s, %s, %s, %s)"

        for row in data:
            norm_text = lemmatizer.lemmatize_text(row['text'])
            self.analytic_cursor.execute(insert_finmarket, (row['timestamp'], row['title'], row['text'], norm_text))
            self.analytic_connection.commit()
            id_finmarket = self.analytic_cursor.fetchone()[0]
            for company in companies:
                name = self.clean_name(company['short_name'])
                if company['ticker'].isdigit():
                    name = name.split()[0]
                    if len(name) < 3 or 'and' in name:
                        continue
                    if re.search(fr"\b{name}\b", norm_text, flags=re.IGNORECASE) is not None:
                        print(f"{name}: in {norm_text}")
                        self.analytic_cursor.execute(insert_reference, (company['ticker'], company['id_company'],
                                                                        company['id_stock_market'], id_finmarket))
                else:
                    if len(name) < 3 or 'and' in name or len(company['ticker']) < 3:
                        continue
                    if re.search(fr"\b{name}\b", norm_text, flags=re.IGNORECASE) is not None \
                            or re.search(fr"\b{company['ticker']}\b", norm_text, flags=re.IGNORECASE) is not None:
                        print(f"{name}:{company['ticker']} in {norm_text}")
                        self.analytic_cursor.execute(insert_reference, (company['ticker'], company['id_company'],
                                                                        company['id_stock_market'], id_finmarket))
        self.analytic_connection.commit()
        print("END")
        self._set_datetime_updated('finmarket_updated', data[0]['added'])


if __name__ == '__main__':
    mx = FinmarketTransformer()
    mx.transform()

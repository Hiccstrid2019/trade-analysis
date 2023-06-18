import re

from transformer.Transformer import Transformer


class ReferenceBuilder(Transformer):
    def _get_all_posts(self):
        query = "SELECT id, norm_text FROM analytics.public.publication_tg"
        self.analytic_cursor.execute(query)
        data = self.analytic_cursor.fetchall()
        return data

    def build(self):
        companies = self._get_all_companies()
        posts = self._get_all_posts()
        command = "INSERT INTO analytics.public.reference " \
                  "(ticker, id_company, id_stock_market, id_publication_tg) " \
                  "VALUES (%s, %s, %s, %s)"
        for post in posts:
            for company in companies:
                name = self._clean_name(company['short_name'])
                if re.search(fr"\b{name}\b", post['norm_text'], flags=re.IGNORECASE) is not None:
                    self.analytic_cursor.execute(command, (company['ticker'], company['id_company'],
                                                 company['id_stock_market'], post['id']))
        self.analytic_connection.commit()


if __name__ == '__main__':
    mx = ReferenceBuilder()
    mx.build()
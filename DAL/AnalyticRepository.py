from datetime import date

import psycopg2 as pg
import psycopg2.extras
import numpy as np


class AnalyticRepository:
    def __init__(self):
        self.connection = pg.connect(host='localhost', port='5432', user='postgres', password='root', database='analytics')
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def get_all_company_exchange(self, market_id: int):
        query = "SELECT id, stock_market_id, short_name FROM analytics.public.company WHERE stock_market_id = %s"
        self.cursor.execute(query, (market_id,))
        companies = self.cursor.fetchall()
        return companies

    def get_stocks_company(self, id_company: int, id_stock_market: int, start: date, end: date):
        if id_stock_market == 1:
            query = "SELECT open, low, high, close, average, to_char(date, 'YYYY-MM-DD') as date " \
                    "FROM analytics.public.stock " \
                    "WHERE id_company = %s AND id_stock_market = %s " \
                    "AND date >= %s AND date < %s " \
                    "ORDER BY date;"
        else:
            query = "SELECT average, to_char(date, 'YYYY-MM-DD') as date " \
                    "FROM analytics.public.stock " \
                    "WHERE id_company = %s AND id_stock_market = %s " \
                    "AND date >= %s AND date < %s AND average > 0 " \
                    "ORDER BY date;"
        self.cursor.execute(query, (id_company, id_stock_market, start, end))
        stocks = self.cursor.fetchall()
        print(stocks)
        return stocks

    def get_media_company(self, id_company: int, id_stock_market: int, start: date, end: date):
        query = "SELECT to_char(date_trunc('day', datetime), 'YYYY-MM-DD') AS date, count(*) " \
                "FROM analytics.public.reference JOIN analytics.public.publication_tg " \
                "ON id_publication_tg = publication_tg.id " \
                "WHERE id_company = %s AND id_stock_market = %s " \
                "AND datetime >= %s AND datetime < %s " \
                "GROUP BY date " \
                "ORDER BY date;"
        self.cursor.execute(query, (id_company, id_stock_market, start, end))
        media = self.cursor.fetchall()
        return media

    def get_company_data(self, id_company: int, id_stock_market: int, start: date, end: date):
        stocks = self.get_stocks_company(id_company, id_stock_market, start, end)
        media = self.get_media_company(id_company, id_stock_market, start, end)
        data = {
            "stocks": stocks,
            "media": media
        }
        return data

    def get_posts_mention_company(self, id_company: int, id_stock_market: int, date: date):
        query = "SELECT to_char(datetime, 'DD-MM-YYYY HH24:MI:SS') as date, text, username, name " \
                "FROM analytics.public.reference  " \
                "JOIN analytics.public.publication_tg ON id_publication_tg = publication_tg.id " \
                "JOIN analytics.public.tg_channel ON id_channel = tg_channel.id  " \
                "WHERE id_company = %s AND id_stock_market = %s " \
                "AND datetime BETWEEN " \
                "DATE_TRUNC('day', %s::DATE) AND " \
                "DATE_TRUNC('day', %s::DATE) + INTERVAL '1 day' - INTERVAL '1 second' ORDER BY datetime;"
        self.cursor.execute(query, (id_company, id_stock_market, date, date))
        posts = self.cursor.fetchall()
        print(posts)
        return posts

    def get_prices(self, id_company: int, id_stock_market: int, date: date):
        query = ("SELECT date, average\n"
                 "        FROM analytics.public.stock\n"
                 "        WHERE id_company = %s and id_stock_market = %s and average is not null and\n"
                 "        date < %s\n"
                 "        ORDER BY date DESC\n"
                 "        LIMIT 5")
        self.cursor.execute(query, (id_company, id_stock_market, date))
        before = self.cursor.fetchall()

        query = '''SELECT date, average
FROM analytics.public.stock
WHERE id_company = %s and id_stock_market = %s and average is not null and
date > %s
ORDER BY date ASC
LIMIT 5'''

        self.cursor.execute(query, (id_company, id_stock_market, date))
        after = self.cursor.fetchall()

        stock_1 = [float(r['average']) for r in before]
        stock_2 = [float(r['average']) for r in after]
        print(stock_1, stock_2)
        autocor = np.corrcoef(stock_1, stock_2)
        return autocor[1,0]


    def __del__(self):
        self.cursor.close()
        self.connection.close()


if __name__ == '__main__':
    mx = AnalyticRepository()
    mx.get_media_company(6, 1, date(2022, 1, 1), date(2023, 1, 1))
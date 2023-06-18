import psycopg2 as pg
import psycopg2.extras
from datetime import datetime


class Transformer:
    def __init__(self):
        self.raw_connection = pg.connect(host='localhost', port='5432',
                                         user='postgres', password='root', database='rowdata')
        self.raw_cursor = self.raw_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.analytic_connection = pg.connect(host='localhost', port='5432',
                                              user='postgres', password='root', database='analytics')
        self.analytic_cursor = self.analytic_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def _get_datetime_updated(self, table_name: str) -> datetime:
        self.analytic_cursor.execute(f"SELECT {table_name} FROM analytics.public.updated")
        timestamp = self.analytic_cursor.fetchone()[0]
        return timestamp

    def _set_datetime_updated(self, table_name, datetime: datetime):
        self.analytic_cursor.execute(f"UPDATE analytics.public.updated SET {table_name} = %s WHERE id = 1", (datetime,))
        self.analytic_connection.commit()

    def _get_all_companies(self):
        query = "SELECT ticker, id_company, id_stock_market, short_name " \
                  "FROM analytics.public.ticker JOIN analytics.public.company ON id_company = id;"
        self.analytic_cursor.execute(query)
        data = self.analytic_cursor.fetchall()
        return data

    def clean_name(self, name):
        return name.replace('-ао', '')\
            .replace(' ао', '')\
            .replace(' ап', '')\
            .replace('+', '')\
            .replace(' Inc.', '')\
            .replace(', Inc.', '').replace(',', '')

    def __del__(self):
        self.raw_cursor.close()
        self.raw_connection.close()
        self.analytic_cursor.close()
        self.analytic_connection.close()
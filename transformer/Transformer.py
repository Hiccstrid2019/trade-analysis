import psycopg2 as pg
import psycopg2.extras


class Transformer:
    def __init__(self):
        self.row_connection = pg.connect(host='localhost', port='5432',
                                         user='postgres', password='root', database='rowdata')
        self.row_cursor = self.row_connection.cursor()
        self.analytic_connection = pg.connect(host='localhost', port='5432',
                                              user='postgres', password='root', database='analytics')
        self.analytic_cursor = self.analytic_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.row_cursor.close()
        self.row_connection.close()
        self.analytic_cursor.close()
        self.analytic_connection.close()
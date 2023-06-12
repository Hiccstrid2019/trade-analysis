import psycopg2 as pg


class Exporter:
    def __init__(self):
        self.connection = pg.connect(host='localhost', port='5432', user='postgres', password='root', database='rowdata')
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.cursor.close()
        self.connection.close()


if __name__ == '__main__':
    d = Exporter()
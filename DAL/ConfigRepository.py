import psycopg2 as pg


class ConfigRepository:
    def __init__(self):
        self.connection = pg.connect(host='localhost', port='5432', user='postgres', password='root', database='config')
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def add_tg_channel(self, username: str, title: str, subs: int, photo_id: str) -> int:
        command = "INSERT INTO tg_channels (username, name, subsribers, photo_id) " \
                  "VALUES (%s, %s, %s, %s) RETURNING id"
        self.cursor.execute(command, (username, title, subs, photo_id))
        self.connection.commit()
        return self.cursor.fetchone()[0]

    def get_all_tg_channels(self):
        command = "SELECT id, username, name, subsribers, photo_id FROM tg_channels;"
        self.cursor.execute(command)
        channels = self.cursor.fetchall()
        return channels

    def get_all_youtube_channels(self):
        command = "SELECT id, username, name, subs FROM config.public.youtube_channels;"
        self.cursor.execute(command)
        channels = self.cursor.fetchall()
        return channels


if __name__ == '__main__':
   mx = ConfigRepository()
   for i in mx.get_all_tg_channels():
       print(i[1])
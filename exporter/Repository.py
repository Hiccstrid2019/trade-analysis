from exporter.Exporter import Exporter


class Repository(Exporter):
    def get_data_range(self, source_name: str) -> tuple:
        if source_name == 'cbr':
            self.cursor.execute('SELECT min(date), max(date) FROM public.cbr_currency')
        elif source_name == 'moex':
            self.cursor.execute('SELECT min(date), max(date) FROM public.moex')
        elif source_name == 'finmarket':
            self.cursor.execute('SELECT min("timestamp"), max("timestamp") FROM public.finmarket;')
        elif source_name == 'spbex':
            self.cursor.execute('SELECT min(date), max(date) FROM public.spbex')

        data = self.cursor.fetchone()
        return data

    def get_data_range_for_tg(self, channel: str) -> tuple:
        command = "SELECT min((publication->>'date')::timestamp), max((publication->>'date')::timestamp) " \
                  "FROM public.telegram WHERE publication->'sender_chat'->>'username' = %s;"
        self.cursor.execute(command, (channel,))
        data = self.cursor.fetchone()
        return data

    def get_channel_offset_msg_id(self, channel: str) -> int:
        query = "SELECT max((publication->>'id')::int) as id " \
                "FROM public.telegram WHERE publication->'sender_chat'->>'username' = %s;"
        self.cursor.execute(query, (channel,))
        id = self.cursor.fetchone()[0]
        if id is not None:
            return id
        return 0

    def get_url_offset(self, channel: str):
        query = "SELECT url FROM rowdata.public.youtube	" \
                "WHERE username = %s " \
                "AND date = (SELECT MAX(date) FROM public.youtube WHERE username = %s);"
        self.cursor.execute(query, (channel, channel))
        url = self.cursor.fetchone()
        if url is not None:
            return url[0]
        return None


if __name__ == '__main__':
    cbre = Repository()
    # print(cbre.get_channel_msg_id('bitkogan'))
    print(cbre.get_url_offset('@Finversia'))
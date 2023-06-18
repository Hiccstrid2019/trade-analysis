import asyncio
from pathlib import Path
from pyrogram import Client, errors
import os

from pyrogram.enums import ChatType

from scraper.setting import APP_ID, APP_HASH


class TelegramParser:
    def __init__(self):
        app_id = APP_ID
        api_hash = APP_HASH
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.app = Client(os.getcwd() + '/scraper/account', app_id, api_hash)
        # self.app = Client('account', app_id, api_hash)

    def get_info(self, channel):
        with self.app:
            try:
                chat = self.app.get_chat(channel)
                if chat.type == ChatType.GROUP:
                    return chat
                return None
            except errors.exceptions.bad_request_400.UsernameNotOccupied:
                return None
            except errors.exceptions.bad_request_400.UsernameInvalid:
                return None

    def save_history(self, channel: str, offset_id: int = 0):

        Path("./data/tg").mkdir(parents=True, exist_ok=True)
        with self.app:
            with open(f'./data/tg/{channel}.json', 'w', encoding='utf8') as file:
                file.write('[')
                for message in self.app.get_chat_history(channel, min_id=offset_id):
                    file.write(f"{message},")
                file.write(']')


if __name__ == '__main__':
    p = TelegramParser()
    # p.save_history('if_stocks', offset_id=5825)
    print(p.get_info('@if_stocks'))

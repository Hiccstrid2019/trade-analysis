import locale
import requests

locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')


class Parser:
    def __init__(self):
        self.session = requests.Session()

    def get_html(self, url: str, encoding=False):
        response = self.session.get(url)
        if encoding:
            response.encoding = encoding
        if response.ok:
            return response.text
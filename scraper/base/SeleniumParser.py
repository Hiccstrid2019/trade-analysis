import locale

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')


class SeleniumParser:
    def __init__(self):
        options = Options()
        options.add_argument("--start-maximized")
        options.add_argument("--mute-audio")
        options.add_experimental_option("useAutomationExtension", False)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    def __del__(self):
        self.driver.close()

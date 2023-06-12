import csv
from datetime import date, timedelta, datetime
from pathlib import Path

import requests
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait

from scraper.base.SeleniumParser import SeleniumParser

months = {
    'январь': 'января',
    'февраль': 'февраля',
    'март': 'марта',
    'апрель': 'апреля',
    'май': 'мая',
    'июнь': 'июня',
    'июль': 'июля',
    'август': 'августа',
    'сентябрь': 'сентября',
    'октябрь': 'октября',
    'ноябрь': 'ноября',
    'декабрь': 'декабря',
}


class SPBParser(SeleniumParser):
    # def __init__(self):
    #     options = Options()
    #     options.add_argument("--start-maximized")
    #     options.add_experimental_option("useAutomationExtension", False)
    #     options.add_experimental_option("excludeSwitches", ["enable-automation"])
    #     self.driver = webdriver.Chrome('chromedriver', options=options)

    def save_year(self, year):
        self.driver.get("https://spbexchange.ru/ru/market-data/archive.aspx")
        WebDriverWait(self.driver, 200).until(
            EC.presence_of_element_located((By.ID, 'ctl00_BXContent_ydiv')))
        with open(f"pdflinks-{year}.csv", "a") as file:
            select = Select(self.driver.find_element(By.ID, 'ctl00_BXContent_ddlYear'))
            select.select_by_value(f"{year}")
            start = date(year, 1, 1)
            if year == date.today().year:
                end = date(year, date.today().month, date.today().day)
            delta = timedelta(days=1)
            while start < end:
                month = start.strftime("%B").lower()
                day = start.strftime("%d")
                xpath = f"//a[@title='{months[month]} {day}']"
                try:
                    # driver.find_element(By.XPATH, xpath).click()
                    script = self.driver.find_element(By.XPATH, xpath).get_attribute('href')
                    self.driver.execute_script(script.replace("javascript:", ""))
                    WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.ID, 'ctl00_BXContent_EveLink')))
                    pdf_link = self.driver.find_element(By.XPATH, '//a[contains(text(), "основной сессии")]').get_attribute(
                        'href')
                    print(f'{start.strftime("%Y-%m-%d")}: {pdf_link}')
                    file.write(f'{start.strftime("%Y-%m-%d")};{pdf_link}\n')
                    self.driver.execute_script("$('#dialog').dialog('close')")
                except NoSuchElementException:
                    print(f'{start.strftime("%Y-%m-%d")}: error')
                    file.write(f'{start.strftime("%Y-%m-%d")};null\n')
                start += delta

    def repeat_save_year(self, year, driver):
        WebDriverWait(self.driver, 200).until(
            EC.presence_of_element_located((By.ID, 'ctl00_BXContent_ydiv')))
        with open(f"pdflinks-repeat-9-{year}.csv") as file:
            reader = csv.reader(file, delimiter=';')
            data = list(reader)
        select = Select(driver.find_element(By.ID, 'ctl00_BXContent_ddlYear'))
        select.select_by_value(f"{year}")
        for d in data:
            if 'notfound' in d[-1]:
                c_date = datetime.strptime(d[0], "%Y-%m-%d").date()
                day = c_date.strftime("%d")
                month = c_date.strftime("%B").lower()
                xpath = f"//a[@title='{months[month]} {day}']"
                try:
                    # driver.find_element(By.XPATH, xpath).click()
                    script = driver.find_element(By.XPATH, xpath).get_attribute('href')
                    driver.execute_script(script.replace("javascript:", ""))
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, 'ctl00_BXContent_EveLink')))
                    pdf_link = driver.find_element(By.XPATH, '//a[contains(text(), "основной сессии")]').get_attribute(
                        'href')
                    print(f'{c_date.strftime("%Y-%m-%d")}: {pdf_link}')
                    d[-1] = pdf_link
                    driver.execute_script("$('#dialog').dialog('close')")
                    # WebDriverWait(driver, 5).until(
                    #     EC.presence_of_element_located((By.XPATH, '//a[contains(@class, "closeButton")]')))
                    # driver.find_element(By.XPATH, '//a[contains(@class, "closeButton")]').click()
                except NoSuchElementException:
                    print(f'{c_date.strftime("%Y-%m-%d")}: error')
        with open(f"pdflinks-repeat-10-{year}.csv", 'w', newline='') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerows(data)

    def save_pdfs_year(self, year: str):
        Path(f"spb-data/{year}").mkdir(exist_ok=True)
        with open(f"pdflinks-repeat-10-{year}.csv") as file:
            reader = csv.reader(file, delimiter=';')
            data = list(reader)
        for row in data:
            if '.pdf' in row[-1]:
                print(row[0])
                response = requests.get(row[-1])
                with open(f"spb-data/{year}/{row[0]}.pdf", "wb") as fb:
                    fb.write(response.content)

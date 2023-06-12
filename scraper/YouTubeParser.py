import csv
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from scraper.base.SeleniumParser import SeleniumParser


class YouTubeParser(SeleniumParser):

    def parse_video(self, url: str) -> dict:
        print(url)
        self.driver.get(url)
        video = {'url': url}

        WebDriverWait(self.driver, 200).until(EC.element_to_be_clickable((By.ID, 'expand')))
        self.driver.find_element(By.ID, 'expand').click()
        info = self.driver.find_element(By.ID, 'info-container').find_elements(By.TAG_NAME, 'span')
        video['views'] = info[0].text
        video['date'] = info[2].text

        WebDriverWait(self.driver, 200).until(EC.presence_of_element_located((By.XPATH, '//div[@id="description"]')))
        description = self.driver.find_element(By.XPATH, '//div[@id="description"]//ytd-text-inline-expander//yt'
                                                         '-attributed-string').find_element(By.TAG_NAME, 'span').text
        video['description'] = description
        self.driver.find_element(By.ID, 'collapse').click()
        self.driver.find_element(By.TAG_NAME, 'html').send_keys(Keys.PAGE_DOWN)
        WebDriverWait(self.driver, 200).until(EC.element_to_be_clickable((By.ID, 'main')))

        try:
            self.driver.find_element(By.XPATH, '//div[@id="main"]//div['
                                               '@id="pinned-comment-badge"]//span[contains(text('
                                               '), "Закреплено")]')
            try:
                self.driver.find_element(By.XPATH, '//span[contains(@class, "more-button")]').click()
            except:
                pass
            pinned_comment = self.driver.find_element(By.XPATH, '//div[@id="main"]//div[@id="content"]//*['
                                                                '@id="content-text"]').text
            pinned_comment = pinned_comment
        except:
            pinned_comment = ""

        video['pinned_comment'] = pinned_comment
        print(video)
        return video

    def get_all_video_urls(self, channel: str, offset_url: str = None):
        self.driver.get(f'https://www.youtube.com/{channel}/videos')
        WebDriverWait(self.driver, 200).until(EC.presence_of_element_located((By.ID, 'contents')))
        urls = [a.get_attribute('href') for a in
                self.driver.find_elements(By.XPATH, "//ytd-rich-item-renderer//a[@id='video-title-link']")]
        while offset_url not in urls:
            try:
                self.driver.find_element(By.TAG_NAME, 'ytd-continuation-item-renderer')
                self.driver.find_element(By.TAG_NAME, 'html').send_keys(Keys.PAGE_DOWN)
            except:
                break
            urls = [a.get_attribute('href') for a in
                    self.driver.find_elements(By.XPATH, "//ytd-rich-item-renderer//a[@id='video-title-link']")]
        if offset_url in urls:
            index = urls.index(offset_url)
            urls = urls[:index]
        else:
            urls = [a.get_attribute('href') for a in
                    self.driver.find_elements(By.XPATH, "//ytd-rich-item-renderer//a[@id='video-title-link']")]
        print(len(urls))
        return urls

    def save_videos(self, channel, offset_url: str = None):
        urls = self.get_all_video_urls(channel, offset_url)
        Path("./data/youtube").mkdir(parents=True, exist_ok=True)
        with open(f"./data/youtube/{channel}.csv", "a", encoding='utf8', newline='') as file:
            video = self.parse_video(urls[0])
            dict_writer = csv.DictWriter(file, video.keys(), delimiter=';')
            dict_writer.writeheader()
            dict_writer.writerow(video)
            for url in urls[1:]:
                video = self.parse_video(url)
                dict_writer.writerow(video)


if __name__ == '__main__':
    y = YouTubeParser()
    # y.save_all_videos('@smirnovacapital')
    y.save_videos('@Finversia', offset_url='https://www.youtube.com/watch?v=l9QS8pQTgI4')
    # y.parse_video('https://www.youtube.com/watch?v=8EhK1qaO9z0')

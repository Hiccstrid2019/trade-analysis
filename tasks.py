from datetime import date

from celery import Celery
from celery.contrib.abortable import AbortableTask

from DAL.ConfigRepository import ConfigRepository
from exporter.CBRExporter import CBRExporter
from exporter.FinmarketExporter import FinmarketExporter
from exporter.MoexExporter import MoexExporter
from exporter.Repository import Repository
from exporter.SpbexExporter import SpbexExporter
from exporter.TelegramExporter import TelegramExporter
from exporter.YouTubeExporter import YouTubeExporter
from scraper.CBRParser import CBRParser
from scraper.FinmarketParser import FinmarketParser
from scraper.MoExParser import MoExParser
from scraper.SPbExParser import SPbExParser
from scraper.TelegramParser import TelegramParser
from scraper.YouTubeParser import YouTubeParser
from transformer.CurrencyTranformer import CurrencyTransformer
from transformer.FinmarketTransformer import FinmarketTransformer
from transformer.StockTransformer import StockTransformer
from transformer.TelegramTransformer import TelegramTransformer

celery = Celery(__name__)
celery.config_from_object('celeryconfig')


@celery.task(serializer="pickle", base=AbortableTask)
def save_data_by_source(source_name: str, start_period: date, end_period: date):
    if source_name == 'cbr':
        parser = CBRParser()
        parser.save_for_period(start_period, end_period)
        exporter = CBRExporter()
        exporter.export()
        transformer = CurrencyTransformer()
        transformer.transform()
    elif source_name == 'moex':
        parser = MoExParser()
        parser.save_for_period(start_period, end_period)
        exporter = MoexExporter()
        exporter.export()
        transformer = StockTransformer()
        transformer.transform_moex()
    elif source_name == 'spbex':
        parser = SPbExParser()
        parser.save_for_period(start_period, end_period)
        exporter = SpbexExporter()
        exporter.export()
        transformer = StockTransformer()
        transformer.transform_spbex()
    elif source_name == 'finmarket':
        parser = FinmarketParser()
        parser.save_for_period(start_period, end_period)
        exporter = FinmarketExporter()
        exporter.export()
        transformer = FinmarketTransformer()
        transformer.transform()
    return True


@celery.task(serializer="pickle", base=AbortableTask)
def save_yt_videos():
    repository = Repository()
    config_repository = ConfigRepository()

    channels_info = config_repository.get_all_youtube_channels()
    channels = []
    parser = YouTubeParser()
    for row in channels_info:
        username = row[1]
        offset_url = repository.get_url_offset(username)
        print("URL offset: ", offset_url)
        parser.save_videos(username, offset_url=offset_url)
        channels.append(username)

    exporter = YouTubeExporter()
    exporter.export(channels)
    return True


@celery.task(serializer="pickle", base=AbortableTask)
def save_tg_posts():
    repository = Repository()
    config_repository = ConfigRepository()
    parser = TelegramParser()

    channels_info = config_repository.get_all_tg_channels()
    channels = []
    for row in channels_info:
        channel = row[1]
        id = repository.get_channel_offset_msg_id(channel)
        parser.save_history(channel, offset_id=id)
        channels.append(channel)
    exporter = TelegramExporter()
    exporter.export(channels)

    transformer = TelegramTransformer()
    transformer.transform()
    return True


def get_task_status(id):
    result = celery.AsyncResult(id)
    return result


def stop_task(id):
    celery.control.revoke(id, terminate=True)
    return True

from datetime import date, timedelta
from pathlib import Path

from scraper.base.Parser import Parser
from utils.PDFExtractor import PDFExtractor


class SPbExParser(Parser):
    def get_stock_day(self, date: date):
        response = self.session.get(f"https://archives.spbexchange.ru/reports/results/{date.strftime('%Y-%m-%d')}-eve.pdf")
        if response.headers.get('content-type') == 'application/pdf':
            with open(f"./data/spbex/pdf/{date.strftime('%Y-%m-%d')}.pdf", "wb") as fb:
                fb.write(response.content)
            print(f"{date.strftime('%Y-%m-%d')} completed")
            return f"./data/spbex/pdf/{date.strftime('%Y-%m-%d')}.pdf"
        print(f"{date.strftime('%Y-%m-%d')} doesn't exists")
        return None

    def save_for_period(self, start_date: date, end_date: date):
        delta = timedelta(days=1)
        Path("./data/spbex/pdf").mkdir(parents=True, exist_ok=True)
        files = []
        while start_date <= end_date:
            res = self.get_stock_day(start_date)
            if res is not None:
                files.append(res)
            start_date += delta
        PDFExtractor.convert_pdfs_to_csv(files)


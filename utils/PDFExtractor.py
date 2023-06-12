import pandas as pd
import tabula
from os import walk, path
from PyPDF2 import PdfReader


class PDFExtractor:
    @staticmethod
    def get_number_pages(path: str):
        with open(path, 'rb') as fl:
            reader = PdfReader(fl)
            return len(reader.pages)

    @staticmethod
    def pdf_to_dataframe(path: str):
        first_page = tabula.read_pdf(path, pages=1, area=(89, 15.7, 760, 1137))
        pages_count = PDFExtractor.get_number_pages(path)
        other_pages = tabula.read_pdf(path, pages=f'2-{pages_count}', pandas_options={'header': None})
        other_pages = [t for t in other_pages if len(t.columns) == 23]
        for page in other_pages:
            page.columns = first_page[0].columns
        first_page.extend(other_pages)
        df = pd.concat(first_page, ignore_index=True)
        df.columns = df.columns.str.replace('\r', ' ', regex=True)
        df = df.replace('\r', ' ', regex=True)
        df = df.assign(date=pd.to_datetime(path.split('/')[-1][:-4]))
        print(f"converted {path}")
        return df

    @staticmethod
    def convert_pdfs_to_csv(files: list):
        # filenames = next(walk("./data/spbex/pdf/"), (None, None, []))[2]
        dataframes = []
        for file in files:
            frame = PDFExtractor.pdf_to_dataframe(file)
            dataframes.append(frame)
        df = pd.concat(dataframes, ignore_index=True)
        df.to_csv('./data/spbex/spbex.csv', index=False, sep=';', mode='w')


# years = []
#
# for year in range(2021, 2024):
#     filenames = next(walk(f"spb-data\{year}"), (None, None, []))[2]
#
#     days = []
#     for file in filenames:
#         print(f"spb-data\{year}\{file}")
#         file_path = f"spb-data\{year}\{file}"
#         tf = tabula.read_pdf(file_path, pages=1, area=(89, 15.7, 760, 1137))
#         pages = get_pdf_page_count(file_path)
#         other = tabula.read_pdf(file_path, pages=f'2-{pages}', pandas_options={'header': None})
#         other = [t for t in other if len(t.columns) == 23]
#         for t in other:
#             t.columns = tf[0].columns
#
#         tf.extend(other)
#         df = pd.concat(tf, ignore_index=True)
#         df.columns = df.columns.str.replace('\r', ' ', regex=True)
#         df = df.replace('\r', ' ', regex=True)
#         df = df.assign(date=pd.to_datetime(file[:-4]))
#         days.append(df)
#     df = pd.concat(days, ignore_index=True)
#     df.to_csv(f"./spb-data/{year}.csv", index=False, sep=';')
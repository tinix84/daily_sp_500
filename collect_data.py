import datetime as dt
import logging
import os

import pandas as pd
import yfinance as yf

from config import config
from shared.utils import create_folder_if_not_exists


def all_companies_yf(company_symbols=None, end_day=None):

    not_found = []

    for ind, c in enumerate(company_symbols):
        print(f'{ind}. Downloading data for {c}')
        logging.info(f'{ind}. Downloading data for {c}')
        try:
            if end_day is None:
                end_day = dt.datetime.now().date().strftime('%Y-%m-%d')

            data = download_company_yf(c, start_date="2006-01-01", end_date=end_day, threads_bool=True)
            create_folder_if_not_exists(config.historical_prices_path, c)
            data.to_csv(os.path.join(config.historical_prices_path, c, 'company_data.csv'), index=True)
        except Exception as e:
            logging.error(e)
            not_found.append(c)

    if len(not_found) > 0:
        logging.info('Not found: ', not_found)
        df = pd.DataFrame(not_found)
        df.to_csv(os.path.join(config.historical_prices_path, 'not_found.csv'), index=False)


def download_company_yf(ticker, start_date, end_date, threads_bool=False):
    logging.info('download_company_yf: {}'.format(ticker))
    ticker_yf = yf.Ticker(ticker)
    # data = ticker_yf.history(period = 'max')
    data = ticker_yf.history(start=start_date, end=end_date, threads=threads_bool)
    return data

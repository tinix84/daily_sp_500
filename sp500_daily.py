import json
import logging
# import os
# import traceback
# from concurrent.futures import ThreadPoolExecutor
# from datetime import datetime
# import datetime as dt
# from pathlib import Path

import pandas as pd
import yfinance as yf
import time

from collect_tickers import get_tickers_csv, get_tickers_json
from daily_update import daily_update, daily_update_thread

# from config import config
# from data_collection.collect_data import download_company_yf
# from data_collection.collect_tickers import get_ticker_info_yf


class Stock():

    def __init__(self, filename, asset_type='stock', close_param='Close'):

        self.filename = None
        self.asset_type = asset_type
        self.tickers = None
        self.tickers_updated = None
        self.close_param = close_param

        logging.info('#' * 50)
        logging.info('Running on ticker file: {}'.format(filename))
        logging.info('Running on close type: {}'.format(close_param))

        if filename.endswith('.json'):
            self.tickers = get_tickers_json(filename)
        elif filename.endswith('.csv'):
            self.tickers = get_tickers_csv(filename)

    def get_tickers(self):
        return self.tickers

def run_DEV():
    MyStock_Tickers = Stock(filename='my_tickers.json', asset_type='stock', close_param='Close')
    # Top10_Tickers = Stock(filename='top10_tickers.json', asset_type='stock', close_param='Close')
    # Russell1000_Tickers = Stock(filename='russell_1000_tickers.json', asset_type='stock', close_param='Close')
    # US_ETF_Tickers = Stock(filename='US_ETF.csv', asset_type='stock', close_param='Close')
    SP500_Tickers = Stock(filename='sp500.csv', asset_type='stock', close_param='Close')
    # Crypto_Tickers = Crypto(filename='my_crypto_tickers.json', asset_type='crypto', close_param='Close')

    # ScreenerList = [SP500_Tickers, Crypto_Tickers]
    ScreenerList = [MyStock_Tickers]

    for screener in ScreenerList:

        # remove_delisted_companies()
        t = time.time()
        daily_update(screener.tickers, force_update=True, force_info_update=True)
        # daily_update_thread(screener.tickers)
        logging.info('Daily update time: {}'.format(time.time() - t))

    logging.info('COMPLETED!')

def sp500_daily_original():
    
    table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    sp_ticks = df["Symbol"].to_list()
    sp_ticks_forYF = [tick.replace(".","-") for tick in sp_ticks]

    closes = pd.DataFrame()
    for idx in range(0,len(sp_ticks_forYF),20):
        df = yf.download(sp_ticks_forYF[idx:idx+20],period="1y", progress=True)["Adj Close"]
        closes = pd.concat([df, closes], axis=1)
        time.sleep(.5)
    closes.to_csv("SP500_prices_1yr.csv")

if __name__ == '__main__':

    t = time.time()
    sp500_daily_original()
    # run_DEV()
    # logging.info('run_DEV: {}'.format(time.time() - t))






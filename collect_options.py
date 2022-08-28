import datetime as dt
import logging
import os
import sys

import numpy as np
import pandas as pd
import yfinance as yf

from config import config
from shared.utils import create_folder_if_not_exists, date_str_generation


now_str, today, yesterday_str, today_str, day_before_yesterday_str, today_filename = date_str_generation()

file_handler = logging.FileHandler(
    filename=f'./log/app/{now_str}_options_downloader.log', mode='w')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(
    level=logging.DEBUG,
    # level=logging.ERROR,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers
)


def all_companies_options_yfinance_today(tickers=None):
    not_found = []

    for ind, ticker in enumerate(tickers):
        logging.info('#' * 50)
        logging.info(f'{ind}. Downloading data for {ticker}')
        try:
            [all_calls_df, all_puts_df] = download_company_options_today(ticker=ticker)
            logging.info("Saving in folder{}".format((os.path.join(config.historical_prices_path, ticker, f'{today_filename}_{ticker}_calls/puts.csv'))))
            create_folder_if_not_exists(config.historical_prices_path, ticker)
            all_calls_df.to_csv(os.path.join(
                config.historical_prices_path, ticker, f'{today_filename}_{ticker}_calls.csv'), index=True)
            all_puts_df.to_csv(os.path.join(
                config.historical_prices_path, ticker, f'{today_filename}_{ticker}_puts.csv'), index=True)

        except Exception as e:
            logging.error(e)
            not_found.append(ticker)

    if len(not_found) > 0:
        logging.info('options not found: ', not_found)
        df = pd.DataFrame(not_found)
        df.to_csv(os.path.join(config.historical_prices_path, 'options_not_found.csv'), index=False)


def download_company_options_today(ticker):
    if ticker is None:
        logging.error('ticker not defined')

    ticker_yf = yf.Ticker(ticker)

    all_calls_df = pd.DataFrame()
    all_puts_df = pd.DataFrame()

    expiration_dates = ticker_yf.options

    # get option chain calls data for specific expiration date
    for expiration_date in expiration_dates:
        opt = ticker_yf.option_chain(date=expiration_date)
        calls_df = opt.calls
        puts_df = opt.puts
        calls_df['get_date'] = puts_df['get_date'] = now_str
        puts_df['ExpirationDate'] = calls_df['ExpirationDate'] = expiration_date

        logging.info(f'Expiration Date: {expiration_date} calls are {calls_df.shape}')
        logging.info(f'Expiration Date: {expiration_date} puts are {puts_df.shape}')

        all_calls_df = all_calls_df.append(calls_df)
        all_puts_df = all_puts_df.append(puts_df)

    return all_calls_df, all_puts_df


def rename_col_yf_like_cboe_csv(calls_df, puts_df):
    # df['ExpirationDate'] = pd.to_datetime(expiration_date, format='%a %b %d %Y')
    # df['ExpirationDate'] = df['ExpirationDate'] + timedelta(hours=16)
    # puts_df['ExpirationDate'] = calls_df['ExpirationDate'] = expiration_date

    calls_df.rename(columns={'contractSymbol': 'Calls',
                             # 'lastTradeDate',
                             'strike': 'StrikePrice',
                             'lastPrice': 'CallLastSale',
                             'bid': 'CallBid',
                             'ask': 'CallAsk',
                             'change': 'CallNet',
                             # 'percentChange',
                             'volume': 'CallVol',
                             'openInterest': 'CallOpenInt',
                             'impliedVolatility': 'CallIV',
                             'inTheMoney': 'CallInTheMoney',
                             'contractSize': 'contractSize',
                             'ExpirationDate': 'expirationDate',
                             # 'currency'
                             }, inplace=True)

    # Drop unnecessary and meaningless columns
    calls_df = calls_df.drop(
        columns=['currency', 'lastTradeDate'])

    # puts_df = opt.puts.copy(deep=True)
    puts_df.rename(columns={'contractSymbol': 'Puts',
                            # 'lastTradeDate',
                            'strike': 'StrikePrice',
                            'lastPrice': 'PutLastSale',
                            'bid': 'PutBid',
                            'ask': 'PutAsk',
                            'change': 'PutNet',
                            # 'percentChange',
                            'volume': 'PutVol',
                            'openInterest': 'PutOpenInt',
                            'impliedVolatility': 'PutIV',
                            'inTheMoney': 'PutInTheMoney',
                            'contractSize': 'contractSize',
                            # 'currency'
                            }, inplace=True)

    # Drop unnecessary and meaningless columns
    puts_df = puts_df.drop(
        columns=['currency', 'lastTradeDate'])

    return calls_df, puts_df


def drop_and_clean_options_df_yf(df):
    df.rename(columns={  # 'contractSymbol': 'callsContractSymbol',
        # 'lastTradeDate',
        'get_date': 'getDate',
        'strike': 'strikePrice',
        'lastPrice': 'lastPrice',
        'bid': 'bid',
        'ask': 'ask',
        'change': 'net',
        # 'percentChange',
        'volume': 'volume',
        'openInterest': 'openInterest',
        'impliedVolatility': 'impliedVolatility',
        'inTheMoney': 'inTheMoney',
        'contractSize': 'contractSize',
        'ExpirationDate': 'expirationDate',
        # 'currency'
    }, inplace=True)

    df.expirationDate = pd.to_datetime(df.expirationDate)
    df.getDate = pd.to_datetime(df.getDate, format='%Y%m%d_%H%M%S', errors='ignore')

    # Drop unnecessary and meaningless columns
    df = df.drop(
        columns=['currency', 'lastTradeDate'])

    return df


def is_third_friday(d):
    return d.weekday() == 4 and 15 <= d.day <= 21


def calc_time_expiration(df1, todayDate=dt.datetime.today(), norm_dte=262):
    df = df1.copy(deep=True)
    # For 0DTE options, I'm setting DTE = 1 day, otherwise they get excluded
    df['daysTillExp'] = [1 / norm_dte if (np.busday_count(todayDate.date(), x.date())) == 0 
                                    else np.busday_count(todayDate.date(), x.date()) / norm_dte 
                                    for x in df.expirationDate]

    next_expiry = df['expirationDate'].min()
    logging.info('Next expiry: {}'.format(next_expiry))

    df['isThirdFriday'] = [is_third_friday(x) for x in df.expirationDate]
    third_fridays = df.loc[df['isThirdFriday'] == True]
    next_monthly_exp = third_fridays['expirationDate'].min()
    logging.info('Next monthly expiry: {}'.format(next_monthly_exp))

    return df, next_expiry, next_monthly_exp


def rename_col_df_like_cboe_csv(df, expiration_date, calls_df, puts_df):
    df['ExpirationDate'] = pd.to_datetime(expiration_date, format='%a %b %d %Y')
    df['ExpirationDate'] = df['ExpirationDate'] + dt.timedelta(hours=16)
    puts_df['ExpirationDate'] = calls_df['ExpirationDate'] = expiration_date

    calls_df.rename(columns={'contractSymbol': 'Calls',
                             # 'lastTradeDate',
                             'strike': 'StrikePrice',
                             'lastPrice': 'CallLastSale',
                             'bid': 'CallBid',
                             'ask': 'CallAsk',
                             'change': 'CallNet',
                             # 'percentChange',
                             'volume': 'CallVol',
                             'openInterest': 'CallOpenInt',
                             'impliedVolatility': 'CallIV',
                             'inTheMoney': 'CallInTheMoney',
                             'contractSize': 'contractSize',
                             'ExpirationDate': 'expirationDate',
                             # 'currency'
                             }, inplace=True)

    # Drop unnecessary and meaningless columns
    calls_df = calls_df.drop(columns=['currency', 'lastTradeDate'])

    # puts_df = opt.puts.copy(deep=True)
    puts_df.rename(columns={'contractSymbol': 'Puts',
                            # 'lastTradeDate',
                            'strike': 'StrikePrice',
                            'lastPrice': 'PutLastSale',
                            'bid': 'PutBid',
                            'ask': 'PutAsk',
                            'change': 'PutNet',
                            # 'percentChange',
                            'volume': 'PutVol',
                            'openInterest': 'PutOpenInt',
                            'impliedVolatility': 'PutIV',
                            'inTheMoney': 'PutInTheMoney',
                            'contractSize': 'contractSize',
                            # 'currency'
                            }, inplace=True)

    # Drop unnecessary and meaningless columns
    puts_df = puts_df.drop(columns=['currency', 'lastTradeDate'])

    return calls_df, puts_df


# def drop_and_clean_options_df_yf(df):
#     df.rename(columns={#'contractSymbol': 'callsContractSymbol',
#                         # 'lastTradeDate',
#                         'get_date': 'getDate',
#                         'strike': 'strikePrice',
#                         'lastPrice': 'lastPrice',
#                         'bid': 'bid',
#                         'ask': 'ask',
#                         'change': 'net',
#                         # 'percentChange',
#                         'volume': 'volume',
#                         'openInterest': 'openInterest',
#                         'impliedVolatility': 'impliedVolatility',
#                         'inTheMoney': 'inTheMoney',
#                         'contractSize': 'contractSize',
#                         'ExpirationDate': 'expirationDate',
#                         # 'currency'
#                         }, inplace=True)
#     df.expirationDate = pd.to_datetime(df.expirationDate)
#     df.getDate = pd.to_datetime(df.getDate)

#     # Drop unnecessary and meaningless columns
#     df = df.drop(
#         columns=['currency', 'lastTradeDate'])

#     return df

def load_option_chain_csv(ticker, day_filename):
    puts_df_all = pd.read_csv(f"data/companies/{ticker}/{day_filename}_{ticker}_puts.csv", index_col=0)
    puts_df_all = drop_and_clean_options_df_yf(puts_df_all)
    calls_df_all = pd.read_csv(f"data/companies/{ticker}/{day_filename}_{ticker}_calls.csv", index_col=0)
    calls_df_all = drop_and_clean_options_df_yf(calls_df_all)
    return calls_df_all, puts_df_all

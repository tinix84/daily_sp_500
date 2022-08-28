import json
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor
# from datetime import datetime
import datetime as dt
from pathlib import Path

import pandas as pd
import yfinance as yf

from config import config
from collect_data import download_company_yf
from collect_tickers import get_ticker_info_yf
from shared.utils import read_json_file, write_json_file


def save_company_data_to_csv(company_data, ticker):
    company_data['Date'] = pd.to_datetime(company_data['Date'])
    company_data['Date'] = company_data['Date'].dt.strftime('%Y-%m-%d')

    output_dir = Path(os.path.join(config.historical_prices_path, ticker))
    output_dir.mkdir(parents=True, exist_ok=True)

    # can join path elements with / operator
    company_data.to_csv(output_dir / 'company_data.csv', index=False)
    # company_data.to_csv(os.path.join(config.historical_prices_path, ticker, 'company_data.csv'), index=False)


def save_company_data_to_pkl(company_data: pd.DataFrame, ticker:str):
    company_data['Date'] = pd.to_datetime(company_data['Date'])
    company_data['Date'] = company_data['Date'].dt.strftime('%Y-%m-%d')

    output_dir = Path(os.path.join(config.historical_prices_path, ticker))
    output_dir.mkdir(parents=True, exist_ok=True)

    # can join path elements with / operator
    company_data.to_pickle(output_dir / 'company_data.pkl')
    # company_data.to_csv(os.path.join(config.historical_prices_path, ticker, 'company_data.csv'), index=False)


def daily_update(tickers, force_update=True, force_info_update=False):
    today = dt.datetime.today().strftime('%Y-%m-%d')

    for ind, ticker in enumerate(tickers):
        print('#' * 50)
        logging.info('{}. Updating data for {}'.format(ind, ticker))

        try:
            # check if we have data of ticker
            # company_data = pd.read_csv(os.path.join(config.historical_prices_path, ticker, 'company_data.csv'))
            company_data = pd.read_pickle(os.path.join(config.historical_prices_path, ticker, 'company_data.pkl'))
            last_stored_date = company_data['Date'].iloc[-1]
            ticker_exist = True
        except Exception as e:
            logging.error(e)
            ticker_exist = False
            force_update = True
            last_stored_date = None

        data = None

        if not force_update:
            try:
                data = download_company_yf(ticker=ticker, threads_bool=False, start_date=last_stored_date, end_date=today)
                data = data.loc[last_stored_date: today]
                ticker_exist = True
                data = data[1:]
                data = data.reset_index()
            except Exception as e:
                logging.error(e)
                force_update = True

        elif ticker_exist is False or force_update:
            if last_stored_date == today:
                logging.info('{} is already updated'.format(ticker))
            else:
                try:
                    logging.info('Download all history company data for {}'.format(ticker))
                    company_data = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'])
                    # this function is using yf.history that use auto_adjust =True
                    data = download_company_yf(ticker=ticker, 
                                                threads_bool=False, 
                                                start_date="2006-01-01",
                                                end_date=today)
                    data = data.reset_index()
                except Exception as e:
                    logging.error(e)

        if data is None:
            logging.error('No data for {}'.format(ticker))
        else:
            # company_data = company_data.append(data, ignore_index=True)
            company_data = pd.concat([company_data, pd.DataFrame.from_records(data)])

            company_data['Date'] = pd.to_datetime(company_data['Date'])
            company_data['Date'] = company_data['Date'].dt.strftime('%Y-%m-%d')
            try:
                output_dir = Path(os.path.join(config.historical_prices_path, str(ticker)))
                output_dir.mkdir(parents=True, exist_ok=True)

                # can join path elements with / operator
                company_data.to_pickle(output_dir / 'company_data.pkl')
                # company_data.to_csv(os.path.join(config.historical_prices_path, ticker, 'company_data.csv'), index=False)
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())  # TODO: fix PRN folder not possible to write https://fossbytes.com/windows-reserved-folder-con-create/

        try:
            company_info = read_json_file(Path(os.path.join(
                config.historical_prices_path, str(ticker))) / 'company_info.json')
            # with open(Path(os.path.join(config.historical_prices_path, str(ticker))) / 'company_info.json') as json_file:
            #     company_info = json.load(json_file)
        except Exception as e:
            logging.error(e)
            company_info = None

        if company_info is None or force_info_update:
            try:
                company_info = get_ticker_info_yf(ticker)
                output_dir = Path(os.path.join(config.historical_prices_path, str(ticker)))

                # with open(output_dir / "company_info.json", "w") as outfile:
                #     json.dump(company_info, outfile)
                write_json_file(filename = output_dir / "company_info.json", content=company_info)
            except Exception as e:
                logging.error(e)

    return True  # loop successfull


def daily_update_thread(tickers):
    ticker_list = []

    for ticker in tickers:
        ticker_list.append(ticker)

    data = yf.download(tickers=ticker_list, 
                        period='10y', interval='1d', group_by='ticker',
                       auto_adjust=False, prepost=False, threads=True, proxy=None)

    data = data.T
    # data.loc[(ticker,),].T.to_csv(config.test_thread +'/' + ticker + '.csv', sep=',', encoding='utf-8')

    for ticker in ticker_list:
        company_data = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'])
        company_data = pd.concat([company_data, pd.DataFrame.from_records(data.loc[(ticker,)].T)])

        company_data['Date'] = pd.to_datetime(company_data['Date'])
        company_data['Date'] = company_data['Date'].dt.strftime('%Y-%m-%d')
        try:
            output_dir = Path(os.path.join(config.test_thread, str(ticker)))
            output_dir.mkdir(parents=True, exist_ok=True)

            # can join path elements with / operator
            company_data.to_csv(output_dir / 'company_data.csv', index=False)
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())

    # save company info
    with ThreadPoolExecutor() as executor:
        executor.map(save_company_info, ticker_list)

    return True  # loop successfull


def save_company_info(ticker):
    try:
        with open(Path(os.path.join(config.test_thread, str(ticker))) / 'company_info.json') as json_file:
            company_info = json.load(json_file)
    except Exception as e:
        logging.error(e)
        company_info = None

    if company_info is None:  # if we have already the info we do nothing
        try:
            company_info = yf.Tickers(ticker).tickers[ticker].info
            output_dir = Path(os.path.join(config.test_thread, str(ticker)))
            with open(output_dir / "company_info.json", "w") as outfile:
                json.dump(company_info, outfile)
        except Exception as e:
            logging.error(e)

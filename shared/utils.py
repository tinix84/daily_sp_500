#!/usr/bin/env python
"""UTILS MODULE"""
__docformat__ = "numpy"

# IMPORTATION STANDARD
import json
import os
# from os import path
import datetime as dt
import logging

# IMPORTATION THIRDPARTY
import pandas as pd
import pandas_market_calendars as mcal
import numpy as np
from pytz import timezone
import matplotlib.pyplot as plt

# IMPORTATION INTERNAL
from config import config


def trading_days(start_date, end_date):
    tz = timezone('US/Eastern')
    nyse = mcal.get_calendar('NYSE')
    market_open_close_df = nyse.schedule(start_date=start_date, end_date=end_date, tz=tz)
    return market_open_close_df['market_open'].dt.strftime('%Y-%m-%d').tolist()


def read_json_file(filename):
    with open(filename) as json_file:
        data = json.load(json_file)
    return data

def write_json_file(filename, content):
    with open(filename, 'w') as json_file:
        data = json.dump(content, json_file, )
    return data

def company_data_from_csv(ticker):
    df = pd.read_csv(os.path.join(config.historical_prices_path, ticker, 'company_data.csv'))
    return {'ticker': ticker, 'data': df}


def get_trading_days():
    """load trading dates from json"""
    with open('./data/trading_days.json') as trading_days_json:
        trade_days = json.load(trading_days_json)
        return trade_days


# Expected format for current_date: YYYY-MM-DD
def create_folder_if_not_exists(folder_location, folder_name):
    """Create folder if it does not exist"""
    if not os.path.exists(os.path.join(folder_location, folder_name)):
        os.makedirs(os.path.join(folder_location, folder_name))


def date_str_generation():
    """Generate date string as support of other functions"""
    today = dt.datetime.now().date().strftime('%Y-%m-%d')
    today_str = dt.datetime.today().strftime('%Y-%m-%d')
    today_filename = dt.datetime.today().strftime('%Y%m%d')

    # now = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    now_str = dt.datetime.now().strftime('%Y%m%d_%H%M%S')

    yesterday_str = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    day_before_yesterday_str = ( dt.datetime.today() - dt.timedelta(days=2)).strftime('%Y-%m-%d')
    
    return now_str, today, yesterday_str, today_str, day_before_yesterday_str, today_filename

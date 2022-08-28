import json
import logging
import bs4 as bs
import pandas as pd
import requests
import yahoo_fin.stock_info as si
import yfinance as yf
from pytickersymbols import PyTickerSymbols

# stock_data = PyTickerSymbols()
# countries = stock_data.get_all_countries()
# indices = stock_data.get_all_indices()
# industries = stock_data.get_all_industries()

# stock_data = PyTickerSymbols()
# german_stocks = stock_data.get_stocks_by_index('DAX')
# uk_stocks = stock_data.

# print(list(uk_stocks))

info_stocks_df_columns = [
    'ticker_yahoo',
    'name',
    'country',
    'indices',
    'industries',
    'symbols',
    'metadata',
    'isins',
    'wiki_name',
    'sector',
    'industry',
    'exchange',
    'market_cap',
    'symbol',
    'avg_volume',
    'short_name',
    'currency',
    'country'
]


def get_crypto_tickers_binance():
    raise NotImplementedError


def get_dow_tickers():
    stock_data = PyTickerSymbols()
    list_of_tickers = stock_data.get_dow_jones_nyc_yahoo_tickers()
    return list_of_tickers


def get_dow_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_dow_tickers()
    df.loc[:, 'indices'] = 'DOW JONES'
    return df


def get_nasdaq_100_tickers():
    stock_data = PyTickerSymbols()
    list_of_tickers = stock_data.get_nasdaq_100_nyc_yahoo_tickers()
    return list_of_tickers


def get_nasdaq_100_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_nasdaq_100_tickers()
    df.loc[:, 'indices'] = 'NASDAQ 100'
    return df


def get_nasdaq_tickers():
    list_of_tickers = si.tickers_nasdaq()
    return list_of_tickers


def get_nasdaq_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_nasdaq_tickers()
    df.loc[:, 'indices'] = 'NASDAQ'
    return df


def get_sp_500_tickers():
    stock_data = PyTickerSymbols()
    list_of_tickers = stock_data.get_sp_500_nyc_yahoo_tickers()
    return list_of_tickers


def get_sp_500_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_sp_500_tickers()
    df.loc[:, 'indices'] = 'S&P 500'
    return df


def get_sp_100_tickers():
    stock_data = PyTickerSymbols()
    list_of_tickers = stock_data.get_sp_100_nyc_yahoo_tickers()
    return list_of_tickers


def get_sp_100_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_sp_100_tickers()
    df.loc[:, 'indices'] = 'S&P 100'
    return df


def get_ftse_100_tickers():
    stock_data = PyTickerSymbols()
    list_of_tickers = stock_data.get_ftse_100_nyc_yahoo_tickers()
    return list_of_tickers


def get_ftse_100_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_ftse_100_tickers()
    df.loc[:, 'indices'] = 'FTSE 100'
    return df


def get_dax_tickers():
    stock_data = PyTickerSymbols()
    list_of_tickers = stock_data.get_dax_frankfurt_yahoo_tickers()
    return list_of_tickers


def get_dax_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_dax_tickers()
    df.loc[:, 'indices'] = 'DAX'
    return df


def get_russell_1000_tickers():
    with open('./data/russell_1000_tickers.json') as symbols_json:
        symbols = json.load(symbols_json)
        return symbols


def get_russell_1000_df():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    df['ticker_yahoo'] = get_russell_1000_tickers()
    df.loc[:, 'indices'] = 'RUI'  # russell_1000
    return df


def get_tickers_json(filename=None):
    with open(f'./data/{filename}') as symbols_json:
        symbols = json.load(symbols_json)
        return symbols


def get_tickers_csv(filename=None):
    df = pd.read_csv(f'./data/{filename}')
    return df.ticker.tolist()


def get_nikkei_tickers():
    # with open('./data/nikkei_tickers.json') as symbols_json:
    #     symbols = json.load(symbols_json)
    #     return symbols
    raise NotImplementedError


def get_all_tickers():
    df = pd.DataFrame(columns=info_stocks_df_columns)
    # df = df.append(get_dax_df(), ignore_index=True)
    # df = df.append(get_sp_100_df(), ignore_index=True)
    df = df.append(get_sp_500_df(), ignore_index=True)
    # df = df.append(get_dow_df(), ignore_index=True)
    # df = df.append(get_ftse_100_df(), ignore_index=True)
    # df = df.append(get_nasdaq_df(), ignore_index=True)
    # df = df.append(get_nasdaq_100_df(), ignore_index=True)
    # df = df.append(get_ftse_100_df(), ignore_index=True)
    # df = df.append(get_russell_1000_df(), ignore_index=True)

    df.drop_duplicates(subset=['ticker_yahoo'], keep='first', inplace=True)
    df.set_index('ticker_yahoo', inplace=True)

    return df


# def get_all_tickers_info():
#     df = pd.DataFrame(columns=info_stocks_df_columns)
#     df = df.append(get_sp_500_df(), ignore_index=True)
#     # df = df.append(get_dax_df(), ignore_index=True)
#     # df = df.append(get_sp_100_df(), ignore_index=True)
#     # df = df.append(get_sp_500_df(), ignore_index=True)
#     # df = df.append(get_dow_df(), ignore_index=True)
#     # df = df.append(get_ftse_100_df(), ignore_index=True)
#     # df = df.append(get_nasdaq_df(), ignore_index=True)
#     # df = df.append(get_nasdaq_100_df(), ignore_index=True)
#     # df = df.append(get_ftse_100_df(), ignore_index=True)
#     # df = df.append(get_russell_1000_df(), ignore_index=True)

#     df.drop_duplicates(subset=['ticker_yahoo'], keep='first', inplace=True)
#     df.set_index('ticker_yahoo', inplace=True)

#     df1.set_index('ticker_yahoo', inplace=True)
#     df2 = pd.concat([df, df1], axis=1, sort=False)
#     return df2

def get_ticker_info(df):
    asset_dict = {}
    df1 = pd.DataFrame()

    for ticker in df.index:
        logging.info('Processing:{}'.format(ticker))
        try:
            ticker_yf = yf.Ticker(ticker)
        except Exception as e:

            ticker_yf = None
            logging.error(e)
            asset_dict = None

        if ticker_yf is not None:
            asset_dict['ticker_yahoo'] = ticker
            asset_dict['sector'] = ticker_yf.info['sector']
            asset_dict['industry'] = ticker_yf.info['industry']

            asset_dict['exchange'] = ticker_yf.info['exchange']
            asset_dict['market_cap'] = ticker_yf.info['marketCap']

            asset_dict['symbol'] = ticker_yf.info['symbol']
            asset_dict['avg_volume'] = ticker_yf.info['volume']
            asset_dict['short_name'] = ticker_yf.info['shortName']
            asset_dict['currency'] = ticker_yf.info['currency']
            asset_dict['country'] = ticker_yf.info['country']
            logging.info('Adding:{}'.format(asset_dict))
            df1 = df1.append(pd.Series(asset_dict), ignore_index=True)

    return df1


def get_ticker_info_yf(ticker):
    logging.info('Downloading yfinance info :{}'.format(ticker))
    try:
        ticker_yf = yf.Ticker(ticker)
    except Exception as e:
        logging.error(e)
        ticker_yf = None

    if ticker_yf is not None:
        ticker_info = ticker_yf.info

    return ticker_info


def get_ticker_info_yahoo_fin():
    raise NotImplementedError


def get_sp500_tickers_from_wikipedia(selectedsector):
    resp = requests.get(
        'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})

    tickers = []
    industries = []
    sub_industries = []

    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        industry = row.findAll('td')[3].text
        sub_industry = row.findAll('td')[4].text

        tickers.append(ticker)
        industries.append(industry)
        sub_industries.append(sub_industry)

    tickers = list(map(lambda s: s.strip(), tickers))
    industries = list(map(lambda s: s.strip(), industries))

    tickerdf = pd.DataFrame(tickers, columns=['ticker'])
    sectordf = pd.DataFrame(industries, columns=['industry'])
    sub_sectordf = pd.DataFrame(sub_industries, columns=['sub_industry'])

    sp500_df = pd.concat([tickerdf, sectordf, sub_sectordf], axis=1)

    filtersector = sp500_df.loc[sp500_df['sub_industry'] == selectedsector]

    listoftickers = filtersector['ticker'].tolist()
    return listoftickers, sp500_df


if __name__ == '__main__':
    # # print(get_dow_jones_tickers())
    # df = print(get_sp500_tickers())
    print(get_nasdaq_tickers())
    df = get_nasdaq_df()
    df.to_json('nasdaq.json')
    # print(get_tickers_csv('US_ETF.csv'))
    # print(get_nasdaq_df().head())
    # print(get_nasdaq_100_df().head())
    # print(get_sp_100_df().head())
    # print(get_sp_500_df().head())
    # print(get_ftse_100_df().head())
    # print(get_dax_df().head())
    # print(get_russell_1000_df().head())
    # print(get_ticker_info_yf('AAPL'))
    # df = get_all_tickers()
    # df.to_json('all_tickers.json')

from os import path

crypto_data_path = './data/crypto_data'
etf_data_path = './data/etf'
historical_prices_path = './data/companies'
rules_path = './data/rules'
trading_days_path = './data/trading_days.json'
company_folder_path = './data/companies/{}'  # TODO: check if really needed
discord_image_path = './discord_alert/img/'
test_thread = './data/test'
# One placeholder is for username
plot_save_folder = './data/plots/{}'

colors = ['green', 'orange', 'purple', 'black', 'brown', 'black', 'black', 'black']


# Important lookback periods for technical indicators

# ma_periods = list(range(3, 23)) + [50, 100, 200]
ma_periods = [9, 22, 50, 100, 200]
ma_periods = ['{}d'.format(p) for p in ma_periods]
rsi_periods = [14]
rsi_periods = ['{}d'.format(p) for p in rsi_periods]

fibonacci_periods = ['5y', '3y', '1y', '6m', '3m', '1m', 'ytd']
trendline_periods = ['5y', '3y', '1y', '6m', '3m', '1m', 'ytd']
vp_periods = ['5y', '3y', '1y', '6m', '3m', '1m', '2w', '1w', 'ytd']
support_resistance_periods = ['5y', '3y', '1y', '6m', '3m', '1m', 'ytd']


''' ------------------------------------ Other ----------------------------------'''

lookback_to_days = {'5y': 1300, '4y': 1040, '3y': 780, '2y': 520, '1y': 260,
                    '6m': 130, '3m': 65, '1m': 22,
                    '52w': 260, '26w': 130, '13w': 65, '4w': 20, '2w': 10, '1w': 5}

for i in range(3, 23):
    lookback_to_days[str(i) + 'd'] = i

important_lookbacks = ['5y', '4y', '3y', '2y', '1y', '6m', '3m', '1m', '52w', '26w', '13w', '4w', '2w', '1w', 'ytd',
                       '50d', '65d', '100d', '130d', '200d', '260d']

for i in range(3, 23):
    important_lookbacks.append(str(i) + 'd')



'--------------------------------------------------------------------------------'

'''
Author: Shun-ping Chiu (jyou.ping@gmail.com)
'''

__author__ = 'schiu'

import datetime as dt
import numpy as np
import math as mt
import pandas as pd
import sys
import argparse

from datetime import datetime
from datetime import date

TRADE_PER_YEAR_DAY_COUNT = 252
TRDAE_PER_MONTH_DAY_COUNT = 22
TRADE_PER_WEEK_DAY_COUNT = 5

def data_reader(ls_symbols, startdate, enddate):
    # We need closing prices so the timestamp should be hours=16.
    # Keys to be read from the data, it is good to read everything in one go.
    ls_keys = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

    start_date_str = datetime.strptime(startdate, '%Y%m%d').strftime('%Y-%m-%d')
    end_date_str = datetime.strptime(enddate, '%Y%m%d').strftime('%Y-%m-%d')
    total_days = 0
    symbol_df = {}
    for symbol in ls_symbols:
        df = pd.read_csv('./data/%s.csv' % symbol)
        cond1 = (df['Date'] <= end_date_str) & (df['Date'] >= start_date_str)
        filtered_df = df[cond1]
        total_days = len(filtered_df.index)
        symbol_df[symbol] = filtered_df
    return (symbol_df, total_days)


def get_invest_share_count(symbol_df, symbol_ratio, initial_money, day = 0):
    '''
    Calculate the share count for ecah invesement, if day = 0, we use the first day as the investment
    '''
    symbol_count = {}
    for symbol in symbol_df.keys():
        adj_close = symbol_df[symbol]['Adj Close'].tolist()[day]
        quantity = initial_money * symbol_ratio[symbol] / adj_close
        symbol_count[symbol] = quantity
    
    return symbol_count



def get_daily_return0(prices):
    prices_df = pd.Series(prices)
    return np.nan_to_num((prices_df / prices_df.shift(1) - 1).to_numpy())

def get_daily_value(symbol_df, symbol_count, day = 0):
    daily_values = pd.Series()
    for symbol in symbol_df.keys():
        adj_close = symbol_df[symbol]['Adj Close'].multiply(symbol_count[symbol])
        daily_values = daily_values.add(adj_close, fill_value=0)

    value_array = np.array(daily_values[day:])
    return value_array

def get_daily_return(daily_values, base_values):
    na_normalized_values = np.divide(daily_values, base_values)
    daily_return = get_daily_return0(na_normalized_values)
    '''
    Debug for daily return
    merged = np.concatenate((daily_values, base_values, na_normalized_values, daily_return), axis=0).reshape(4, -1)
    merged = np.transpose(merged)
    pd.set_option('display.max_rows', 10001)
    print (pd.DataFrame(data=merged, columns=['daily_values', 'base_values', 'na_normalized_values', 'daily_return']))
    '''
    return daily_return, na_normalized_values

def calculate_metrics(daily_return, total_returns):
    avg_daily_return = np.average(daily_return)
    std = np.std(daily_return)
    sharpe = avg_daily_return/std * mt.sqrt(TRADE_PER_YEAR_DAY_COUNT)
    total_return = total_returns[-1]/total_returns[0] - 1
    average_yearly = pow(total_returns[-1]/total_returns[0], 1.0/(total_returns.shape[0]/TRADE_PER_YEAR_DAY_COUNT)) -1
    return 'total return %f %%, sharpe %f, std %f, average_daily %f %%, average_yearly %f %%' % (total_return * 100, sharpe, std, 100 * avg_daily_return, 100 * average_yearly);



def portfoilo_simulation(symbol_ratio, startdate, enddate, invest_type):
    years = 10
    print ("portfolio %s" % symbol_ratio)
    everytime_money = 10000

    (symbol_df, total_days) = data_reader(symbol_ratio.keys(), startdate, enddate)


    each_daily_value = []   # daily value for each investment
    each_base_value = []    # base value for each investment added up 
    total_money = 0

    if invest_type =='ONCE':
        periods = range(0, 1)
    elif invest_type == 'MONTHLY':
        periods = range(1, total_days, TRDAE_PER_MONTH_DAY_COUNT)
    elif invest_type == 'WEEKLY':
        periods = range(1, total_days, TRADE_PER_WEEK_DAY_COUNT)
    else:
        periods = range(0, 1)

    for i in periods:   # generate series for each investment, we iterate from the last element
        symbol_count = get_invest_share_count(symbol_df, symbol_ratio, everytime_money, -i)
        daily_value = get_daily_value(symbol_df, symbol_count, -i)
        each_daily_value.append(daily_value)
        base_value = np.full(daily_value.shape[0], everytime_money)
        each_base_value.append(base_value)

    total_daily_value = np.array([])
    # Some sample -1st:           10000 100100 100200
    #             -2st:    10000  10010 100200 100300
    # total:               10000  20010 200300 200500    
    for daily_value in each_daily_value:
        shape1 = total_daily_value.shape
        shape2 = daily_value.shape
        total_daily_value.resize(shape2)
        total_daily_value = np.roll(total_daily_value, shape2[0] - shape1[0])
        total_daily_value = np.add(total_daily_value, daily_value)

    total_base_money = np.array([])
    for daily_base in each_base_value:
        shape1 = total_base_money.shape
        shape2 = daily_base.shape
        total_base_money.resize(shape2)
        total_base_money = np.roll(total_base_money, shape2[0] - shape1[0])
        total_base_money = np.add(total_base_money, daily_base)

    weighted_daily_return, weighted_daily_value = get_daily_return(total_daily_value, total_base_money)
    print ('Overall: %s' % calculate_metrics(weighted_daily_return, weighted_daily_value))
    print ('Past 1 years: %s' % calculate_metrics(weighted_daily_return[-TRADE_PER_YEAR_DAY_COUNT:], weighted_daily_value[-TRADE_PER_YEAR_DAY_COUNT:]))
    print ('Past 3 years: %s' % calculate_metrics(weighted_daily_return[-TRADE_PER_YEAR_DAY_COUNT*3:], weighted_daily_value[-TRADE_PER_YEAR_DAY_COUNT*3:]))
    print ('Past 5 years: %s' % calculate_metrics(weighted_daily_return[-TRADE_PER_YEAR_DAY_COUNT*5:], weighted_daily_value[-TRADE_PER_YEAR_DAY_COUNT*5:]))
    print ('\n')


def portfolio_processor(file, startdate, enddate, invest_type):
    print ('open %s ....' % file)
    with open(file) as portfolio_file:
        symbol_ratio = {}
        for symbol_line in portfolio_file:
            array = symbol_line.split(',')
            symbol = array[0]
            ratio = float(array[1])
            symbol_ratio[symbol] = ratio
        portfoilo_simulation(symbol_ratio, startdate, enddate, invest_type)

if __name__ == '__main__':
    today = date.today().strftime('%Y%m%d')
    INVEST_OPTION = ['ONCE', 'MONTHLY', 'WEEKLY']
    parser = argparse.ArgumentParser(description='Calculate sharpe ratio for investment')
    parser.add_argument('-f', '--portfolio', nargs='+', type=str, help='Files that store the investment symbol, separate by line, format: SYMBOL, RATIO', required=True)
    parser.add_argument('-s', '--start', type=str, help='start time for simluation, default = 20070101', default='20070101')
    parser.add_argument('-e', '--end', type=str, help="end time for simluation, default = today", default=today)
    parser.add_argument('-t', '--invest_type', type=str, help='investment type', choices=list(INVEST_OPTION))
    args = parser.parse_args()
    for file in args.portfolio:
        portfolio_processor(file, args.start, args.end, args.invest_type)

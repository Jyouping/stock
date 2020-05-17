'''
Author: Shun-ping Chiu (jyou.ping@gmail.com)
'''

__author__ = 'schiu'

import urllib
import urllib.request
import datetime
import argparse
import os
import sys

from datetime import datetime
from datetime import date
from tqdm import tqdm

DATA_PATH = './data/'
START_TIME = 1151712000
END_TIME = 1584489600

def get_data(data_path, ls_symbols, starttime, endtime):

    # Create path if it doesn't exist
    if not (os.access(data_path, os.F_OK)):
        os.makedirs(data_path)

    # utils.clean_paths(data_path)   

    miss_ctr=0; #Counts how many symbols we could not get
    for symbol in tqdm(ls_symbols):
        # Preserve original symbol since it might
        # get manipulated if it starts with a "$"
        symbol_name = symbol
        if symbol[0] == '$':
            symbol = '^' + symbol[1:]

        symbol_data = []
        # print "Getting {0}".format(symbol)
        
        try:
            url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=1d&events=history" % (symbol, int(starttime), int(endtime))
            url_get= urllib.request.urlopen(url)
            
            header = url_get.readline()
            symbol_data.append (url_get.readline())
            while (len(symbol_data[-1]) > 0):
                symbol_data.append(url_get.readline())
            
            symbol_data.pop(-1) #The last element is going to be the string of length zero. We don't want to write that to file.
            #now writing data to file
            f = open (data_path + symbol_name + ".csv", 'w')
            
            #Writing the header
            f.write (header.decode('utf-8'))
            
            while (len(symbol_data) > 0):
                f.write (symbol_data.pop(0).decode('utf-8'))
             
            f.close();    
                        
        except urllib.error.HTTPError:
            miss_ctr += 1
            print ("Unable to fetch data for stock: {0} at {1}".format(symbol_name, url))
        except urllib.error.URLError:
            miss_ctr += 1
            print ("URL Error for stock: {0} at {1}".format(symbol_name, url))
            
    print ("All done. Got {0} stocks. Could not get {1}".format(len(ls_symbols) - miss_ctr, miss_ctr))

def read_symbols(s_symbols_file):

    ls_symbols=[]
    file = open(s_symbols_file, 'r')
    for line in file.readlines():
        str_line = str(line)
        if str_line.strip(): 
            ls_symbols.append(str_line.strip())
    file.close()
    
    return ls_symbols  

def main():
    today = date.today().strftime('%Y%m%d')
    parser = argparse.ArgumentParser(description='Pull data from Yahoo finance')
    parser.add_argument('-f', '--portfolio', type=str, help='Files that store the investment symbol, separate by line, format: SYMBOL, RATIO', required=True)
    parser.add_argument('-s', '--start', type=str, help="start time for data pull, default = 20070101   ", default="20070101")
    parser.add_argument('-e', '--end', type=str, help="end time for data pull, default = today", default=today)
    args = parser.parse_args()
    
    path = DATA_PATH
    ls_symbols = read_symbols(args.symbol_files)
    start_date_str = args.start
    end_date_str = args.end
    starttime = (datetime.strptime(start_date_str, '%Y%m%d') - datetime(1970,1,1)).total_seconds()
    endtime = (datetime.strptime(end_date_str, '%Y%m%d') - datetime(1970,1,1)).total_seconds()
    print('Pull data for Symbols: %s, start date %s, end date %s' % (', '.join(ls_symbols), start_date_str, end_date_str)) 

    get_data(path, ls_symbols, starttime, endtime)

if __name__ == '__main__':
    main()

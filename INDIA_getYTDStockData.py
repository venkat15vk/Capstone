#!/Users/VK/anaconda/bin/python

from nsepy import get_history, get_index_pe_history
from datetime import date
import pandas as pd
import sys
import datetime

ticker = str(sys.argv[1])
now = datetime.datetime.now()
from_year = 2014
from_month = 1
from_date = 1

to_year = int(now.strftime("%Y"))
to_month = int(now.strftime("%m"))
to_date = int(now.strftime("%d"))


mydata_df = get_history(symbol=ticker,
                    start=date(from_year,from_month,from_date), 
                    end=date(to_year,to_month,to_date))

filename = '/Users/VK/Desktop/UChicago/Stocks/data/StockPrices/India/' + ticker + '.csv'

mydata_df.to_csv(filename)

#!/Users/VK/anaconda/bin/python

import yahoo_finance
import pandas as pd
import sys
import datetime

#ticker = str(sys.argv[1])
ticker = "AAPL"
filename = '../../data/StockPrices/US/1_day_5_years/' + ticker + '.csv'

now = datetime.datetime.now()
from_date = "2013-01-01"
to_date = now.strftime("%Y-%m-%d")


symbol = yahoo_finance.Share(ticker)
mydata = symbol.get_historical(from_date, to_date)
mydata_df = pd.DataFrame(mydata)

#final_format = ticker + ',' + mydata_df['Date'] + ',' + mydata_df['Open'] + ',' + mydata_df['High'] + ',' + mydata_df['Low'] + ',' + mydata_df['Close'] + ',' + mydata_df['Volume']
final_format = ticker + ',' +  ',' + mydata_df['High'] 
final_format.to_csv(filename)

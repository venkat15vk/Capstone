#!/Users/VK/anaconda/bin/python

import pandas_datareader.data as wb
import datetime
import csv
import pandasql as pdsql

tickers = []

if __name__ == "__main__":
	etfFile = './etfs.txt'
	etf = open(etfFile,'r')

	for row in etf:
		ticker = row.strip()
		web_df=None

		try:
			web_df = wb.DataReader(ticker, 'yahoo',
                       		datetime.date(2017,12,1),
                       		datetime.date(2018,1,22))

			print (ticker)

			query1 = """
					SELECT count(*) from web_df where
						web_df.Volume > 100000 AND
						(web_df.High - web_df.Low) > .8 AND
						(web_df.High*500) < 15000
						and COUNT > 20
				"""
			print  (pdsql.sqldf(query1, globals()))

		except:
			print ("Cannot get values for ", ticker)

# volume
# high*500 should not exceed 15000
# high minus low should be $1
#       print total number of days vs how many times threshold breached
# print overall trend
        # is it moving up or down
        # what has been its max value in past 1 year
        # what is its biggest drop in past year

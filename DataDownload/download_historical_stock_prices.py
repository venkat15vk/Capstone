#!/Users/VK/anaconda/bin/python

import pandas_datareader.data as web
import csv
import datetime as dt
import re
import pymysql
from sqlalchemy import create_engine

def load_historical_stockprices_to_csv(cursor,engine):

	from_date = dt.datetime(2013,1,1)
	to_date = dt.datetime(2017,8,8)

	cursor.execute("SELECT Ticker FROM US_STOCKS.SandP500 order by Ticker")

	for (result) in cursor:
		stock = result[0]
		mydata_df = web.DataReader(stock,'google',from_date,to_date)
		mydata_df['Stock'] = stock.upper()
		mydata_df = mydata_df[['Stock', 'Open', 'High', 'Low', 'Close', 'Volume']]
		filename = '../../data/StockPrices/US/1_day_5_years/' + stock + '.csv'
		mydata_df.to_csv(filename)
		print ("load data local infile '/Users/VK/Desktop/UChicago/Capstone/data/StockPrices/US/1_day_5_years/" + stock + ".csv' into table US_STOCKS.StockPrices fields terminated by ','")


def connect_to_db():
	fh = open("/Users/VK/.db_details.txt");
	user = None
	password = None
	for line in fh:
		user_match = re.search('DBUSER=(.*)',line.strip())
		pwd_match = re.search('DBPASSWORD=(.*)',line.strip())
		if user_match:
			user = user_match.group(1) 
		else: 
			password = pwd_match.group(1)	
	db = pymysql.connect("localhost",user,password,"US_STOCKS")
	cursor = db.cursor()
	cursor.execute("DELETE from US_STOCKS.StockPrices")
	db.commit()
	engine = create_engine('mysql+pymysql://'+user+':'+password+'@localhost:3306/US_STOCKS', echo=False)
	return (engine,cursor)

def main():
	(engine,cursor) = connect_to_db()
	load_historical_stockprices_to_csv(cursor,engine)

main()



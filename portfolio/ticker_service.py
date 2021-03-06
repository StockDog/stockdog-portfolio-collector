import pymysql.cursors
from iexfinance.stocks import Stock

# Past date data collection needs more work here
def collectIEXTickersPrice(mysql_secret, iextoken):
	connection = pymysql.connect(host=mysql_secret['host'],user=mysql_secret['username'],password=mysql_secret['password'],db=mysql_secret['dbname'], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
	try:
		with connection.cursor() as cursor:
			tickerPrices = {}
			tickerCount = getTickersCount(cursor);
			print("Total number of tickers in the system ",tickerCount )
			pageSize = 50;
			for i in range(1, int(tickerCount), pageSize):
				tickers = getTickerSymbols(i,pageSize,cursor)
				try:
					batchCollection = Stock(tickers, output_format='pandas',token=iextoken)
					data = batchCollection.get_quote()
					for ticker in tickers:
						tickerPrices[ticker] = data[ticker]["latestPrice"];
				except Exception as e:
					print(e)
			for key,value in tickerPrices.items():
				cursor.execute('''INSERT INTO TickerHistory(ticker,sharePrice) VALUES(%s,%s)''',(key,value))
			connection.commit();
			return tickerPrices
	finally:
		connection.close()
	return ""

def getTickersCount(cursor):
	cursor.execute("SELECT count(ticker) from PortfolioItem pi left join Portfolio p on p.id=pi.portfolioId left join League l on l.id=p.leagueId where DATEDIFF(l.start, curdate())<0 and DATEDIFF(l.end,  curdate()) >=0")
	tickerCount = cursor.fetchone()
	return next(iter(tickerCount.values()))

def getTickerSymbols(offset,size,cursor):
	ignoreSymbols = ["BLIN","EVGBC","IVENC","ZTEST","EVLMC","IVFGC","EVSTC","IVFVC",]
	tickerArray = []
	#most portfolios are already done so we dont have to collect.
    #cursor.execute("SELECT symbol from Ticker order by symbol asc limit "+str(offset)+","+str(size))
	cursor.execute("SELECT ticker from PortfolioItem pi left join Portfolio p on p.id=pi.portfolioId left join League l on l.id=p.leagueId where DATEDIFF(l.start, curdate())<0 and DATEDIFF(l.end,  curdate()) >=0")

	tickers = cursor.fetchall()
	for ticker in tickers:
		if ticker["ticker"].strip() not in ignoreSymbols:
			tickerArray.append(ticker["ticker"])
	return tickerArray

# Past date data collection needs more work here
def collectIEXTickerPrice(ticker,cursor,iextoken):
	print("Collecting the ticker price from IEX : ",ticker)
	currentPrice = {}
	tickerPrice = Stock(ticker, output_format='pandas',token=iextoken)
	price = tickerPrice.get_quote()[ticker]["latestPrice"]
	cursor.execute('''INSERT INTO TickerHistory(ticker,sharePrice) VALUES(%s,%s)''',(ticker,price))
	currentPrice[ticker]=price
	return currentPrice[ticker]

#For now we are only doing for the given day after close
def getTickerPrice(ticker,mysql_secret,iextoken):
	connection = pymysql.connect(host=mysql_secret['host'],user=mysql_secret['username'],password=mysql_secret['password'],db=mysql_secret['dbname'], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
	try:
		with connection.cursor() as cursor:
			cursor.execute("SELECT sharePrice from TickerHistory where ticker=%s order by dateCreated desc limit 1",(ticker))
			tickerPrice = cursor.fetchone()
			price = 0.0
			if tickerPrice != None:
				price = next(iter(tickerPrice.values()))
			else:
				price = collectIEXTickerPrice(ticker,cursor,iextoken)
			connection.commit()
			return price
	finally:
		connection.close()
	return ""

from ticker_service import getTickerPrice
from decimal import *
import pymysql.cursors

# Past date portfolio history cals needs more work here
def calculatePortfolioHistories(mysql_secret,iextoken):
	connection = pymysql.connect(host=mysql_secret['host'],user=mysql_secret['username'],password=mysql_secret['password'],db=mysql_secret['dbname'], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
	try:
		with connection.cursor() as cursor:
			portfolioCount = getPortfolioCount(cursor);
			print("Total number of portfolios in the system ",portfolioCount )
			pageSize = 1;
			for i in range(0, int(portfolioCount),pageSize):
				portfolioWithItems = getPortfolioWithItems(cursor,i,pageSize)
				try:
					calculatePortfolioHistory(cursor,portfolioWithItems,mysql_secret,iextoken);
				except Exception as e:
					print(e)
			connection.commit();
			return portfolioCount
	finally:
		connection.close()
	return ""

def calculatePortfolioHistory(cursor,portfolioWithItems,mysql_secret,iextoken):
	portfolioItems = portfolioWithItems["portfolioItems"];
	portfolioId = portfolioWithItems["portfolio"]["id"]
	value = portfolioWithItems["portfolio"]["buyPower"];
	for portfolioItem in portfolioItems:
		shareCount = portfolioItem["shareCount"];
		ticker = portfolioItem["ticker"];
		price = getTickerPrice(ticker,mysql_secret,iextoken)
		currentValue = shareCount * price;
		value = value + Decimal(currentValue);
	cursor.execute('''INSERT INTO PortfolioHistory(portfolioId,datetime,value) VALUES(%s,NOW(),%s)''',(portfolioId,value))
	return None;


def getPortfolioCount(cursor):
	cursor.execute("SELECT count(*) from Portfolio p left join League l on l.id=p.leagueId where  DATEDIFF(l.end,  curdate()) >=0 ")
	portfolioCount = cursor.fetchone();
	return next(iter(portfolioCount.values()))

def getPortfolioWithItems(cursor,offset,size):
	portfolioWithItems = {};
	cursor.execute("SELECT * from Portfolio p left join League l on l.id=p.leagueId where DATEDIFF(l.end,  curdate()) >=0 order by id limit "+str(offset)+","+str(size))
	portfolio = cursor.fetchone()
	cursor.execute("SELECT * from PortfolioItem where portfolioId=%s",(portfolio["id"]))
	portfolioItems = cursor.fetchall()
	portfolioWithItems["portfolio"] = portfolio;
	portfolioWithItems["portfolioItems"] = portfolioItems;
	return portfolioWithItems;

import dbfuncs as db
######### Packages ########
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import yaml
from datetime import datetime

## Read Config
config = yaml.safe_load(open("config.yaml"))
databasename = config["postgreSQL"]["database"]

def importRetailSpendingFromPostgreSQL(postgresStr):
    spendingDataSQL = """SELECT date_part('year', date_day::date) as year,
       date_part('week', date_day::date) AS weekly,
       AVG(total) as total, AVG(acf) as acf, AVG(aer) as aer,
       AVG(apg) as apg, AVG(grf) as grf, AVG(hcs) as hcs,
       AVG(tws) as tws, AVG(retail) as retail, AVG(retail_nogroc) as retail_nogroc           
       FROM cosp
       GROUP BY year, weekly
       ORDER BY year, weekly;"""
    # Create an engine instance
    engine = create_engine(postgresStr)
    # Connect to PostgreSQL server
    dbConnection = engine.connect()
    # Read data from PostgreSQL database table and load into a DataFrame instance
    retailSpendingDF = pd.read_sql(spendingDataSQL, dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    # Close the database connection
    dbConnection.close()
    return retailSpendingDF

def importTwitterSentimentFromPostgreSQL(postgresStr):
    twitterSentimentSQL = """select * from public.tweetsentiment"""
    # Create an engine instance
    engine = create_engine(postgresStr)
    # Connect to PostgreSQL server
    dbConnection = engine.connect()
    # Read data from PostgreSQL database table and load into a DataFrame instance
    twitterSentimentDF = pd.read_sql(twitterSentimentSQL, dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    # Close the database connection
    dbConnection.close()
    return twitterSentimentDF


def importStockPricesFromPostgreSQL(postgresStr):
    stockPricesSQL = """SELECT * FROM close_price_change"""
    # Create an engine instance
    engine = create_engine(postgresStr)
    # Connect to PostgreSQL server
    dbConnection = engine.connect()
    # Read data from PostgreSQL database table and load into a DataFrame instance
    stockPricesDF = pd.read_sql(stockPricesSQL, dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    # Close the database connection
    dbConnection.close()
    return stockPricesDF

postgresStr = db.getPostgresPsycopgString(config)
retailSpendingDF = importRetailSpendingFromPostgreSQL(postgresStr)
twitterSentDF = importTwitterSentimentFromPostgreSQL(postgresStr)
stockPricesDF = importStockPricesFromPostgreSQL(postgresStr)

twitterSentDF['tweetdate'] = pd.to_datetime(twitterSentDF['tweetdate']).dt.date
retailSpendingDF['date'] = pd.to_datetime(retailSpendingDF.apply(lambda x: datetime.strptime('{0} {1} 1'.format(int(x['year']), int(x['weekly'])), '%Y %W %w'), axis=1)).dt.date
stockPricesDF['Date'] = pd.to_datetime(stockPricesDF['Date'],utc=True).dt.date

combinedDf = pd.merge(twitterSentDF, retailSpendingDF, left_on='tweetdate', right_on='date', how = 'inner')
combinedDf2 = pd.merge(combinedDf, stockPricesDF, left_on='tweetdate', right_on='Date', how = 'inner')


combinedFieldsDf = combinedDf2[['Date','avgsentimentscore','sentimentscoretotal',
                                'total', 'acf', 'aer', 'apg', 'grf', 'hcs', 'tws', 'retail', 'retail_nogroc',
                                '^SP500-255040', '^SP500-30', '^SP500-35', '^SP500-60']]

#Run Pearson correlation on all columns of the DataFrame.
correlationsDf = combinedFieldsDf.corr()
correlationsDf

print(correlationsDf)

### Plot 
import plotters as pttrs

# plot Consumer Spending per Sector
pttrs.plot_consumer_spending(retailSpendingDF)

# plot stock indices close change
pttrs.plot_close_change(stockPricesDF)

# plot Sentiment
pttrs.generateSentimentChart(twitterSentDF)


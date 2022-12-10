import dbfuncs as db
######### Packages ########
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
from sodapy import Socrata
import matplotlib as mpl
import yaml
import dbfuncs as db
from datetime import datetime

## Read Config
config = yaml.safe_load(open("config.yaml"))
databasename = config["postgreSQL"]["database"]

#Fionn

avg_week_year = """SELECT date_part('year', date_day::date) as year,
       date_part('week', date_day::date) AS weekly,
       AVG(total) as total, AVG(acf) as acf, AVG(aer) as aer,
       AVG(apg) as apg, AVG(grf) as grf, AVG(hcs) as hcs,
       AVG(tws) as tws, AVG(retail) as retail, AVG(retail_nogroc) as retail_nogroc           
       FROM cosp
       GROUP BY year, weekly
       ORDER BY year, weekly;"""

try:
    conn = db.getDBConnection(config,databasename)
    cosp_df = sqlio.read_sql_query(avg_week_year, conn)
    cosp_df.to_csv('all_avg_week_year.csv')
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(conn):
        conn.close()
print("test")

# cosp_df.info()
# print(cosp_df.head())

#Sean


avg_week_year = """SELECT *           
       FROM close_price_change
       ;"""

try:
    conn = db.getDBConnection(config,databasename)
    changeDf = sqlio.read_sql_query(avg_week_year, conn)
   
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(conn):
        conn.close()

# changeDf.info()
# print(changeDf.head())

#Donal
avg_week_year = """SELECT *           
       FROM tweetsentiment
       ;"""

try:
    conn = db.getDBConnection(config,databasename)
    dfTwitterSentiment = sqlio.read_sql_query(avg_week_year, conn)
   
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(conn):
        conn.close()

# dfTwitterSentiment.info()
# print(dfTwitterSentiment.head())

#dfTwitterSentiment

cosp_df['date'] = pd.to_datetime(cosp_df.apply(lambda x: datetime.strptime('{0} {1} 1'.format(int(x['year']), int(x['weekly'])), '%Y %W %w'), axis=1),utc=True)

dfTwitterSentiment['tweetdate'] = pd.to_datetime(dfTwitterSentiment['tweetdate'],utc=True)


combinedDf = pd.merge(dfTwitterSentiment, cosp_df, left_on='tweetdate', right_on='date', how = 'inner')

print(combinedDf)

#changeDf.reset_index(inplace=True)
changeDf['Date'] = pd.to_datetime(changeDf['Date'],utc=True)
combinedDf2 = pd.merge(combinedDf, changeDf, left_on='tweetdate', right_on='Date', how = 'inner')

print(combinedDf2)

#Run Pearson correlation on all columns of the DataFrame.
correlationsDf = combinedDf2.corr()
correlationsDf.info()

#print(correlationsDf)


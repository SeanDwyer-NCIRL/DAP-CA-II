######### Packages ########
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
from sodapy import Socrata
import matplotlib.pyplot as plt
import yaml
import dbfuncs as db

## Read Config
config = yaml.safe_load(open("config.yaml"))
databasename = config["postgreSQL"]["database"]

########## Consumer Spending Percentage Change Downloaded and Converted to CSV ###########
# This is updated weekly
# See https://dev.socrata.com/foundry/data.ct.gov/xpjq-6wxn for details/code

# For Header Description:
# See https://data.ct.gov/Business/Percent-Change-in-Consumer-Spending-2020-2022/xpjq-6wxn

# Public dataset aunthentication is not required, see None
client = Socrata("data.ct.gov", None)

# Returned as JSON from API / converted to Python list of dictionaries by sodapy.
spend = client.get("xpjq-6wxn", limit=10000)

# Convert to pandas DataFrame
spend_df = pd.DataFrame.from_records(spend)

# index=false as index column causes issues in postgres down the line
spend_df.to_csv('consumer_spending.csv', index=False) 

# Added new column id (1,2,3..) to csv, this will be the key in postgres
spend_df = spend_df.assign(id = lambda x: range(1, 1 + len(spend_df)))

# Edit colum headers, column all (causes syntax error in psycopg/postgres) changed to total.
spend_df.columns =['statefips', 'date_day', 'total', 
                     'acf', 'aer', 'apg', 'grf', 'hcs', 'tws', 
                     'retail', 'retail_nogroc', 'id']
					
spend_df.to_csv("consumer_spending.csv", index=False)

#################### Import CSV to PostgresSQL ####################
# https://www.dataquest.io/blog/loading-data-into-postgres/

# Connect to postgres and create database (cospdb)
# try:
    # # conn = psycopg2.connect(user = config['postgreSQL']['username'],
        # # password = config['postgreSQL']['password'],
        # # host = config['postgreSQL']['hostname'],
        # # port = config['postgreSQL']['port'],
        # # database = "postgres")
    # conn = getDBConnection ('postgres')
    # conn.autocommit = True
    # cursor = conn.cursor()
    # cospdb = '''CREATE database cospdb;'''
    # cursor.execute(cospdb)
    # conn.close()
# except (Exception , psycopg2.Error) as dbError :
    # print ("Error:", dbError)
# finally:
    # if(conn):
        # conn.close()


# Create Table
cosp_table ="""
    CREATE TABLE IF Not EXISTS cosp(
    statefips text,
    date_day date,
    total float,
    acf float,
    aer float,
    apg float,
    grf float,
    hcs float,
    tws float,
    retail float,
    retail_nogroc float,
    id integer PRIMARY KEY
)
"""
try:
    conn = db.getDBConnection(config,databasename)
    cursor = conn.cursor()
    cursor.execute(cosp_table)
    conn.commit()
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(conn):
        conn.close()

# insert csv data in to table
try:
    conn = db.getDBConnection(config,databasename)
    cursor = conn.cursor()
    with open('consumer_spending.csv', 'r') as f:
        next(f) 
        cursor.copy_from(f, 'cosp', sep=',')
    conn.commit()
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(conn):
        conn.close()

############ Working on postgres database from python ############
# Average % change per week grouped by week and year
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


cosp_df.to_csv('all_avg_week_year.csv')

# try:
#     plt.plot(cosp_df['total'], label = 'All')
#     plt.plot(cosp_df['acf'], label = 'Accom. & Food Service')
#     plt.plot(cosp_df['aer'], label = 'Arts, Enter., Rec.')
#     plt.plot(cosp_df['apg'], label = 'Gen. Merch. & Apparel')
#     plt.plot(cosp_df['grf'], label = 'Groc. & Food Store')
#     plt.plot(cosp_df['hcs'], label = 'Health & Social Ass.')
#     plt.plot(cosp_df['tws'], label = 'Transport & Wareh.')
#     plt.plot(cosp_df['retail'], label = 'Retail Spending')
#     plt.plot(cosp_df['retail_nogroc'], label = 'Retail Excl. Groc.')
#     plt.ylabel('% Chnage', fontsize=14)
#     plt.xlabel('Week', fontsize=14)
#     plt.title('Consumer Spending per Sector', fontsize=16)
#     plt.legend()
# except (Exception) as Error :
#     print ("Error:", Error)
# finally:
#     plt.show()


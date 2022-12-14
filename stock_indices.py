# stock_indices.py
import numpy as np 
import pandas as pd 
import pandas_datareader.data as web
import yfinance as yf
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import yaml
import dbfuncs as db
import pymongo         


def get_mongo_db_smi_collection(hostname, port):
    client = pymongo.MongoClient(hostname, port)
    db = client.financials
    smi_coll = db.stock_market_indices
    return smi_coll

def initialize_collection_with_ticker_data(collection, data):
    collection.delete_many({})
    records = data.reset_index().to_dict(orient='records')
    collection.insert_many(records)
    return collection.count_documents({})

def replace_postgres_with_price_chg_history(df, postgresStr) :
    engine = create_engine(postgresStr)
    df.to_sql('close_price_change',engine,if_exists='replace')
                            
def get_ticker_history (symbol, start, end, interval):
     # Retrieve data from Yahoo! Finance
    tickerData = yf.Ticker(symbol)
    tickerDf = tickerData.history(start=start, end=end, interval=interval) 
    tickerDf['ticker'] = symbol 
    return tickerDf

def get_ticker_history_dr (symbol, start, end, interval):
    tickerData = web.get_data_yahoo(symbol, start=start, end=end,interval=interval)
    tickerData['ticker'] = symbol
    return tickerData
    
def get_major_stock_indices():
    df_list = pd.read_html('https://finance.yahoo.com/world-indices/')
    StockIdxs = df_list[0]
    return StockIdxs
    
def get_s_and_p_equities(interval):
    tickers = ['^SP500-255040','^SP500-60','^SP500-35','^SP500-30']

    stock_list = []
    for s in tickers:
        df = get_ticker_history(s,'2020-01-01','2022-11-30',interval)
        stock_list.append(df)

        # Concatenate all data
    msi = pd.concat(stock_list, axis = 0)
    msi = msi.reset_index()

    msi['ccBegin'] = '2020-01-01'
    return msi
 
def transform_ticker_data(msi):
    # Transform the data to be ticker column-wise
    ccBegin = msi.groupby(['Date', 'ticker'])['ccBegin'].first().unstack()
    # Fill null values with the values on the row before
    ccBegin = ccBegin.fillna(method='bfill')
    return ccBegin

## Lambdas 

def getRegion(ticker, cat_index):
    for k in cat_index.keys():
        if ticker in cat_index[k]:
            return k
        
def calcClosePC(ticker, val, begRef):
    start_val = begRef.loc[begRef.ticker == ticker, 'Close'].values[0]
    return (val/start_val - 1) * 100
    
## Plot functions
def plot_close_change(cat_idx, ccBegin, si_dict):
    plt.style.use('classic') 
    fig, axes = plt.subplots(1,1, figsize=(12, 8),sharex=True)

    pallete = ["red", "green", "blue","yellow"]
    for i, k in enumerate(cat_idx.keys()):
    # foreach Index  
        ax = axes
        for j,t in enumerate(cat_idx[k]):      
            ax.plot(ccBegin.index, ccBegin[t], marker='', linewidth=1, color = pallete[j])
            ax.legend([si_dict[t] for t in cat_idx[k]], loc='upper left', fontsize=7)
            ax.set_title(k, fontweight='bold')

    fig.text(0.5,0, "Year", ha="center", va="center", fontweight ="bold")
    fig.text(0,0.5, "Close Price Change (%)", ha="center", va="center", rotation=90, fontweight ="bold")
    fig.suptitle("Close Price Change for S&P 500 Equites Indices 2020-2022", fontweight ="bold",y=1.05, fontsize=14)
    fig.tight_layout()
    ax.set_title(k, fontweight='bold')
    plt.show()


#########
# Main

## Read DB Configs
config = yaml.safe_load(open("config.yaml"))

# Set db variables
# postgresStr = 'postgresql://dap:dap@192.168.56.30:5432/postgres'    
postgresStr = db.getPostgresString(config)
print(postgresStr)

mongoHost = config['mongoDB']['hostname']
mongoPort = config['mongoDB']['port']


## 1 - Get financial info from yahoo
msi =  get_s_and_p_equities('1d')

## 2 - add raw ticker data  MongoDB collection
smi_collection = get_mongo_db_smi_collection(mongoHost, mongoPort)
rowsadded = initialize_collection_with_ticker_data(smi_collection, msi)
print ('added ' + str(rowsadded) + ' to collection')

## 3 -  categorize share indexes using lambda functions
category_idx = { 'S&P 500 Share Indices Close Change %' :['^SP500-255040','^SP500-60','^SP500-35','^SP500-30']}
msi['region']= msi.ticker.apply(lambda x: getRegion(x, category_idx))
msi.head()

## 4 - set up close price data 
startdate = msi["Date"].iloc[0]
print(startdate)
beginHere  = msi.loc[msi.Date == startdate]
print(beginHere)
msi['ccBegin'] = msi.apply(lambda x: calcClosePC(x.ticker, x.Close, beginHere),axis=1)
changeDf = transform_ticker_data(msi)
changeDf.head()

## 5 - insert close price change data into Postgres database for comparison analysis
replace_postgres_with_price_chg_history(changeDf,postgresStr)

## 6 - PLot the stock indices close price change 
si_dict = {'^SP500-255040': 'S&P 500 Specialty Retail (Industry)',
           '^SP500-60': 'S&P 500 Real Estate (Sector)', 
           '^SP500-35': 'S&P 500 Health Care (Sector)',
          '^SP500-30': 'S&P 500 Consumer Staples (Sector)'}

#plot_close_change(category_idx,changeDf, si_dict )


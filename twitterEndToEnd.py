import pymongo
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import requests, io, json, os, re
from nltk.sentiment import SentimentIntensityAnalyzer
import matplotlib.pyplot as plt
import yaml
import dbfuncs as dbf


# Function to open text file and read Twitter handles
def readTwitterUserFile(filename):
    twitterUser = []
    try:
        with open(filename,'r') as fh : lines = fh.readlines()
    except FileNotFoundError:
         print('The file {} was not found.'.format(filename))
    except IsADirectoryError :
         print('{} is a directory.'.format(filename))
    except PermissionError:
         print('Insufficient permissions to open {}.'.format(filename))
    else:
         for line in lines:
            if line[0:1] == '@':
                twitterUser.append(line[1:].strip())
            else:
                twitterUser.append(line.strip())         
    return twitterUser

def generateAPIAccountQuery(twUsers):
    user_search_url = "https://api.twitter.com/2/users/by?usernames=TheEconomist"
    for user in twUsers:
        user_search_url = user_search_url + "," + user
    return user_search_url

# Function to generate request headers authorised to access Twitter's API
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer AAAAAAAAAAAAAAAAAAAAAOH0jAEAAAAAAT1mQvzriiowqhlxxcD%2BiBT2QWI%3D5c45q78E693ygLYA2Eta0kDkpSZyzzDrwTxZ8WDf9hOsUVf84Z"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r

# Function to send GET requests to the Twitter API
def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print('Response status code:', response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# Function to pass Twitter usernames and return user Ids 
def getUserIds(url, params):
    userListData = []
    json_response = connect_to_endpoint(url, params)
    userList = json_response['data']
    for user in userList:
        userListData.append([user['name'], user['username'], user['id']])        
    return userListData


def getTweetHistory(userListIds):
    tweetDataCompiled = []
    for user in userListIds:
        print()
        print("name: {} username: {} id: {}".format(user[0], user[1], user[2]))
        print()
        url = "https://api.twitter.com/2/users/{}/tweets?tweet.fields=created_at&start_time=2020-01-01T00:00:00.00Z&expansions=author_id&user.fields=created_at&max_results=100".format(user[2])
        print('url:', url)
        try:
            tweetListData, isNextToken = getTweets(url, {})
            for tweetData in tweetListData:
                tweetDataCompiled.append([user[0], user[1], user[2], tweetData[0], tweetData[1]])
        except:
            print("No tweets returned")

        while (len(isNextToken) > 0):
            #url = "https://api.twitter.com/2/users/{}/tweets?start_time=2019-01-01T00:00:00Z&tweet.fields=created_at&expansions=author_id&user.fields=created_at&max_results=100&pagination_token={}".format(user[2], isNextToken)
            url = "https://api.twitter.com/2/users/{}/tweets?tweet.fields=created_at&start_time=2020-01-01T00:00:00.00Z&expansions=author_id&user.fields=created_at&max_results=100&pagination_token={}".format(user[2], isNextToken)
            print('url:', url)
            try:
                tweetListData, isNextToken = getTweets(url, {})
                for tweetData in tweetListData:
                    tweetDataCompiled.append([user[0], user[1], user[2], tweetData[0], tweetData[1]])
            except:
                print("No tweets returned")

    dfTweets = pd.DataFrame(tweetDataCompiled, columns=['name', 'username', 'id', 'Time', 'Tweet Text'])
    return dfTweets


def getTweets(url, params):
    tweetListData = []
    json_response = connect_to_endpoint(url, params)
    #print('json_response:', json_response)
    #tweetList = json_response['data']
    try:
        listNextToken = json_response['meta']['next_token']
    except:
        listNextToken = ""
    print('listNextToken:', listNextToken)
    
    for tweet in json_response['data']:
        #print(user)
        tweetListData.append([tweet['created_at'], tweet['text']])        
    return tweetListData, listNextToken


def emptyTweetsCollection(host, port):
    # Connect to MongoDB
    client = pymongo.MongoClient(host,port)
 
    # Database Name
    db = client['myTwitterDB']
 
    # Collection Name
    collection = db['tweetsCollection']
 
    collection.delete_many({})
    print('All records deleted from MongoDB tweetsCollection')
    

def loadTweetsToMongoDB(inputTweetsDF, host, port):
    # Connect to MongoDB
    client = pymongo.MongoClient(host,port)
    
    # Database Name
    db = client['myTwitterDB']
    
    # Collection Name
    collection = db['tweetsCollection']
    
    # Format and insert collection
    inputTweetsDF.reset_index(inplace=True)
    data_dict = inputTweetsDF.to_dict("records")
    collection.insert_many(data_dict)
    print('Tweets loaded to MongoDB tweetsCollection')
    

def importTweetsFromMongoDB(host,port):
    # Connect to MongoDB
    client = pymongo.MongoClient(host,port)
    
    # Database Name
    db = client['myTwitterDB']
    
    # Collection Name
    collection = db['tweetsCollection']
    
    tweetsData = collection.find()
    tweetsDataDF = pd.DataFrame(tweetsData)
    tweetsDataDF.columns = ['ObjectId', 'TweetIndex', 'AccountName', 'AccountUserName', 'AccountID', 'TweetPostTime', 'TweetText']
    #Print all column headers - short method:
    print("Tweets data imported from MongoDB to a Python Dataframe.")
    return tweetsDataDF


def regexRemoveNewLineTab(inputText):
    pattern = re.compile(r'(\n+)')
    updatedText = pattern.sub(r'___', inputText)
    pattern2 = re.compile(r'(\r+)')
    updatedText2 = pattern2.sub(r'___', updatedText)
    return updatedText2


def cleanTweetText(df):
    df['TweetText'] = df['TweetText'].apply(regexRemoveNewLineTab)
    print('New Line characters have been removed from TweetText column of the dataframe')
    
    
def analyseSentiment(textToAnalyse):
    sia = SentimentIntensityAnalyzer()
    sentimentScore = sia.polarity_scores(textToAnalyse)
    return sentimentScore['compound']
    

def loadTweetsDataToPostgreSQL(df, engineStr):
    print('Loading Tweets to PostgreSQL')
    engine = create_engine(engineStr)
    #engine = create_engine('postgresql+psycopg2://postgres:@localhost/postgres')

    #df.head(0).to_sql('tweetdata', engine, index=False) 
    df.head(0).to_sql('tweetdata', engine, if_exists='replace',index=False) #drops old table and creates new empty table

    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'tweetdata', null="") # null values become ''
    conn.commit()

def createPostgreSentimentTable(config):
    print('Running createPostgreSentimentTable')
    sqlAddTweetDateColumn = """
        ALTER TABLE public.tweetdata
        ADD COLUMN TweetDate date;
        """
    sqlPopulateTweetDateColumn = """
        update public.tweetdata
        set TweetDate = TO_DATE(left("TweetPostTime", 10),'YYYY-MM-DD');
        """
    sqlCreateTweetSentimentTable = """
        DROP TABLE IF EXISTS TweetSentiment;
        CREATE TABLE TweetSentiment (
        TweetDate date Primary Key,
        AvgSentimentScore NUMERIC(9, 8) NULL,
        SentimentScoreTotal NUMERIC(9, 6) NULL);
        """
    sqlInsertToTweetSentimentTable = """
        insert into public.tweetsentiment(TweetDate, AvgSentimentScore)
        select tweetdate, avg("SentimentScore")
        from  public.tweetdata
        where "SentimentScore" <> 0
        group by tweetdate
        order by tweetdate desc;
        """
    sqlCalcSentimentScoreTotal = """
        update public.tweetsentiment twsentiment
        set SentimentScoreTotal = runningTotal.runningSentimentScoreTotal
            from (select 
            TweetDate, 
            AvgSentimentScore,
            SUM(AvgSentimentScore) OVER (ORDER BY TweetDate) AS runningSentimentScoreTotal
            FROM public.tweetsentiment) runningTotal
        where twsentiment.TweetDate = runningTotal.TweetDate
        """
        
    #conn = psycopg2.connect("host=localhost dbname=postgres user=postgres")
    # conn = psycopg2.connect(user = config['postgreSQL']['username'],
    #     password = config['postgreSQL']['password'],
    #     host = config['postgreSQL']['hostname'],
    #     port = config['postgreSQL']['port'],
    #     database = config['postgreSQL']['database'])

    conn = dbf.getDBConnection(config, config['postgreSQL']['database'])      
    cur = conn.cursor()
    
    try:
        cur.execute(sqlAddTweetDateColumn)
    finally:
        conn.commit()
        print('sqlAddTweetDateColumn is running.')
    try:
        cur.execute(sqlPopulateTweetDateColumn)
    finally:
        conn.commit()
        print('sqlPopulateTweetDateColumn is running.')
    try:
        cur.execute(sqlCreateTweetSentimentTable)
    finally:
        conn.commit()
        print('sqlCreateTweetSentimentTable is running.')
    try:
        cur.execute(sqlInsertToTweetSentimentTable)
    finally:
        conn.commit()
        print('sqlInsertToTweetSentimentTable is running.')
    try:
        cur.execute(sqlCalcSentimentScoreTotal)
    finally:
        conn.commit()
        print('sqlCalcSentimentScoreTotal is running.')
    cur.close()  
    

def importFromPostgreSQL(engineStr):
    # Create an engine instance
    #engine = create_engine('postgresql+psycopg2://postgres:@localhost/postgres')
    engine = create_engine(engineStr)
    # Connect to PostgreSQL server
    dbConnection = engine.connect()

    # Read data from PostgreSQL database table and load into a DataFrame instance
    dataFrameTW = pd.read_sql("select * from public.tweetsentiment", dbConnection)
    
    pd.set_option('display.expand_frame_repr', False)

    # Close the database connection
    dbConnection.close()
    
    return dataFrameTW


def generateSentimentChart(dfTwitterSentiment):
    plt.plot(dfTwitterSentiment["tweetdate"], dfTwitterSentiment["sentimentscoretotal"])
    plt.xlabel("Date")
    plt.ylabel("Accumulated Sentiment Total")
    plt.show()
    
##
    
############################################################################################################################################

# Main

# Bearer Token provides access the Twitter API
# Set as environment variable on local machine

## Read Config
config = yaml.safe_load(open("config.yaml"))

# postgresStr = 'postgresql://dap:dap@192.168.56.30:5432/postgres'    
#postgresStr = 'postgresql://' + config['postgreSQL']['username'] + ':' + config['postgreSQL']['password'] + '@' + config['postgreSQL']['hostname']  + '/' + config['postgreSQL']['database']
postgresStr = dbf.getPostgresString(config)
print(postgresStr)
        
#bearer_token = os.environ.get("BEARER_TOKEN")
    
# STEP 1: Get list of Twitter account handles from CSV file
twitterUsers = readTwitterUserFile('TwitterHandleList2.csv')

# STEP 2: Generate https request string for Twitter accounts
twitterUsersQuery = generateAPIAccountQuery(twitterUsers)

# STEP 3: Send request to Twitter API to return account Ids. 
userListIdentifiers = getUserIds(twitterUsersQuery, {})

# STEP 4: Send request to Twitter API to return Tweets sent by accounts. 
dfTweets = getTweetHistory(userListIdentifiers)

# STEP 5: Delete Tweets Collection from MongoDB
mongoHost = config['mongoDB']['hostname']
mongoPort = config['mongoDB']['port']
emptyTweetsCollection(mongoHost, mongoPort)

# STEP 6: Load Tweets to MongoDB
loadTweetsToMongoDB(dfTweets, mongoHost, mongoPort)

# STEP 7: Import Tweets from MongoDB
tweetsDataFrame = importTweetsFromMongoDB(mongoHost, mongoPort )

# STEP 8: Clean Tweet Text to remove New Line characters
cleanTweetText(tweetsDataFrame)

# STEP 9: Run Sentiment Analysis on all tweets and add 'SentimentScore' column to the DataFrame
## nltk
import nltk
print('downloading lexicon')
nltk.download('vader_lexicon')
print('Calculating the SentimentScore for all tweets')
tweetsDataFrame['SentimentScore'] = tweetsDataFrame['TweetText'].apply(analyseSentiment)

# STEP 10: Load Data to PostgreSQL Database
loadTweetsDataToPostgreSQL(tweetsDataFrame, postgresStr)

# STEP 11: Generate SentimentTable and calculate Average Daily Sentiment
createPostgreSentimentTable(config)

# STEP 12: Imported Average Sentiment from PostgreSQL Database to a Python Dataframe
dfTwitterSentiment = importFromPostgreSQL(postgresStr)

# STEP 13: Generate Sentiment Chart
generateSentimentChart(dfTwitterSentiment)

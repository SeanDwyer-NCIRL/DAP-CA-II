import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer
#nltk.download()

#fileAddress = r'C:\Users\donal\Documents\PostGrad Diploma in Data Analytics\Database & Analytics\Team Project\ProjectRepo\TweetsDataOutput.csv'
fileAddress = r'C:\Users\donal\Documents\PostGrad Diploma in Data Analytics\Database & Analytics\Team Project\TweetsDataOutputShort.csv'

#dfTweets = pd.read_csv(fileAddress, low_memory=False, parse_dates=['Time'], dayfirst=True)
dfTweets = pd.read_csv(fileAddress, low_memory=False)

dfTweets['Tweet Text String'] = dfTweets['Tweet Text'].apply(lambda x:x[2:-1].encode().decode("unicode_escape").encode('raw_unicode_escape').decode())

#stopwords = nltk.corpus.stopwords.words("english")

#print('stopwords:',stopwords)
sia = SentimentIntensityAnalyzer()

def analyseSentiment(textToAnalyse):
    sentimentScore = sia.polarity_scores(textToAnalyse)
    return sentimentScore['compound']
    
dfTweets['Sentiment Score'] = dfTweets['Tweet Text'].apply(analyseSentiment)

#export DataFrame to CSV
#exportFile = r'C:\Users\donal\Documents\PostGrad Diploma in Data Analytics\Database & Analytics\Team Project\TweetsDFOut2.csv'
#dfTweets.to_csv(exportFile)

dfTweets
from datetime import datetime
#cosp_df
#dfTwitterSentiment
#changeDf
cosp_df['date'] = pd.to_datetime(cosp_df.apply(lambda x: datetime.strptime('{0} {1} 1'.format(int(x['year']), int(x['weekly'])), '%Y %W %w'), axis=1))
dfTwitterSentiment['tweetdate'] = pd.to_datetime(dfTwitterSentiment['tweetdate'])
combinedDf = pd.merge(dfTwitterSentiment, cosp_df, left_on='tweetdate', right_on='date', how = 'inner')
changeDf.reset_index(inplace=True)
combinedDf2 = pd.merge(combinedDf, changeDf, left_on='tweetdate', right_on='Date', how = 'inner')
#combinedDf2.head()

#Run Pearson correlation on all columns of the DataFrame.
correlationsDf = combinedDf2.corr()
correlationsDf

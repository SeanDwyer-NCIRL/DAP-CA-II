from datetime import datetime, timedelta

#Add date column to cosp_df DataFrame
cosp_df['date'] = pd.to_datetime(cosp_df.apply(lambda x: datetime.strptime('{0} {1} 1'.format(int(x['year']), int(x['weekly'])), '%Y %W %w'), axis=1))
#Convert tweetdate column to datetime type 
dfTwitterSentiment['tweetdate'] = pd.to_datetime(dfTwitterSentiment['tweetdate'])

#Perform inner join merge on the 2 DataFrames and store results to combinedDf
combinedDf = pd.merge(dfTwitterSentiment, cosp_df, left_on='tweetdate', right_on='date', how = 'inner')

#Run Pearson correlation on all columns of the DataFrame.
correlationsDf = combinedDf.corr()
correlationsDf
ECHO Running importTweets
PAUSE
CD "C:\Program Files\MongoDB\Server\6.0\bin\"

ECHO changed directory
PAUSE

mongoimport -d twitterDB -c tweets --type csv --file "C:\Users\donal\Documents\PostGrad Diploma in Data Analytics\Database & Analytics\Team Project\ProjectRepo\TweetsData.csv" --headerline
ECHO File imported to MongoDB
PAUSE

mongoexport --db twitterDB --collection tweets --type=csv --fields Time,name,username,id,"Tweet Text" --out "C:\Users\donal\Documents\PostGrad Diploma in Data Analytics\Database & Analytics\Team Project\ProjectRepo\TweetsDataOutput.csv"
ECHO CSV file expoted from MongoDB
PAUSE

CD "C:\Users\donal\Documents\PostGrad Diploma in Data Analytics\Database & Analytics\Team Project\ProjectRepo\"
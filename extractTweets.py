import requests
import os
import json
import pandas as pd
        

# Bearer Token provides access the Twitter API
# Set as environment variable on local machine
bearer_token = os.environ.get("BEARER_TOKEN")


# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
query_params = {}


# Function to generate request headers
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r


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
            twitterUser.append(line[1:].strip())
    return twitterUser



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
        #print(user)
        userListData.append([user['name'], user['username'], user['id']])        
    return userListData


# Function to request tweets by account Id. Each request is limited to 100 tweets. 
# If there are additional tweets available a 'next_token' value will be included
# in the meta section of the JSON response. 
def getTweets(url, params):
    tweetListData = []
    json_response = connect_to_endpoint(url, params)
    tweetList = json_response['data']
    try:
        listNextToken = json_response['meta']['next_token']
    except:
        listNextToken = ""
    
    for tweet in tweetList:
        #print(user)
        tweetListData.append([tweet['created_at'], tweet['text']])        
    return tweetListData, listNextToken
        

#STEP 1: Get list of Twitter accounts from CSV file
twitterUsers = readTwitterUserFile('TwitterHandleList.csv')


#STEP 2: Generate url containing list of Twitter usernames. 
user_search_url = "https://api.twitter.com/2/users/by?usernames=TheEconomist"
for user in twitterUsers:
    user_search_url = user_search_url + "," + user
#print(user_search_url)


#STEP 3: Send request to Twitter API in order to return account Ids. 
userListIdentifiers = getUserIds(user_search_url, query_params)


#STEP 4: Send request to Twitter API passing user Ids and requesting all tweets from that account.
#        The API limits each request to 100 tweets so a pagination_token is captured from each 
#        response and passed into the next looped request.
tweetDataCompiled = []
for user in userListIdentifiers:
    print()
    print("name: {} username: {} id: {}".format(user[0], user[1], user[2]))
    print()
    url = "https://api.twitter.com/2/users/{}/tweets?tweet.fields=created_at".format(user[2])
    print('url:', url)
    tweetListData, isNextToken = getTweets(url, query_params)
    #print('isNextToken:', isNextToken)
    for tweetData in tweetListData:
        tweetDataCompiled.append([user[0], user[1], user[2], tweetData[0], tweetData[1].encode('utf-8')])
        
    while (len(isNextToken) > 0):
        url = "https://api.twitter.com/2/users/{}/tweets?tweet.fields=created_at&expansions=author_id&user.fields=created_at&max_results=100&pagination_token={}".format(user[2], isNextToken)
        print('url:', url)
        tweetListData, isNextToken = getTweets(url, query_params)
        for tweetData in tweetListData:
            tweetDataCompiled.append([user[0], user[1], user[2], tweetData[0], tweetData[1].encode('utf-8')])


#STEP 5: Create a Dataframe with the results and save to CSV file. 
dfTweets = pd.DataFrame(tweetDataCompiled, columns=['name', 'username', 'id', 'Time', 'Tweet Text'])
dfTweets.to_csv('TweetsData.csv')
print('Tweets saved to CSV. Process Complete!')
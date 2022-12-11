import matplotlib.pyplot as plt 

def plotTwitterSent():
    plt.rcParams["figure.figsize"] = [10, 5]
    plt.rcParams["figure.autolayout"] = True
    plt.plot(twitterSentDF['tweetdate'], twitterSentDF['sentimentscoretotal'], color='blue', label='Twitter Sentiment Score')
    plt.xticks(rotation=45)
    plt.grid(linewidth=0.25)
    plt.title("Accumulated Twitter Sentiment score over Time")
    plt.xlabel('Date')
    plt.ylabel('Accumulated Twitter Sentiment')
    plt.legend()
    plt.show()

def plotCombined():
    date = combinedFieldsDf['Date']
    sentimentScoreTotal = combinedFieldsDf['sentimentscoretotal']
    consumerSpending = combinedFieldsDf['total']
    stockMarket = combinedFieldsDf['^SP500-35']
    
    fig, ax = plt.subplots()
    ax.plot(date, sentimentScoreTotal, color='blue', label='Twitter Sentiment Total')
    ax.tick_params(axis='y', labelcolor='blue')
    
    # Generate a new Axes instance, on the twin-X axes 
    ax2 = ax.twinx()
    ax2.plot(date, consumerSpending, color='green', label='Consumer Spending Change')
    ax2.plot(date, stockMarket, color='red', label='Stock Market Change')
    ax2.tick_params(axis='y', labelcolor='green')
        
    # Set labels
    ax.set_xlabel('Time')
    ax.set_ylabel('Twitter Sentiment Score')
    ax2.set_ylabel('Consumer Spending & Stock Market % Movement')
    plt.title("Twitter Sentiment Score & Total Consumer Spending & Stock Sector 35")
    leg = ax.legend(loc=4)
    leg = ax2.legend(loc=2)
    plt.show()

#Main
plotTwitterSent()
plotCombined()
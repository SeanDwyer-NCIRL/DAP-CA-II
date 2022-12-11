import matplotlib.pyplot as plt

## Plot functions
def plot_close_change(ccBegin):
    cat_idx = { 'S&P 500 Share Indices Close Change %' :['^SP500-255040','^SP500-60','^SP500-35','^SP500-30']}
    si_dict = {'^SP500-255040': 'S&P 500 Specialty Retail (Industry)',
           '^SP500-60': 'S&P 500 Real Estate (Sector)', 
           '^SP500-35': 'S&P 500 Health Care (Sector)',
          '^SP500-30': 'S&P 500 Consumer Staples (Sector)'}
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


def plot_consumer_spending(cosp_df): 
    try:
        plt.plot(cosp_df['total'], label = 'All')
        plt.plot(cosp_df['acf'], label = 'Accom. & Food Service')
        plt.plot(cosp_df['aer'], label = 'Arts, Enter., Rec.')
        plt.plot(cosp_df['apg'], label = 'Gen. Merch. & Apparel')
        plt.plot(cosp_df['grf'], label = 'Groc. & Food Store')
        plt.plot(cosp_df['hcs'], label = 'Health & Social Ass.')
        plt.plot(cosp_df['tws'], label = 'Transport & Wareh.')
        plt.plot(cosp_df['retail'], label = 'Retail Spending')
        plt.plot(cosp_df['retail_nogroc'], label = 'Retail Excl. Groc.')
        plt.ylabel('% Chnage', fontsize=14)
        plt.xlabel('Week', fontsize=14)
        plt.title('Consumer Spending per Sector', fontsize=16)
        plt.legend()
    except (Exception) as Error :
        print ("Error:", Error)
    finally:
        plt.show()

def generateSentimentChart(dfTwitterSentiment):
    plt.plot(dfTwitterSentiment["tweetdate"], dfTwitterSentiment["sentimentscoretotal"])
    plt.xlabel("Date")
    plt.ylabel("Accumulated Sentiment Total")
    plt.show()

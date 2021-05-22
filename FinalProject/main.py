# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
import json
import mysql.connector
import pandas as pd
import csv
from faker import Faker as Faker
from sqlalchemy import create_engine
from pandas import DataFrame
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
import string
from collections import Counter
import time

#references
#https://fairyonice.github.io/extract-someones-tweet-using-tweepy.html
#Cleaning text cite: https://codereview.stackexchange.com/questions/249329/finding-the-most-frequent-words-in-pandas-dataframe

# Create the class that will handle the tweet stream

# Enter Twitter API Keys
fake = Faker()
access_token = "1179438016382656512-ftkE6yiVtZ6v5ubPSvNyFNMf3leG3s"
access_token_secret = "GFwhsCS8vIVcl8p0EEOI6J1lhlzfo26h4aUmB7qzUYxdq"
consumer_key = "LHLvtqoboEHpqGXAgZVuQQeWH"
consumer_secret = "gr2FLJKYv5HuQfVM0tYtoenqkH2tGH64s960lCFGFAZWG9uIoA"

conn = mysql.connector.connect(
    host="localhost",
    user="crewe",
    password="dbManagement123!",
    auth_plugin='mysql_native_password',
    database='mydb'
)
host = "localhost"
user = "crewe"
password = "dbManagement123!"
database = 'mydb'
port = "3306"

engine = create_engine("mysql+mysqlconnector://" + user + ":" + password + "@" + host + ":" + port + "/" + database, echo=False)
dbConnection = engine.connect()
my_cursor = conn.cursor()
stop_words = stopwords.words()


#Cleaning the text of the tweets to allow for data analysis without errors
def cleaning(text):
    # converting to lowercase, removing URL links, special characters, punctuations...
    text = text.lower()
    text = re.sub('https?://\S+|www\.\S+', '', text)
    text = re.sub('<.*?>+', '', text)
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub('\n', '', text)
    text = re.sub('[’“”…]', '', text)

    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    text = emoji_pattern.sub(r'', text)

    # removing the stop-words
    text_tokens = word_tokenize(text)
    tokens_without_sw = [word for word in text_tokens if not word in stop_words]
    filtered_sentence = (" ").join(tokens_without_sw)
    text = filtered_sentence

    return text


#Creates tables in database
def CreateTwitterTable():
    my_cursor.execute('''
    CREATE TABLE tweets(
        TweetId VARCHAR(64) PRIMARY KEY,
        Username VARCHAR(64),
        INDEX company_ibfk_1 (Username ASC)
    );
''')
    my_cursor.execute('''
    CREATE TABLE company(
        Username VARCHAR(64),
        NetWorth INTEGER,
        NumEmployees INTEGER,
        FOREIGN KEY (Username) REFERENCES tweets(Username),
        UNIQUE INDEX company_ibfk_1 (Username ASC)
    );
''')
    my_cursor.execute('''
    CREATE TABLE text(
        TweetId VARCHAR(64) PRIMARY KEY,
        Text VARCHAR(1000),
        Date VARCHAR(64),
        FOREIGN KEY (TweetId) REFERENCES tweets(TweetId)
    );
''')

    my_cursor.execute('''
    CREATE TABLE sentiment(
        Username VARCHAR(64),
        Word VARCHAR(64),
        Frequency Integer,
        FOREIGN KEY (Username) REFERENCES tweets(Username)
    );
''')

    my_cursor.execute('''
    CREATE TABLE popularity(
        TweetId VARCHAR(64) PRIMARY KEY,
        Likes INTEGER,
        Retweets INTEGER,
        FOREIGN KEY (TweetId) REFERENCES tweets(TweetId)
    );
''')


#Deletes all tables
def DeleteTables():
    my_cursor.execute("drop table company")
    my_cursor.execute("drop table text")
    my_cursor.execute("drop table popularity")
    my_cursor.execute("drop table sentiment")
    my_cursor.execute("drop table tweets")


#This is the main part of the project
#User is able to enter a twitter handle, the function gathers the data, parses the json and adds it to the correct table
def newData(userID):
    # Authorize our Twitter credentials
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name=userID,
                               # 200 is the maximum allowed count
                               count=200,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

    all_tweets = []
    all_tweets.extend(tweets)
    oldest_id = tweets[-1].id
    while True:
        tweets = api.user_timeline(screen_name=userID,
                                   # 200 is the maximum allowed count
                                   count=200,
                                   include_rts=False,
                                   max_id=oldest_id - 1,
                                   # Necessary to keep full_text
                                   # otherwise only the first 140 words are extracted
                                   tweet_mode='extended'
                                   )
        if len(tweets) == 0:
            break
        oldest_id = tweets[-1].id
        all_tweets.extend(tweets)

    outTweets = [[tweet.id_str,
                  userID]
                 for idx, tweet in enumerate(all_tweets)]

    outCompany = [[userID,
                  tweet.id_str,
                  tweet.created_at,
                  tweet.favorite_count,
                  tweet.retweet_count,
                  tweet.full_text.encode("utf-8").decode("utf-8")]
                 for idx, tweet in enumerate(all_tweets)]

    outText = [[tweet.id_str,
                tweet.full_text.encode("utf-8").decode("utf-8"),
                tweet.created_at]
                 for idx, tweet in enumerate(all_tweets)]

    outSentiment = [[userID,
                 tweet.full_text.encode("utf-8").decode("utf-8")]
                 for idx, tweet in enumerate(all_tweets)]

    outPopularity = [[tweet.id_str,
                  tweet.favorite_count,
                  tweet.retweet_count]
                 for idx, tweet in enumerate(all_tweets)]

    df_tweets = DataFrame(outTweets, columns=["TweetId", "Username"])
    df_company = DataFrame(outCompany, columns=["userId", "tweetId", "created_at", "favorite_count", "retweet_count", "text"])
    df_text = DataFrame(outText, columns=["TweetId", "Text", "Date"])

    #Cleaning the text and applying NLP to find most used words, putting at in a DF then inserting into database
    df_sentiment = DataFrame(outSentiment, columns=["userId", "text"])
    dt = df_sentiment['text'].apply(cleaning)
    p = Counter(" ".join(dt).split()).most_common(10)
    rslt = pd.DataFrame(p, columns=['Word', 'Frequency'])
    data = {'Username': [userID, userID, userID, userID, userID, userID, userID, userID, userID, userID]}
    newDf = pd.DataFrame(data)
    newDf = newDf.join(rslt)

    df_popularity = DataFrame(outPopularity, columns=["TweetId", "Likes", "Retweets"])
    df_tweets.to_csv('%s_tweets.csv' % userID, index=False)
    df_company.to_csv('%s_company.csv' % userID, index=False)
    df_text.to_csv('%s_text.csv' % userID, index=False)
    newDf.to_csv('%s_sentiment.csv' % userID, index=False)
    df_popularity.to_csv('%s_popularity.csv' % userID, index=False)

    df_tweets.to_sql('tweets', engine, if_exists='append', index=False)
    df_text.to_sql('text', engine, if_exists='append', index=False)
    df_popularity.to_sql('popularity', engine, if_exists='append', index=False)
    newDf.to_sql('sentiment', engine, if_exists='append', index=False)
    conn.commit()


#User is able to create a fake tweet
def AddTweet(TweetId):
    new_tweet = []
    new_text = []
    new_popularity = []
    Username = input("Username: ")
    Text = input("Enter Text for the Tweet: ")
    Date = input("Date: ")
    Likes = input("Amount of Likes : ")
    Retweets = input("Amount of Retweets : ")
    new_tweet.append((TweetId, Username))
    new_text.append((TweetId, Text, Date))
    new_popularity.append((TweetId, Likes, Retweets))
    tweetEx = 'INSERT INTO tweets(TweetId, Username) VALUES("' + TweetId + '","' + Username + '");'
    my_cursor.execute(tweetEx)
    textEx = 'INSERT INTO text(TweetId, Text, Date) VALUES("' + TweetId + '","' + Text + '","' + Date + '");'
    my_cursor.execute(textEx)
    popEx = 'INSERT INTO popularity(TweetId, Likes, Retweets) VALUES("' + TweetId + '","' + Likes + '","' + Retweets + '");'
    my_cursor.execute(popEx)
    print("Tweet added to tables")
    conn.commit()

    return TweetId


#Function to be able to update a tweet based off the user inputed, TweetId
def Update(TweetId):
    print("To uphold referential integrity, the only values available to be updated are in the Text and Popularity Tables")
    print("\nPlease enter which table you would like to update")
    print("\n1: The Text Table (Tweet Text and Date)")
    print("2: The Popularity table (Number of likes and retweets")
    x = int(input(": "))
    if(x == 1):
        new_text = []
        print("Enter fields: ")
        Text = input("Enter Text for the Tweet: ")
        Date = input("Date: ")
        new_text.append((TweetId, Text, Date))
        textUpdate = "UPDATE text SET Text = '" + Text + "'  WHERE TweetId = '" + TweetId + "'"
        dateUpdate = "UPDATE text SET Date = '" + Date + "' WHERE TweetId = '" + TweetId + "'"
        my_cursor.execute(textUpdate)
        my_cursor.execute(dateUpdate)
        conn.commit()
        print("Tweet updated in the Text table")
    elif(x == 2):
        new_popularity = []
        print("Enter fields: ")
        Likes = input("Amount of Likes : ")
        Retweets = input("Amount of Retweets : ")
        new_popularity.append((TweetId, Likes, Retweets))
        likeUpdate = "UPDATE popularity SET Likes = '" + Likes + "'  WHERE TweetId = '" + TweetId + "'"
        rtUpdate = "UPDATE popularity SET Retweets = '" + Retweets + "' WHERE TweetId = '" + TweetId + "'"
        my_cursor.execute(likeUpdate)
        my_cursor.execute(rtUpdate)
        conn.commit()
        print("Tweet updated in the popularity table")
    else:
        print("Not a valid entry, returning to menu")


#Function to be able to delete a tweet based off the user inputed, TweetId
def Delete(TweetId):

    deletionEx3 = "DELETE FROM text WHERE TweetId = '%s'" % (TweetId)
    my_cursor.execute(deletionEx3)
    deletionEx4 = "DELETE FROM popularity WHERE TweetId = '%s'" % (TweetId)
    my_cursor.execute(deletionEx4)
    deletionEx = "DELETE FROM tweets WHERE TweetId = '%s'" % (TweetId)
    my_cursor.execute(deletionEx)
    conn.commit()

    print("TweetId: " + str(id) + " has been deleted from all tables")


#Function for querying the tweets table and printing it to the terminal
def DisplayTweets():
    my_cursor.execute('SELECT * FROM tweets')
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['TweetId', 'Username'])
    print(df.to_string())
    del df


#Function for querying the company table and printing it to the terminal
def DisplayCompany():
    my_cursor.execute('SELECT * FROM company')
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['Username', 'NetWorth', 'NumEmployees'])
    print(df.to_string())
    del df


#Function for querying the text table and printing it to the terminal
def DisplayText():
    my_cursor.execute('SELECT * FROM text')
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['TweetId', 'Text', 'Date'])
    print(df.to_string())
    del df


#Function for querying the sentiment table and printing it to the terminal
def DisplaySentiment():
    my_cursor.execute('SELECT * FROM sentiment')
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['Username', 'Word', 'Frequency'])
    print(df.to_string())
    del df


#Function for querying the popularity table and printing it to the terminal
def DisplayPopularity():
    my_cursor.execute('SELECT * FROM popularity')
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['TweetId', 'Likes', 'Retweets'])
    print(df.to_string())
    del df


#Function for the command line application display
def DisplayPrompt(val):
    if(val == 0):
        print()
        print("\nPlease Select One of the following options by entering the corresponding number:\n")
        print("\n1: Search Twitter for data")
        print("2: Create Tweet")
        print("3: Update Tweet")
        print("4: Delete Tweet")
        print("5: Display a Data Table")
        print("6: Perform Queries on Data")
        print("7: Exit\n")
    elif(val == 5):
        print("Please select which table you would like to display the data from:\n")
        print("\n1: Tweets Table")
        print("2: Company Table")
        print("3: Text Table")
        print("4: Sentiment Table")
        print("5: Popularity Table")
    elif(val == 6):
        print("\nPlease select which of the following queries you would like to perform:")
        print("\n1: Displaying all of the data gathered")
        print("2: Sorting tweets by the amount of likes")
        print("3: Display each user with their total likes on all of their tweets")


#Function for the query of displaying all of the data
def DisplayAll():
    query = '''
    SELECT * 
    FROM tweets
    Natural Join text
    Natural Join popularity;
        '''
    my_cursor.execute(query)
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['TweetId', 'Username', 'Text', 'Date', 'Likes', 'Retweets'])
    print(df.to_string())
    del df


#Function for the query to sort by likes
def sortByLikes():
    query = '''
    SELECT TweetId, Username, Likes, Date 
    FROM tweets
    Natural Join text
    Natural Join popularity
    Order By Likes Desc;
        '''
    my_cursor.execute(query)
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['TweetId', 'Username', 'Likes', 'Date'])
    print(df.to_string())
    del df


#Function for the query to sort by likes
def sumLikes():
    query = '''
    Select Username, SUM(Likes) as total
    From(
        SELECT TweetId, Username, Likes, Date 
        FROM tweets
        Natural Join text
        Natural Join popularity
    ) as sub
    Group by Username
    Order By total Desc;
        '''
    my_cursor.execute(query)
    my_records = my_cursor.fetchall()
    df = pd.DataFrame(my_records, columns=['Username', 'TotalLikes'])
    print(df.to_string())
    del df


def main():
    #Creating Tables
    #This try and except will delete the tables from a previous run through the program
    #This is to make it possible to run if you have encountered a bug in a previous run through
    try:
        DeleteTables()
    except:
        print("\n")
    CreateTwitterTable()
    #This does not allow and queries to be ran before gathering twitter data
    isImported = False
    #Lists to show what has been done
    twitterSearched = []
    newIds = []
    #While loop for prompts
    while(True):

        print("\n\nTwitter Handles who's data is already in the database: ")
        for y in range(len(twitterSearched)):
            print(twitterSearched[y])

        if(len(newIds) > 0):
            print("\n\nId's of tweets you have added to the database")
            for z in range(len(newIds)):
                print(newIds[z])

        DisplayPrompt(0)
        x = int(input(": "))
        if x == 1:
            print("Enter the twitter handle that you would like to gather data from: ")
            handle = str(input(": "))
            newData(handle)
            twitterSearched.append(handle)
            isImported = True
        elif x == 7:
            DeleteTables()
            break;
        elif not isImported:
            print("You must go through option one to collect data before running any other commands")
            print("Please choose option 1 before continuing")
        elif x == 2:
            #Create a tweet
            TweetId = input("Enter TweetId: ")
            AddTweet(TweetId)
            newIds.append(TweetId)
        elif x == 3:
            #Update a created tweet
            print(
                "Due to the fact that the data collected is all real twitter data, you may only Update id's that you have created")
            if (len(newIds) > 0):
                print("\n\nId's of tweets you have added to the database")
                for z in range(len(newIds)):
                    print(newIds[z])
                inputId = input("Enter an Id from the list of previously entered id's above: ")
                if(inputId in newIds):
                    Update(inputId)
            else:
                print("You have created no ID's or have entered an invalid ID, going back to the main menu")
                time.sleep(5)
        elif x == 4:
            print("Due to the fact that the data collected is all real twitter data, you may only delete id's that you have created")
            if (len(newIds) > 0):
                print("\n\nId's of tweets you have added to the database")
                for z in range(len(newIds)):
                    print(newIds[z])
                inputId = input("Enter an Id from the list of previously entered id's above: ")
                if(inputId in newIds):
                    Delete(inputId)
                    newIds.remove(inputId)
            else:
                print("You have created no ID's or have entered an invalid ID, going back to the main menu")
                time.sleep(5)
        elif x == 5:
            DisplayPrompt(5)
            x5 = int(input(":"))
            if(x5 == 1):
                DisplayTweets()
                time.sleep(3)
            elif(x5 == 2):
                DisplayCompany()
                time.sleep(3)
            elif(x5 == 3):
                DisplayText()
                time.sleep(3)
            elif(x5 == 4):
                DisplaySentiment()
                time.sleep(3)
            elif(x5 == 5):
                DisplayPopularity()
                time.sleep(3)
        elif x == 6:
            DisplayPrompt(6)
            x6 = int(input(": "))
            if(x6 == 1):
                DisplayAll()
                time.sleep(3)
            elif(x6 == 2):
                sortByLikes()
                time.sleep(3)
            elif(x6 == 3):
                sumLikes()
                time.sleep(3)
            else:
                print("Invalid input, returning to menu")
        else:
            print("Not a valid choice, try again: \n")



main()
conn.close()

# 9.One query must contain a sub-query. NEED TO DO

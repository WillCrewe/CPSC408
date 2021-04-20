#William Crewe
#CPSC 408
#crewe@chapman.edu

import sqlite3
import pandas as pd
from faker import Faker as Faker
import csv

#initializing faker
fake = Faker()
conn = sqlite3.connect('./Twitter.sqlite')
my_cursor = conn.cursor()

#Creating the 5 normalized tables
def CreateTwitterTable():
    my_cursor.execute('''
    CREATE TABLE Tweets(
        TweetId INTEGER PRIMARY KEY AUTOINCREMENT,
        Username TEXT NOT NULL
    );
''')
    my_cursor.execute('''
    CREATE TABLE Company(
        CompanyName TEXT NOT NULL PRIMARY KEY,
        Username TEXT,
        NetWorth INTEGER,
        NumEmployees INTEGER,
        FOREIGN KEY (Username) REFERENCES Tweets(Username)
    );
''')
    my_cursor.execute('''
    CREATE TABLE Text(
        TweetId INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        Text TEXT,
        Date DATE,
        FOREIGN KEY (TweetId) REFERENCES Tweets(TweetId)
    );
''')

    my_cursor.execute('''
    CREATE TABLE Sentiment(
        Username TEXT NOT NULL PRIMARY KEY,
        MUW TEXT,
        FOREIGN KEY (Username) REFERENCES Tweets(Username)
    );
''')

    my_cursor.execute('''
    CREATE TABLE Popularity(
        TweetId INTEGER NOT NULL PRIMARY KEY,
        Likes INTEGER,
        Retweets INTEGER,
        FOREIGN KEY (TweetId) REFERENCES Tweets(TweetId)
    );
''')

#Creating Tweets using faker and pushing them to a csv then reading them into the table using pandas
#Done similarly for the other 4 tables, as well as functions
def CreateTweets(filename, rows):
    with open(filename, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['TweetId', 'Username'])
        for i in range(rows):
            writer.writerow(['', fake.name()])

    tweet_data = pd.read_csv(filename)
    tweet_data.to_sql('Tweets', conn, if_exists='append', index=False)


def CreateText(filename, rows):
    with open(filename, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['TweetId', 'Text', 'Date'])
        for i in range(rows):
            writer.writerow(['', fake.sentence(), fake.date()])
    tweet_data = pd.read_csv(filename)
    tweet_data.to_sql('Text', conn, if_exists='append', index=False)


def CreateCompany(filename, rows):
    with open(filename, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['CompanyName', 'Username', 'NetWorth', 'NumEmployees'])
        for i in range(rows):
            writer.writerow([fake.sentence(3), fake.name(), fake.random_int(min=10000, max=10000000), fake.random_int(min=1, max = 100000)])
    tweet_data = pd.read_csv(filename)
    tweet_data.to_sql('Company', conn, if_exists='append', index=False)


def CreateSentiment(filename, rows):
    with open(filename, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['Username', 'MUW'])
        for i in range(rows):
            writer.writerow([fake.sentence(3), fake.sentence(1)])
    tweet_data = pd.read_csv(filename)
    tweet_data.to_sql('Sentiment', conn, if_exists='append', index=False)


def CreatePopularity(filename, rows):
    with open(filename, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(['TweetId', 'Likes', 'Retweets'])
        for i in range(rows):
            writer.writerow(['', fake.random_int(min=1, max=100000), fake.random_int(min=1, max=10000)])
    tweet_data = pd.read_csv(filename)
    tweet_data.to_sql('Popularity', conn, if_exists='append', index=False)

#Deletes all tables
def DeleteTables():
    my_cursor.execute("drop table Tweets")
    my_cursor.execute("drop table Company")
    my_cursor.execute("drop table Text")
    my_cursor.execute("drop table Popularity")
    my_cursor.execute("drop table Sentiment")

#CommandLine Prompt
def DisplayPrompt():
    print("\n1: Create Data For Tweets Table")
    print("2: Create Data For Text Table")
    print("3: Create Data For Company Table")
    print("4: Create Data for Sentiment Table")
    print("5: Create Data for Popularity Table")
    print("6: Exit\n")


def main():
    CreateTwitterTable()
    while True:
        DisplayPrompt()
        x = int(input(": "))
        if x == 1:
            print("Creating Tweets Data")
            print("Enter the filename for the csv")
            filename = str(input(": "))
            print("Enter the amount of tweets")
            rows = int(input(": "))
            CreateTweets(filename, rows)

        elif x == 2:
            print("Creating Text Data")
            print("Enter the filename for csv")
            filename = str(input(": "))
            print("Enter the amount of tweets")
            rows = int(input(": "))
            CreateText(filename, rows)
        elif x == 3:
            print("Creating Company Data")
            print("Enter the filename for csv")
            filename = str(input(": "))
            print("Enter the amount of Companies")
            rows = int(input(": "))
            CreateCompany(filename, rows)
        elif x == 4:
            print("Creating Sentiment Data")
            print("Enter the filename for csv")
            filename = str(input(": "))
            print("Enter the amount of rows")
            rows = int(input(": "))
            CreateSentiment(filename, rows)
        elif x == 5:
            print("Creating Popularity Data")
            print("Enter the filename for csv")
            filename = str(input(": "))
            print("Enter the amount of rows")
            rows = int(input(": "))
            CreatePopularity(filename, rows)
        elif x == 6:
            break;
        else:
            print("Not a valid choice, try again: \n")

    print("Deleting All Tables")
    DeleteTables()
    print("Exiting")


main()
conn.close()
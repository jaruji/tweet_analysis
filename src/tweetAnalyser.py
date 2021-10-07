from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2
import demoji
import pandas as pd
import pandas.io.sql as sqlio
import re
import time
#import swifter

def connect():
    #connect to postgres PDT database
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="PDT",
            user="postgres",
            password="postgres",
            port=5433)
        print("Connected to database")
    except:
        print("Connecting to database failed.")
    finally:
        return conn;

def handleTweet(tweetId, tweet, cur, sia):
    tweet = cleanTweet(tweet)
    dict = sentiment(tweet, sia)
    insertSentiment(tweetId, dict, cur)


def cleanTweet(tweet):
    #remove all emoji
    tweet = demoji.replace(tweet, '')
    #remove all mentions and hashtags
    tweet = re.sub(r'#([a-zA-Z0-9_]+)|@([a-zA-Z0-9_]+)', '', tweet)
    return tweet;

def sentiment(tweet, sia):
    dict = sia.polarity_scores(tweet)
    return dict

def addColumns(conn, cur):
    #execute only once!
    cur.execute("ALTER TABLE tweets ADD COLUMN neu FLOAT, ADD COLUMN neg FLOAT, ADD COLUMN pos FLOAT, ADD COLUMN compound FLOAT")
    conn.commit()

def insertSentiment(tweetId, dict, cur):
    cur.execute("UPDATE tweets SET neu = %s, neg = %s, pos = %s, compound = %s WHERE tweets.id = %s;", (dict['neu'], dict['neg'], dict['pos'], dict['compound'], tweetId))

def executeSelect(conn):
    start = time.time();
    df = sqlio.read_sql_query("""SELECT DISTINCT(t.id), t.content FROM tweets as t
                              JOIN tweet_hashtags as th ON t.id=th.tweet_id
                              JOIN hashtags as h ON th.hashtag_id=h.id
                              WHERE h.value ILIKE any(array[
                                  '%DeepstateVirus%',
                                  '%DeepStateFauci%',
                                  '%DeepStateVaccine%',
                                  '%QAnon%',
                                  '%Agenda21%',
                                  '%CCPVirus%',
                                  '%ClimateChangeHoax%',
                                  '%GlobalWarmingHoax%',
                                  '%ChinaLiedPeopleDied%',
                                  '%5GCoronavirus%',
                                  '%SorosVirus%',
                                  '%MAGA%',
                                  '%WWG1WGA%',
                                  '%Chemtrails%',
                                  '%flatEarth%',
                                  '%MoonLandingHoax%',
                                  '%moonhoax%',
                                  '%911truth%',
                                  '%911insidejob%',
                                  '%illuminati%',
                                  '%reptilians%',
                                  '%pizzaGateIsReal%',
                                  '%PedoGateIsReal%'
                              ])""", conn)
    print("Number of rows: ", df.shape[0])
    end = time.time();
    print("Select completed, elapsed time was ", end - start, "\n");
    start = time.time();
    cur = conn.cursor()
    #uloha 2
    #############################################
    #runtime 1hour
    #sia = SentimentIntensityAnalyzer()
    #addColumns(conn, cur)
    #df.apply(lambda x: handleTweet(x['id'], x['content'], cur, sia), axis = 1)
    #DONE
    #############################################
    #uloha3
    #############################################
    #CREATE TABLE conspiracies ( id int NOT NULL, value varchar(50), PRIMARY KEY (ID))
    #CREATE TABLE tweet_conspiracies (
    #    id int NOT NULL,
    #    tweet_id varchar(20) NOT NULL,
    #    conspiracy_id int NOT NULL,
    #    PRIMARY KEY(id),
    #    FOREIGN KEY(tweet_id) REFERENCES tweets(id),
    #    FOREIGN KEY(conspiracy_id) REFERENCES conspiracies(id)
    #)
    #fill the table with conspiracies now
    #SELECT COUNT(*) FROM tweets WHERE compound is not NULL
    # theories = ['Deepstate', 'Qanon', 'New World Order', 'The virus escaped from a Chinese lab ', 'Global Warming is HOAX', 'COVID19 and microchipping', 'COVID19 is preaded by 5G', 'Moon landing is fake '
    # , '9/11 was inside job', 'Pizzagate conspiracy theory', 'Chemtrails', 'Illuminati', 'Reptilian conspiracy theory']
    # INSERT INTO conspiracies(id, value) VALUES %s %s
    #cez df prejdem a podla hastagov mapujem do novej tabulky idcka conspiracies a tweets (many to many)
    #############################################
    #uloha4
    #############################################
    #BLABLABLA
    #############################################
    #uloha5
    #############################################
    #BLABLABLA
    #############################################
    #uloha6
    #############################################
    #BLABLABLA
    #############################################
    conn.commit()
    cur.close()
    end = time.time()
    print("Update completed, elapsed time was ", end - start, "\n")
    #df = df.apply(lambda x: insertSentiment(x['id'], sentiment(x['content']), conn), axis = 1)
    #print(df.to_string())

    # cur = conn.cursor()
    # cur.execute("SELECT * from tweets LIMIT 100")
    # rows = cur.fetchall()
    # df = pd.DataFrame(columns=['id', 'content'])
    # for i, row in enumerate(rows):
    #     df = df.append([row[0], row[1]])
    # print(df.to_string())

#main
if __name__ == "__main__":
    conn = connect()
    #main pipeline
    #select tweets with conspiracy hashtags, remove emotes, mentions and hashtags.
    #calculate sentiment using VADER library
    executeSelect(conn)
    if conn is not None:
        conn.close()
        print("Connection closed.")


        

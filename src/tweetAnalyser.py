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

def addConspiracyTable(conn, cur):
    cur.execute("CREATE TABLE conspiracies ( id SERIAL PRIMARY KEY, value varchar(50))")
    conn.commit()

# def addConspiracyHashtagsTable(conn, cur):
#     cur.execute("""CREATE TABLE conspiracy_hashtags(
# 	    id SERIAL PRIMARY KEY,
# 	    conspiracy_id int,
# 	    hashtag_id int,
# 	    FOREIGN KEY(conspiracy_id) REFERENCES conspiracies(id),
# 	    FOREIGN KEY(hashtag_id) REFERENCES hashtags(id))"""
#     )
#     conn.commit()

def fillConspiracyTable(cur, data):
    data = [(value,) for value in data]
    cur.executemany("INSERT INTO conspiracies(value) VALUES(%s)", data)

def addTweetConspiracyTable(conn, cur):
    cur.execute("""CREATE TABLE tweet_conspiracies (
        id SERIAL PRIMARY KEY,
        tweet_id varchar(20) NOT NULL,
        conspiracy_id int NOT NULL,
        FOREIGN KEY(tweet_id) REFERENCES tweets(id),
        FOREIGN KEY(conspiracy_id) REFERENCES conspiracies(id)
        )"""
    )
    conn.commit()

def addConspiracyInWeeksTable(conn, cur):
    cur.execute("""CREATE TABLE conspiracies_in_weeks (
        id SERIAL PRIMARY KEY,
        week float,
        year float,
        conspiracy_id int NOT NULL,
        tweet_ratio float,
        tweet_count bigint,
        tweet_extreme_count bigint,
        tweet_neutral_count bigint,
        FOREIGN KEY(conspiracy_id) REFERENCES conspiracies(id)
        )"""
    )
    conn.commit()

def fillConspiracyInWeeksTable(cur):
    cur.execute("""INSERT INTO conspiracies_in_weeks(week, year, conspiracy_id, tweet_ratio, tweet_count, tweet_extreme_count, tweet_neutral_count)
        SELECT date_week, date_year, conspiracy_id, CASE WHEN tweet_neutral_count = 0 THEN NULL ELSE CAST(tweet_extreme_count as float) / CAST(tweet_neutral_count as float) END AS ratio, tweet_count, tweet_extreme_count, tweet_neutral_count
        FROM (
        SELECT date_part('week', t.happened_at) date_week, date_part('year', t.happened_at) date_year, tc.conspiracy_id,
        count(*) tweet_count, sum(CASE WHEN t.compound >= 0.5 OR t.compound <= -0.5 THEN 1 ELSE 0 END) tweet_extreme_count,
        sum(CASE WHEN t.compound < 0.5 AND t.compound > -0.5 THEN 1 ELSE 0 END) tweet_neutral_count
        FROM tweet_conspiracies as tc 
        JOIN tweets as t on t.id = tc.tweet_id 
        GROUP BY date_part('week', t.happened_at), date_part('year', t.happened_at), tc.conspiracy_id
        ORDER BY date_part('year', t.happened_at) asc, date_part('week', t.happened_at) asc) as sub;
        """
    )


def insertSentiment(tweetId, dict, cur):
    cur.execute("UPDATE tweets SET neu = %s, neg = %s, pos = %s, compound = %s WHERE tweets.id = %s;", (dict['neu'], dict['neg'], dict['pos'], dict['compound'], tweetId))

def insertTweetConspiracy(cur):
    #create many-to-many table between tables tweets and conspiracies (mapping only conspiracy tweets)
    cur.execute("""INSERT INTO tweet_conspiracies(tweet_id, conspiracy_id)
        SELECT DISTINCT sub.id, sub.conspiracy FROM (SELECT t.id, CASE 
        WHEN hash.value ILIKE '%deepstate%' THEN (SELECT id FROM conspiracies WHERE value ILIKE  '%Deepstate%')
        WHEN hash.value ILIKE '%qanon%' OR hash.value ILIKE '%MAGA%' OR hash.value ILIKE '%WWG1WGA%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Qanon%')
        WHEN hash.value ILIKE '%Agenda21%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%New world order%')
        WHEN hash.value ILIKE '%CCPVirus%' OR hash.value ILIKE '%ChinaLiedPeopleDied%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Chinese lab%')
        WHEN hash.value ILIKE '%ClimateChangeHoax%' OR hash.value ILIKE '%GlobalWarmingHoax%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Global warming%')
        WHEN hash.value ILIKE '%SorosVirus%' OR hash.value ILIKE '%BillGates%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%microchipping%')
        WHEN hash.value ILIKE '%5gCoronavirus%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%5G%')
        WHEN hash.value ILIKE '%moonhoax%' OR hash.value ILIKE '%moonLandingHoax%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Moon landing%')
        WHEN hash.value ILIKE '%911%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%9/11%')
        WHEN hash.value ILIKE '%pizzaGate%' OR hash.value ILIKE '%pedoGate%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Pizzagate%')
        WHEN hash.value ILIKE '%chemtrails%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Chemtrails%')
        WHEN hash.value ILIKE '%flatEarth%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%FlatEarth%')
        WHEN hash.value ILIKE '%illuminati%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Illuminati%')
        WHEN hash.value ILIKE '%reptilians%' THEN (SELECT id FROM conspiracies WHERE value ILIKE '%Reptilian%')
        END AS conspiracy FROM tweets as t
        JOIN tweet_hashtags as th ON t.id = th.tweet_id JOIN hashtags as hash ON th.hashtag_id = hash.id WHERE compound is not NULL) as sub
        WHERE sub.conspiracy IS NOT NULL;"""
    )

def generateGraphs(cur):
    cur.execute("SELECT week, year")

def execute(conn):
    # start = time.time();
    # df = sqlio.read_sql_query("""SELECT DISTINCT(t.id), t.content FROM tweets as t
    #                           JOIN tweet_hashtags as th ON t.id=th.tweet_id
    #                           JOIN hashtags as h ON th.hashtag_id=h.id
    #                           WHERE h.value ILIKE any(array[
    #                               '%DeepstateVirus%',
    #                               '%DeepStateFauci%',
    #                               '%DeepStateVaccine%',
    #                               '%QAnon%',
    #                               '%Agenda21%',
    #                               '%CCPVirus%',
    #                               '%ClimateChangeHoax%',
    #                               '%GlobalWarmingHoax%',
    #                               '%ChinaLiedPeopleDied%',
    #                               '%5GCoronavirus%',
    #                               '%SorosVirus%',
    #                               '%MAGA%',
    #                               '%WWG1WGA%',
    #                               '%Chemtrails%',
    #                               '%flatEarth%',
    #                               '%MoonLandingHoax%',
    #                               '%moonhoax%',
    #                               '%911truth%',
    #                               '%911insidejob%',
    #                               '%illuminati%',
    #                               '%reptilians%',
    #                               '%pizzaGateIsReal%',
    #                               '%PedoGateIsReal%'
    #                           ])""", conn)
    # print("Number of rows: ", df.shape[0])
    # end = time.time();
    # print("Select completed, elapsed time was ", end - start, "\n");
    # start = time.time()
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
    # try:
    #     addConspiracyTable(conn, cur)
    #     addTweetConspiracyTable(conn, cur) 
    # except:
    #     print('tables already exist.')

    # fillConspiracyTable(
    #     cur, ['Deepstate', 'Qanon', 'New World Order', 'The virus escaped from a Chinese lab ', 'Global Warming is HOAX'
    #     , 'COVID19 and microchipping', 'COVID19 is spreaded by 5G', 'Moon landing is fake', '9/11 was an inside job'
    #     , 'Pizzagate conspiracy theory', 'Chemtrails' , 'FlatEarth', 'Illuminati', 'Reptilian conspiracy theory']
    # )
    #start = time.time()
    #insertTweetConspiracy(cur)
    #end = time.time();
    #print("Insert many-to-many completed, elapsed time was ", end - start, "\n");
    #DONE
    #############################################



    #uloha4
    #############################################
    start = time.time()
    
    try:
        addConspiracyInWeeksTable(conn, cur)
        print("Conspiracy in weeks table created")
        fillConspiracyInWeeksTable(cur)
        conn.commit()
        end = time.time();
        print("Conspiracy in weeks table filled, elapsed time was ", end - start, "\n");
    except:
        print("Drop the table and try again.")
    
    #generateGraphs(cur)
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
    #end = time.time()
    #print("Update completed, elapsed time was ", end - start, "\n")

#main
if __name__ == "__main__":
    conn = connect()
    #main pipeline
    #select tweets with conspiracy hashtags, remove emotes, mentions and hashtags.
    #calculate sentiment using VADER library
    execute(conn)
    if conn is not None:
        conn.close()
        print("Connection closed.")


        

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2

def connect():
    #connect to PG database
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

def executeSelect(conn):
    cur = conn.cursor()
    cur.execute("SELECT * from tweets LIMIT 100")
    rows = cur.fetchall()
    for row in rows:
        print("Id = ", row[0], )
        print("content = ", row[1], "\n")


#main
if __name__ == "__main__":
    conn = connect();
    #main pipeline
    #select tweets with conspiracy hashtags, remove emotes, mentions and hashtags.
    #calculate sentiment using VADER library
    executeSelect(conn);
    if conn is not None:
        conn.close()
        print("Connection closed.")


        

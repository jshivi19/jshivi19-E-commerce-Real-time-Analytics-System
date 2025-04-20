"""
Script to store processed tweet data into PostgreSQL
"""
import json
import psycopg2
from kafka import KafkaConsumer
from config.settings import POSTGRES_CONFIG, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

def store_tweet_data(tweet):
    """Insert tweet data into the PostgreSQL database"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tweets (tweet_id, text, user_id, created_at, hashtags)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (tweet_id) DO NOTHING
            """, (
                tweet["id"],
                tweet["text"],
                tweet["user"],
                tweet["created_at"],
                json.dumps(tweet.get("hashtags", []))
            ))
            conn.commit()
    except Exception as e:
        print(f"Error storing tweet data: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    consumer = KafkaConsumer(
        KAFKA_TOPICS["tweets_topic"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="store_tweet_group",
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("Listening for processed tweets...")
    for message in consumer:
        store_tweet_data(message.value)

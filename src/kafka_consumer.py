from kafka import KafkaConsumer
import json
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

def process_tweet(tweet):
    """Process the tweet data (e.g., print or save to a database)"""
    print(f"Processing tweet: {tweet}")

if __name__ == "__main__":
    consumer = KafkaConsumer(
        KAFKA_TOPICS["tweets_topic"],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="tweet_group",
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("Kafka consumer started. Listening for tweets...")
    for message in consumer:
        process_tweet(message.value)

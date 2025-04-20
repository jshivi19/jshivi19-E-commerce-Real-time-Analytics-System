import tweepy
from kafka import KafkaProducer
import json

# Twitter API credentials
API_KEY = "JLjTkSbIFUmnAIb6IQtbwW5Ie"
API_SECRET = "9fufGHD73L4UW6AslfK3OPubBrNeSsposvk2mMr1LrjZ3rHli3"

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TWEETS_TOPIC = "tweets_topic"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Stream Listener class
class TwitterStreamListener(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        try:
            # Publish tweet to Kafka
            producer.send(TWEETS_TOPIC, {"id": tweet.id, "text": tweet.text})
            print(f"Tweet published: {tweet.text}")
        except Exception as e:
            print(f"Error publishing tweet: {e}")

    def on_error(self, status_code):
        print(f"Error: {status_code}")
        if status_code == 420:
            # Disconnect on rate limit
            return False

# Main function
def main():
    # Initialize Twitter stream listener
    stream_listener = TwitterStreamListener(API_KEY, API_SECRET)
    # Add filters (e.g., track keywords)
    stream_listener.add_rules(tweepy.StreamRule("keyword"))
    # Start streaming
    stream_listener.filter(tweet_fields=["text"])

if __name__ == "__main__":
    main()

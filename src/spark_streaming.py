from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import StructType, StringType
from pyspark.sql.streaming import DataStreamWriter

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "tweets_topic"

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("TwitterSparkStreaming") \
        .getOrCreate()

    # Define schema for incoming tweets
    schema = StructType() \
        .add("id", StringType()) \
        .add("text", StringType()) \
        .add("user", StringType()) \
        .add("created_at", StringType())

    # Read data from Kafka
    tweets_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Deserialize Kafka value
    tweets_df = tweets_df.selectExpr("CAST(value AS STRING)") \
        .select(split("value", ",").alias("tweet_data")) \
        .selectExpr("tweet_data[0] as id", "tweet_data[1] as text", "tweet_data[2] as user", "tweet_data[3] as created_at")

    # Perform basic analysis (e.g., count hashtags)
    hashtags_df = tweets_df.select(explode(split(tweets_df.text, " ")).alias("word")) \
        .filter("word LIKE '#%'") \
        .groupBy("word") \
        .count() \
        .orderBy("count", ascending=False)

    # Write output to console
    query = hashtags_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()

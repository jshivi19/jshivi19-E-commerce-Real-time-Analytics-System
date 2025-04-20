"""
Script to compare performance of streaming and batch processing
"""
import time
from scripts.batch_processing import get_top_hashtags
from pyspark.sql import SparkSession

def measure_batch_performance():
    """Measure execution time for batch processing"""
    start_time = time.time()
    get_top_hashtags()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Batch processing execution time: {execution_time:.2f} seconds")
    print(f"Resource usage: Memory and CPU metrics can be added here.")

def measure_streaming_performance():
    """Measure execution time for streaming processing"""
    spark = SparkSession.builder \
        .appName("PerformanceComparison") \
        .getOrCreate()

    start_time = time.time()
    # Simulate streaming processing by reading a static dataset
    df = spark.read.json("path_to_sample_tweets.json")
    hashtags_df = df.selectExpr("explode(split(text, ' ')) as word") \
        .filter("word LIKE '#%'") \
        .groupBy("word") \
        .count() \
        .orderBy("count", ascending=False)
    hashtags_df.show()
    end_time = time.time()
    print(f"Streaming processing execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    print("Comparing performance of streaming and batch processing...")
    measure_batch_performance()
    measure_streaming_performance()

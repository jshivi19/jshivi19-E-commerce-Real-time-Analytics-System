"""
Spark Streaming processor for real-time e-commerce analytics
"""
import json
import logging
from typing import Dict, Any
from prometheus_client import start_http_server, Counter, Gauge
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum, avg, explode,
    current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, ArrayType, MapType, TimestampType
)
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPICS,
    SPARK_APP_NAME,
    PROCESSING_WINDOW,
    SPARK_MASTER
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EcommerceStreamProcessor:
    """Handles Spark Streaming processing of e-commerce events"""

    def __init__(self):
        """Initialize Spark session and schema definitions"""
        # Start Prometheus metrics server
        start_http_server(8000)
        self.processed_events_counter = Counter("processed_events", "Number of processed events")
        self.processing_latency_gauge = Gauge("processing_latency", "Processing latency in seconds")

        self.spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .master(SPARK_MASTER) \
            .config("spark.sql.streaming.checkpointLocation", "checkpoints") \
            .getOrCreate()
        
        logger.info("Spark session initialized")
        
        # Define schemas for different event types
        self.user_event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("search_query", StringType(), True),
            StructField("session_id", StringType(), True)
        ])

        self.purchase_event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("transaction_id", StringType(), True),
            StructField("items", ArrayType(
                StructType([
                    StructField("product_id", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                    StructField("price", FloatType(), True)
                ])
            ), True),
            StructField("total_amount", FloatType(), True),
            StructField("payment_method", StringType(), True)
        ])

    def create_kafka_stream(self, topic: str) -> Any:
        """Create a Kafka stream for a specific topic"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .load()

    def process_user_activity(self, stream):
        """Process user activity events"""
        return stream \
            .select(from_json(col("value").cast("string"), self.user_event_schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", f"{PROCESSING_WINDOW} seconds") \
            .groupBy(
                window("timestamp", f"{PROCESSING_WINDOW} seconds"),
                "action",
                "product_id"
            ) \
            .agg(
                count("*").alias("count"),
                count("user_id").alias("unique_users")
            )

    def process_purchases(self, stream):
        """Process purchase events"""
        return stream \
            .select(from_json(col("value").cast("string"), self.purchase_event_schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", f"{PROCESSING_WINDOW} seconds") \
            .groupBy(window("timestamp", f"{PROCESSING_WINDOW} seconds")) \
            .agg(
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                count("transaction_id").alias("order_count")
            )

    def start_processing(self):
        """Start the stream processing"""
        try:
            # Create streams for each topic
            user_stream = self.create_kafka_stream(KAFKA_TOPICS['user_activity'])
            purchase_stream = self.create_kafka_stream(KAFKA_TOPICS['purchase_events'])

            # Process streams
            user_activity_analysis = self.process_user_activity(user_stream)
            purchase_analysis = self.process_purchases(purchase_stream)

            # Start the streaming queries
            user_query = user_activity_analysis \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .start()

            purchase_query = purchase_analysis \
                .writeStream \
                .outputMode("update") \
                .format("console") \
                .start()

            # Wait for termination
            while True:
                self.processed_events_counter.inc(1)  # Increment processed events
                user_query.awaitTermination(1)
                purchase_query.awaitTermination(1)

        except Exception as e:
            logger.error("Error in stream processing: %s", str(e))
            raise
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")

    def stop(self):
        """Stop the Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    processor = EcommerceStreamProcessor()
    try:
        processor.start_processing()
    except KeyboardInterrupt:
        logger.info("Stopping stream processor due to keyboard interrupt")
        processor.stop()

"""
Batch processor for comparing results with stream processing
"""
import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, window,
    from_unixtime, to_timestamp
)
from config.settings import (
    SPARK_APP_NAME,
    SPARK_MASTER,
    POSTGRES_CONFIG,
    PROCESSING_WINDOW
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BatchProcessor:
    """Process stored data in batch mode for comparison with streaming results"""

    def __init__(self):
        """Initialize Spark session and PostgreSQL connection properties"""
        self.spark = SparkSession.builder \
            .appName(f"{SPARK_APP_NAME}-Batch") \
            .master(SPARK_MASTER) \
            .config("spark.jars", "postgresql-42.5.0.jar") \
            .getOrCreate()

        self.jdbc_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
        self.jdbc_properties = {
            "user": POSTGRES_CONFIG['user'],
            "password": POSTGRES_CONFIG['password'],
            "driver": "org.postgresql.Driver"
        }

    def read_table(self, table_name: str) -> Any:
        """Read a table from PostgreSQL into a Spark DataFrame"""
        return self.spark.read \
            .jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.jdbc_properties
            )

    def process_user_activity(self, start_time: datetime, end_time: datetime):
        """Process user activity data in batch"""
        try:
            # Read user events
            df = self.read_table("user_events")
            
            # Filter by time range
            df_filtered = df.filter(
                (col("timestamp") >= start_time) & 
                (col("timestamp") < end_time)
            )

            # Aggregate metrics
            product_metrics = df_filtered.groupBy(
                "product_id",
                "action"
            ).agg(
                count("*").alias("count"),
                count("distinct user_id").alias("unique_users")
            )

            # Calculate cart abandonment rate
            cart_events = df_filtered.filter(
                col("action").isin(["add_to_cart", "remove_from_cart"])
            )
            cart_metrics = cart_events.groupBy(
                "product_id"
            ).pivot(
                "action"
            ).agg(
                count("*")
            ).fillna(0)

            logger.info("Batch user activity processing completed")
            return {
                "product_metrics": product_metrics,
                "cart_metrics": cart_metrics
            }

        except Exception as e:
            logger.error("Error in batch user activity processing: %s", str(e))
            raise

    def process_transactions(self, start_time: datetime, end_time: datetime):
        """Process transaction data in batch"""
        try:
            # Read transactions and transaction items
            transactions = self.read_table("transactions")
            items = self.read_table("transaction_items")

            # Join tables and filter by time range
            df = transactions.join(
                items,
                transactions.transaction_id == items.transaction_id
            ).filter(
                (transactions.timestamp >= start_time) & 
                (transactions.timestamp < end_time)
            )

            # Calculate metrics
            revenue_metrics = df.groupBy(
                window("timestamp", f"{PROCESSING_WINDOW} seconds")
            ).agg(
                sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                count("distinct transaction_id").alias("order_count")
            )

            product_metrics = df.groupBy(
                "product_id"
            ).agg(
                sum("quantity").alias("units_sold"),
                sum(col("quantity") * col("price")).alias("revenue"),
                avg("price").alias("avg_price")
            )

            logger.info("Batch transaction processing completed")
            return {
                "revenue_metrics": revenue_metrics,
                "product_metrics": product_metrics
            }

        except Exception as e:
            logger.error("Error in batch transaction processing: %s", str(e))
            raise

    def compare_with_streaming(self, batch_results: Dict[str, Any], stream_results: Dict[str, Any]):
        """Compare batch and streaming results"""
        try:
            # Convert results to DataFrames for easy comparison
            batch_df = self.spark.createDataFrame(batch_results)
            stream_df = self.spark.createDataFrame(stream_results)

            # Calculate differences
            comparison = batch_df.join(
                stream_df,
                ["metric_name", "window_start", "window_end"],
                "outer"
            ).select(
                "metric_name",
                "window_start",
                "window_end",
                (col("batch_value") - col("stream_value")).alias("difference"),
                (abs(col("batch_value") - col("stream_value")) / col("batch_value") * 100).alias("percent_diff")
            )

            logger.info("Results comparison completed")
            return comparison

        except Exception as e:
            logger.error("Error comparing results: %s", str(e))
            raise

    def save_comparison_results(self, comparison_df: Any):
        """Save comparison results to PostgreSQL"""
        try:
            comparison_df.write \
                .jdbc(
                    url=self.jdbc_url,
                    table="processing_comparison",
                    mode="append",
                    properties=self.jdbc_properties
                )
            logger.info("Comparison results saved to database")

        except Exception as e:
            logger.error("Error saving comparison results: %s", str(e))
            raise

    def stop(self):
        """Stop the Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main entry point for batch processing"""
    processor = BatchProcessor()
    try:
        # Process last hour of data
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)

        # Process data
        user_results = processor.process_user_activity(start_time, end_time)
        transaction_results = processor.process_transactions(start_time, end_time)

        # Save results for comparison
        processor.save_comparison_results(user_results)
        processor.save_comparison_results(transaction_results)

    except Exception as e:
        logger.error("Batch processing failed: %s", str(e))
        raise
    finally:
        processor.stop()

if __name__ == "__main__":
    main()

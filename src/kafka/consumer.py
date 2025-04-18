"""
Kafka consumer for receiving e-commerce events
"""
import json
import logging
from typing import Dict, Any, Callable, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EcommerceKafkaConsumer:
    """Handles consuming messages from Kafka topics"""

    def __init__(
        self,
        topics: Optional[list] = None,
        group_id: str = 'ecommerce_consumer_group',
        auto_offset_reset: str = 'latest'
    ):
        """
        Initialize the Kafka consumer
        
        Args:
            topics: List of topics to subscribe to. If None, subscribes to all topics.
            group_id: Consumer group ID
            auto_offset_reset: Where to start reading messages ('latest' or 'earliest')
        """
        self.topics = topics or list(KAFKA_TOPICS.values())
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda v: v.decode('utf-8') if v else None,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        logger.info(
            "Kafka consumer initialized for topics %s with bootstrap servers: %s",
            self.topics,
            KAFKA_BOOTSTRAP_SERVERS
        )

    def consume_events(self, handler: Callable[[str, Dict[str, Any]], None]) -> None:
        """
        Consume events and process them with the provided handler
        
        Args:
            handler: Callback function that takes topic and message as arguments
        """
        try:
            for message in self.consumer:
                try:
                    topic = message.topic
                    key = message.key
                    value = message.value
                    
                    logger.debug(
                        "Received message - Topic: %s, Partition: %s, Offset: %s",
                        topic,
                        message.partition,
                        message.offset
                    )
                    
                    # Process message with handler
                    handler(topic, value)
                    
                except Exception as e:
                    logger.error("Error processing message: %s", str(e))
                    continue
                    
        except KafkaError as e:
            logger.error("Kafka consumer error: %s", str(e))
            raise
        except KeyboardInterrupt:
            logger.info("Stopping consumer due to keyboard interrupt")
        finally:
            self.close()

    def close(self):
        """Close the Kafka consumer"""
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed successfully")
        except Exception as e:
            logger.error("Error closing Kafka consumer: %s", str(e))
            raise

def example_message_handler(topic: str, message: Dict[str, Any]) -> None:
    """Example handler function for processing consumed messages"""
    logger.info("Received message from topic %s: %s", topic, message)

if __name__ == "__main__":
    # Example usage
    consumer = EcommerceKafkaConsumer()
    consumer.consume_events(example_message_handler)

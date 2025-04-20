"""
Kafka producer for sending e-commerce events
"""
import json
import logging
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EcommerceKafkaProducer:
    """Handles producing messages to Kafka topics"""

    def __init__(self):
        """Initialize the Kafka producer with configuration"""
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=str.encode,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        self.topics = KAFKA_TOPICS
        logger.info("Kafka producer initialized with bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)

    def _on_send_success(self, record_metadata):
        """Callback for successful message sending"""
        logger.debug(
            "Message delivered to topic %s partition %s offset %s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset
        )

    def _on_send_error(self, excp):
        """Callback for message sending errors"""
        logger.error("Error sending message to Kafka: %s", str(excp))

    def send_event(self, event_type: str, event_data: Dict[str, Any], key: str = None) -> None:
        """
        Send an event to the appropriate Kafka topic. Extended to handle tweet-specific data.
        
        Args:
            event_type: Type of event ('user_activity', 'purchase', 'inventory', 'tweet')
            event_data: Event data to be sent
            key: Optional message key for partitioning
        """
        try:
            # Map event type to topic
            topic = self.topics.get(event_type)
            if not topic:
                logger.error("Unknown event type: %s", event_type)
                return

            # If no key provided, use event ID from data
            if event_type == 'tweet':
                # Ensure tweet-specific fields are present
                required_fields = ['tweet_id', 'user_id', 'text', 'timestamp']
                for field in required_fields:
                    if field not in event_data:
                        logger.error("Missing required field '%s' in tweet data", field)
                        return

            if not key and isinstance(event_data, dict):
                key = event_data.get('event_id', str(hash(json.dumps(event_data))))

            # Send message
            future = self.producer.send(
                topic=topic,
                key=key,
                value=event_data
            )
            future.add_callback(self._on_send_success).add_errback(self._on_send_error)
            
        except Exception as e:
            logger.error("Failed to send event: %s", str(e))
            raise

    def close(self):
        """Close the Kafka producer"""
        try:
            self.producer.flush()  # Ensure all messages are sent
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error("Error closing Kafka producer: %s", str(e))
            raise

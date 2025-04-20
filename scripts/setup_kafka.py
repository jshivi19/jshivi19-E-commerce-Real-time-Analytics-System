"""
Script to initialize Kafka topics for the E-commerce Analytics System
"""
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_topics():
    """Create Kafka topics if they don't exist"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id='ecommerce-admin'
        )

        # Define topics with their configurations
        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=3,  # Multiple partitions for parallelism
                replication_factor=1  # Set to higher value in production
            )
            for topic_name in settings.kafka_topics.values()
        ]

        # Create topics
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info("Successfully created Kafka topics: %s", list(settings.kafka_topics.values()))

    except TopicAlreadyExistsError:
        logger.info("Topics already exist")
    except Exception as e:
        logger.error("Error creating Kafka topics: %s", str(e))
        raise
    finally:
        admin_client.close()

def delete_topics():
    """Delete Kafka topics if they exist (useful for testing)"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id='ecommerce-admin'
        )

        # Delete topics
        admin_client.delete_topics(list(settings.kafka_topics.values()))
        logger.info("Successfully deleted Kafka topics: %s", list(settings.kafka_topics.values()))

    except Exception as e:
        logger.error("Error deleting Kafka topics: %s", str(e))
        raise
    finally:
        admin_client.close()

def main():
    """Main entry point"""
    import argparse
    parser = argparse.ArgumentParser(description='Setup Kafka topics')
    parser.add_argument('--delete', action='store_true', help='Delete existing topics')
    args = parser.parse_args()

    try:
        if args.delete:
            delete_topics()
        create_topics()
    except Exception as e:
        logger.error("Setup failed: %s", str(e))
        exit(1)

if __name__ == "__main__":
    main()

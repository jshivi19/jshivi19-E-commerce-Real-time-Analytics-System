"""
Main entry point for the E-commerce Analytics System
"""
import time
import logging
import signal
import sys
from threading import Thread, Event
from typing import Dict, Any

from data_generator.generator import EcommerceDataGenerator
from kafka.producer import EcommerceKafkaProducer
from spark.stream_processor import EcommerceStreamProcessor
from database.db_handler import DatabaseHandler
from config.settings import SIMULATION_INTERVAL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EcommerceAnalyticsSystem:
    """Coordinates all components of the e-commerce analytics system"""

    def __init__(self):
        """Initialize system components"""
        self.stop_event = Event()
        self.data_generator = EcommerceDataGenerator()
        self.kafka_producer = EcommerceKafkaProducer()
        self.spark_processor = EcommerceStreamProcessor()
        self.db_handler = DatabaseHandler()
        logger.info("E-commerce Analytics System initialized")

    def generate_and_send_events(self):
        """Continuously generate and send events to Kafka"""
        try:
            while not self.stop_event.is_set():
                # Generate a random event
                event = self.data_generator.generate_event()
                event_type = event['type']
                event_data = event['data']

                # Send event to Kafka
                self.kafka_producer.send_event(event_type, event_data)
                logger.debug("Event sent to Kafka: %s", event_type)

                # Sleep for the simulation interval
                time.sleep(SIMULATION_INTERVAL)
        except Exception as e:
            logger.error("Error in event generation thread: %s", str(e))
            self.stop()

    def handle_signals(self):
        """Handle system signals for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info("Received shutdown signal")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def start(self):
        """Start all system components"""
        try:
            logger.info("Starting E-commerce Analytics System")
            
            # Start event generation in a separate thread
            generator_thread = Thread(
                target=self.generate_and_send_events,
                name="EventGenerator"
            )
            generator_thread.daemon = True
            generator_thread.start()

            # Start Spark streaming process
            self.spark_processor.start_processing()

        except Exception as e:
            logger.error("Error starting system: %s", str(e))
            self.stop()
            raise

    def stop(self):
        """Stop all system components"""
        logger.info("Stopping E-commerce Analytics System")
        self.stop_event.set()
        
        try:
            self.kafka_producer.close()
        except Exception as e:
            logger.error("Error closing Kafka producer: %s", str(e))

        try:
            self.spark_processor.stop()
        except Exception as e:
            logger.error("Error stopping Spark processor: %s", str(e))

        try:
            self.db_handler.close()
        except Exception as e:
            logger.error("Error closing database connection: %s", str(e))

def main():
    """Main entry point"""
    system = EcommerceAnalyticsSystem()
    system.handle_signals()
    
    try:
        system.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        system.stop()
    except Exception as e:
        logger.error("System error: %s", str(e))
        system.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()

"""
Unit tests for Kafka producer and consumer
"""
import unittest
from unittest.mock import patch, MagicMock, call
import json
from kafka.errors import KafkaError
from src.kafka.producer import EcommerceKafkaProducer
from src.kafka.consumer import EcommerceKafkaConsumer
from src.data_generator.events import UserEvent, PurchaseEvent

class TestEcommerceKafkaProducer(unittest.TestCase):
    """Test cases for EcommerceKafkaProducer class"""

    @patch('src.kafka.producer.KafkaProducer')
    def setUp(self, mock_kafka_producer):
        """Set up test fixtures"""
        self.mock_producer = mock_kafka_producer.return_value
        self.producer = EcommerceKafkaProducer()

    def test_producer_initialization(self):
        """Test producer initialization with correct configuration"""
        self.assertIsNotNone(self.producer)
        self.assertIsNotNone(self.producer.topics)
        self.assertEqual(self.producer.producer, self.mock_producer)

    def test_send_event_success(self):
        """Test successful event sending"""
        # Create test event
        event = UserEvent(
            event_id="test-id",
            timestamp="2025-01-01T00:00:00",
            event_type="user_activity",
            user_id="user-1",
            action="view",
            product_id="product-1"
        )
        event_data = json.loads(event.to_json())

        # Configure mock
        future = MagicMock()
        self.mock_producer.send.return_value = future

        # Send event
        self.producer.send_event("user_activity", event_data)

        # Verify calls
        self.mock_producer.send.assert_called_once()
        future.add_callback.assert_called_once()
        future.add_errback.assert_called_once()

    def test_send_event_unknown_type(self):
        """Test sending event with unknown type"""
        event_data = {"test": "data"}
        self.producer.send_event("unknown_type", event_data)
        self.mock_producer.send.assert_not_called()

    def test_send_event_error_handling(self):
        """Test error handling during event sending"""
        # Configure mock to raise error
        self.mock_producer.send.side_effect = KafkaError("Test error")

        event_data = {"test": "data"}
        
        with self.assertRaises(Exception):
            self.producer.send_event("user_activity", event_data)

    def test_close_producer(self):
        """Test producer cleanup"""
        self.producer.close()
        self.mock_producer.flush.assert_called_once()
        self.mock_producer.close.assert_called_once()

class TestEcommerceKafkaConsumer(unittest.TestCase):
    """Test cases for EcommerceKafkaConsumer class"""

    @patch('src.kafka.consumer.KafkaConsumer')
    def setUp(self, mock_kafka_consumer):
        """Set up test fixtures"""
        self.mock_consumer = mock_kafka_consumer.return_value
        self.consumer = EcommerceKafkaConsumer(
            topics=['test_topic'],
            group_id='test_group'
        )

    def test_consumer_initialization(self):
        """Test consumer initialization with correct configuration"""
        self.assertIsNotNone(self.consumer)
        self.assertEqual(self.consumer.topics, ['test_topic'])
        self.assertEqual(self.consumer.consumer, self.mock_consumer)

    def test_consume_events_success(self):
        """Test successful event consumption"""
        # Create test messages
        message1 = MagicMock()
        message1.topic = 'test_topic'
        message1.value = {'event_type': 'test', 'data': 'test_data1'}
        
        message2 = MagicMock()
        message2.topic = 'test_topic'
        message2.value = {'event_type': 'test', 'data': 'test_data2'}

        # Configure mock consumer
        self.mock_consumer.__iter__.return_value = [message1, message2]

        # Create mock handler
        mock_handler = MagicMock()

        # Start consuming
        self.consumer.consume_events(mock_handler)

        # Verify handler calls
        expected_calls = [
            call('test_topic', {'event_type': 'test', 'data': 'test_data1'}),
            call('test_topic', {'event_type': 'test', 'data': 'test_data2'})
        ]
        mock_handler.assert_has_calls(expected_calls)

    def test_consume_events_handler_error(self):
        """Test handling of errors in message handler"""
        # Create test message
        message = MagicMock()
        message.topic = 'test_topic'
        message.value = {'event_type': 'test', 'data': 'test_data'}

        # Configure mock consumer
        self.mock_consumer.__iter__.return_value = [message]

        # Create failing handler
        def failing_handler(topic, message):
            raise Exception("Handler error")

        # Start consuming - should not raise exception
        self.consumer.consume_events(failing_handler)

    def test_consume_events_kafka_error(self):
        """Test handling of Kafka errors"""
        # Configure mock to raise KafkaError
        self.mock_consumer.__iter__.side_effect = KafkaError("Kafka error")

        with self.assertRaises(KafkaError):
            self.consumer.consume_events(lambda t, m: None)

    def test_close_consumer(self):
        """Test consumer cleanup"""
        self.consumer.close()
        self.mock_consumer.close.assert_called_once()

    @patch('src.kafka.consumer.KafkaConsumer')
    def test_consumer_with_auto_offset_reset(self, mock_kafka_consumer):
        """Test consumer initialization with different auto_offset_reset values"""
        # Test with 'earliest'
        consumer = EcommerceKafkaConsumer(
            topics=['test_topic'],
            auto_offset_reset='earliest'
        )
        mock_kafka_consumer.assert_called_with(
            *['test_topic'],
            bootstrap_servers='localhost:9092',
            group_id='ecommerce_consumer_group',
            auto_offset_reset='earliest',
            value_deserializer=consumer.consumer.value_deserializer,
            key_deserializer=consumer.consumer.key_deserializer,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )

        # Test with 'latest'
        consumer = EcommerceKafkaConsumer(
            topics=['test_topic'],
            auto_offset_reset='latest'
        )
        mock_kafka_consumer.assert_called_with(
            *['test_topic'],
            bootstrap_servers='localhost:9092',
            group_id='ecommerce_consumer_group',
            auto_offset_reset='latest',
            value_deserializer=consumer.consumer.value_deserializer,
            key_deserializer=consumer.consumer.key_deserializer,
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )

if __name__ == '__main__':
    unittest.main()

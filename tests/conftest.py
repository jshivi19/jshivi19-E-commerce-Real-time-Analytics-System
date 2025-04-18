"""
Test configuration and fixtures
"""
import os
import sys
import pytest
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

# Test environment settings
@pytest.fixture(autouse=True)
def test_env():
    """Setup test environment variables"""
    os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'localhost:9092'
    os.environ['POSTGRES_HOST'] = 'localhost'
    os.environ['POSTGRES_PORT'] = '5432'
    os.environ['POSTGRES_DB'] = 'ecommerce_analytics_test'
    os.environ['POSTGRES_USER'] = 'postgres'
    os.environ['POSTGRES_PASSWORD'] = 'test_password'
    os.environ['SIMULATION_INTERVAL'] = '0.1'
    os.environ['NUMBER_OF_PRODUCTS'] = '10'
    os.environ['NUMBER_OF_USERS'] = '5'
    os.environ['PROCESSING_WINDOW'] = '60'
    os.environ['SPARK_APP_NAME'] = 'E-commerce Analytics Test'
    os.environ['SPARK_MASTER'] = 'local[*]'
    
    yield
    
    # Clean up environment after tests
    env_vars = [
        'KAFKA_BOOTSTRAP_SERVERS',
        'POSTGRES_HOST',
        'POSTGRES_PORT',
        'POSTGRES_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
        'SIMULATION_INTERVAL',
        'NUMBER_OF_PRODUCTS',
        'NUMBER_OF_USERS',
        'PROCESSING_WINDOW',
        'SPARK_APP_NAME',
        'SPARK_MASTER'
    ]
    for var in env_vars:
        os.environ.pop(var, None)

# Mock Kafka fixtures
@pytest.fixture
def mock_kafka_producer(mocker):
    """Create a mock Kafka producer"""
    mock = mocker.patch('kafka.KafkaProducer')
    mock_instance = mock.return_value
    mock_instance.send.return_value.get.return_value = None
    return mock_instance

@pytest.fixture
def mock_kafka_consumer(mocker):
    """Create a mock Kafka consumer"""
    mock = mocker.patch('kafka.KafkaConsumer')
    mock_instance = mock.return_value
    mock_instance.__iter__.return_value = []
    return mock_instance

# Mock PostgreSQL fixtures
@pytest.fixture
def mock_db_connection(mocker):
    """Create a mock PostgreSQL connection"""
    mock = mocker.patch('psycopg2.connect')
    mock_instance = mock.return_value
    mock_instance.cursor.return_value.__enter__.return_value = mocker.MagicMock()
    return mock_instance

# Mock Spark fixtures
@pytest.fixture
def mock_spark_session(mocker):
    """Create a mock Spark session"""
    mock = mocker.patch('pyspark.sql.SparkSession.builder')
    mock_instance = mock.return_value
    mock_instance.appName.return_value = mock_instance
    mock_instance.master.return_value = mock_instance
    mock_instance.config.return_value = mock_instance
    mock_instance.getOrCreate.return_value = mocker.MagicMock()
    return mock_instance

# Test data fixtures
@pytest.fixture
def sample_user_event():
    """Create a sample user event"""
    return {
        'event_id': 'test-user-event',
        'timestamp': '2025-01-01T00:00:00',
        'event_type': 'user_activity',
        'user_id': 'test-user',
        'action': 'view',
        'product_id': 'test-product',
        'session_id': 'test-session'
    }

@pytest.fixture
def sample_purchase_event():
    """Create a sample purchase event"""
    return {
        'event_id': 'test-purchase-event',
        'timestamp': '2025-01-01T00:00:00',
        'event_type': 'purchase',
        'user_id': 'test-user',
        'transaction_id': 'test-transaction',
        'items': [
            {'product_id': 'p1', 'quantity': 2, 'price': 10.0},
            {'product_id': 'p2', 'quantity': 1, 'price': 20.0}
        ],
        'total_amount': 40.0,
        'payment_method': 'credit_card',
        'shipping_address': {
            'street': '123 Test St',
            'city': 'Test City',
            'state': 'TS',
            'zip': '12345'
        }
    }

@pytest.fixture
def sample_inventory_event():
    """Create a sample inventory event"""
    return {
        'event_id': 'test-inventory-event',
        'timestamp': '2025-01-01T00:00:00',
        'event_type': 'inventory',
        'product_id': 'test-product',
        'quantity': 10,
        'operation': 'add',
        'previous_quantity': 5
    }

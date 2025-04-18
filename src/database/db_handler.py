"""
PostgreSQL database handler for e-commerce analytics
"""
import logging
from typing import Dict, Any, List
import psycopg2
from psycopg2.extras import execute_batch
from config.settings import POSTGRES_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseHandler:
    """Handles all database operations for the e-commerce analytics system"""

    def __init__(self):
        """Initialize database connection and tables"""
        self.conn = None
        self.connect()
        self.create_tables()

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**POSTGRES_CONFIG)
            logger.info("Successfully connected to PostgreSQL database")
        except psycopg2.OperationalError as e:
            logger.error("Operational error while connecting to PostgreSQL: %s", str(e))
            raise
        except Exception as e:
            logger.error("Unexpected error while connecting to PostgreSQL: %s", str(e))
            raise

    def create_tables(self):
        """Create necessary database tables if they don't exist"""
        try:
            with self.conn.cursor() as cur:
                # User Events table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_events (
                        event_id VARCHAR(36) PRIMARY KEY,
                        timestamp TIMESTAMP NOT NULL,
                        user_id VARCHAR(36) NOT NULL,
                        action VARCHAR(50) NOT NULL,
                        product_id VARCHAR(36),
                        search_query TEXT,
                        session_id VARCHAR(36),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Transactions table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        transaction_id VARCHAR(36) PRIMARY KEY,
                        event_id VARCHAR(36) NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        user_id VARCHAR(36) NOT NULL,
                        total_amount DECIMAL(10,2) NOT NULL,
                        payment_method VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Transaction Items table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS transaction_items (
                        id SERIAL PRIMARY KEY,
                        transaction_id VARCHAR(36) REFERENCES transactions(transaction_id),
                        product_id VARCHAR(36) NOT NULL,
                        quantity INTEGER NOT NULL,
                        price DECIMAL(10,2) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Analytics Results table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS analytics_results (
                        id SERIAL PRIMARY KEY,
                        window_start TIMESTAMP NOT NULL,
                        window_end TIMESTAMP NOT NULL,
                        metric_name VARCHAR(100) NOT NULL,
                        metric_value DECIMAL(15,2) NOT NULL,
                        dimensions JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                self.conn.commit()
                logger.info("Successfully created database tables")
        except psycopg2.DatabaseError as e:
            logger.error("Database error while creating tables: %s", str(e))
            self.conn.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error while creating tables: %s", str(e))
            self.conn.rollback()
            raise

    def insert_user_event(self, event: Dict[str, Any]):
        """Insert a user event into the database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_events (
                        event_id, timestamp, user_id, action,
                        product_id, search_query, session_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    event['event_id'],
                    event['timestamp'],
                    event['user_id'],
                    event['action'],
                    event.get('product_id'),
                    event.get('search_query'),
                    event.get('session_id')
                ))
                self.conn.commit()
        except psycopg2.IntegrityError as e:
            logger.error("Integrity error while inserting user event: %s", str(e))
            self.conn.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error while inserting user event: %s", str(e))
            self.conn.rollback()
            raise

    def insert_transaction(self, event: Dict[str, Any]):
        """Insert a transaction and its items into the database"""
        try:
            with self.conn.cursor() as cur:
                # Insert transaction
                cur.execute("""
                    INSERT INTO transactions (
                        transaction_id, event_id, timestamp,
                        user_id, total_amount, payment_method
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING transaction_id
                """, (
                    event['transaction_id'],
                    event['event_id'],
                    event['timestamp'],
                    event['user_id'],
                    event['total_amount'],
                    event['payment_method']
                ))

                transaction_id = cur.fetchone()[0]

                # Insert transaction items
                item_data = [
                    (transaction_id, item['product_id'], item['quantity'], item['price'])
                    for item in event['items']
                ]
                execute_batch(cur, """
                    INSERT INTO transaction_items (
                        transaction_id, product_id, quantity, price
                    ) VALUES (%s, %s, %s, %s)
                """, item_data)

                self.conn.commit()
        except psycopg2.IntegrityError as e:
            logger.error("Integrity error while inserting transaction: %s", str(e))
            self.conn.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error while inserting transaction: %s", str(e))
            self.conn.rollback()
            raise

    def insert_analytics_result(self, result: Dict[str, Any]):
        """Insert an analytics result into the database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO analytics_results (
                        window_start, window_end, metric_name,
                        metric_value, dimensions
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    result['window_start'],
                    result['window_end'],
                    result['metric_name'],
                    result['metric_value'],
                    result.get('dimensions')
                ))
                self.conn.commit()
        except psycopg2.DataError as e:
            logger.error("Data error while inserting analytics result: %s", str(e))
            self.conn.rollback()
            raise
        except Exception as e:
            logger.error("Unexpected error while inserting analytics result: %s", str(e))
            self.conn.rollback()
            raise

    def batch_insert_events(self, events: List[Dict[str, Any]]):
        """Batch insert multiple events into appropriate tables"""
        user_events = []
        transactions = []
        
        for event in events:
            if event['event_type'] == 'user_activity':
                user_events.append(event)
            elif event['event_type'] == 'purchase':
                transactions.append(event)

        try:
            with self.conn.cursor() as cur:
                if user_events:
                    execute_batch(cur, """
                        INSERT INTO user_events (
                            event_id, timestamp, user_id, action,
                            product_id, search_query, session_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, [(
                        event['event_id'],
                        event['timestamp'],
                        event['user_id'],
                        event['action'],
                        event.get('product_id'),
                        event.get('search_query'),
                        event.get('session_id')
                    ) for event in user_events])

                if transactions:
                    for event in transactions:
                        self.insert_transaction(event)

                self.conn.commit()
        except Exception as e:
            logger.error("Error in batch insert: %s", str(e))
            self.conn.rollback()
            raise

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

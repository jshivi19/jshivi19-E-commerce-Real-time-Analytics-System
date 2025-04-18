"""
Unit tests for database handler
"""
import unittest
from unittest.mock import patch, MagicMock, call
import json
from datetime import datetime
from psycopg2.errors import UniqueViolation, ForeignKeyViolation
from src.database.db_handler import DatabaseHandler

class TestDatabaseHandler(unittest.TestCase):
    """Test cases for DatabaseHandler class"""

    @patch('src.database.db_handler.psycopg2')
    def setUp(self, mock_psycopg2):
        """Set up test fixtures"""
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = self.mock_conn
        self.mock_conn.cursor.return_value.__enter__.return_value = self.mock_cursor
        
        self.db_handler = DatabaseHandler()

    def test_connection_initialization(self):
        """Test database connection initialization"""
        self.assertIsNotNone(self.db_handler)
        self.assertIsNotNone(self.db_handler.conn)
        self.mock_cursor.execute.assert_called()  # Should be called during table creation

    def test_create_tables(self):
        """Test table creation"""
        self.db_handler.create_tables()
        
        # Verify all required tables are created
        execute_calls = self.mock_cursor.execute.call_args_list
        table_creations = [call for call in execute_calls if 'CREATE TABLE' in str(call)]
        
        self.assertGreaterEqual(len(table_creations), 4)  # At least 4 tables
        
        # Verify specific table creations
        tables = ['user_events', 'transactions', 'transaction_items', 'analytics_results']
        for table in tables:
            self.assertTrue(
                any(table in str(call) for call in table_creations),
                f"Table {table} creation not found"
            )

    def test_insert_user_event(self):
        """Test user event insertion"""
        event = {
            'event_id': 'test-id',
            'timestamp': datetime.now().isoformat(),
            'user_id': 'user-1',
            'action': 'view',
            'product_id': 'product-1',
            'search_query': None,
            'session_id': 'session-1'
        }

        self.db_handler.insert_user_event(event)

        # Verify insert query execution
        self.mock_cursor.execute.assert_called_once()
        self.mock_conn.commit.assert_called_once()

    def test_insert_user_event_error(self):
        """Test error handling in user event insertion"""
        self.mock_cursor.execute.side_effect = UniqueViolation("Duplicate key")

        event = {
            'event_id': 'test-id',
            'timestamp': datetime.now().isoformat(),
            'user_id': 'user-1',
            'action': 'view',
            'product_id': 'product-1'
        }

        with self.assertRaises(Exception):
            self.db_handler.insert_user_event(event)
        
        self.mock_conn.rollback.assert_called_once()

    def test_insert_transaction(self):
        """Test transaction insertion"""
        event = {
            'event_id': 'test-id',
            'transaction_id': 'trans-1',
            'timestamp': datetime.now().isoformat(),
            'user_id': 'user-1',
            'total_amount': 100.0,
            'payment_method': 'credit_card',
            'items': [
                {'product_id': 'p1', 'quantity': 2, 'price': 25.0},
                {'product_id': 'p2', 'quantity': 1, 'price': 50.0}
            ]
        }

        # Mock transaction_id return
        self.mock_cursor.fetchone.return_value = ['trans-1']

        self.db_handler.insert_transaction(event)

        # Verify transaction and items insertion
        self.assertGreaterEqual(self.mock_cursor.execute.call_count, 2)
        self.mock_conn.commit.assert_called_once()

    def test_insert_transaction_error(self):
        """Test error handling in transaction insertion"""
        self.mock_cursor.execute.side_effect = ForeignKeyViolation("Invalid foreign key")

        event = {
            'transaction_id': 'trans-1',
            'event_id': 'test-id',
            'timestamp': datetime.now().isoformat(),
            'user_id': 'user-1',
            'total_amount': 100.0,
            'payment_method': 'credit_card',
            'items': []
        }

        with self.assertRaises(Exception):
            self.db_handler.insert_transaction(event)
        
        self.mock_conn.rollback.assert_called_once()

    def test_insert_analytics_result(self):
        """Test analytics result insertion"""
        result = {
            'window_start': datetime.now(),
            'window_end': datetime.now(),
            'metric_name': 'test_metric',
            'metric_value': 42.0,
            'dimensions': {'dimension1': 'value1'}
        }

        self.db_handler.insert_analytics_result(result)

        # Verify insert query execution
        self.mock_cursor.execute.assert_called_once()
        self.mock_conn.commit.assert_called_once()

    def test_batch_insert_events(self):
        """Test batch event insertion"""
        events = [
            {
                'event_type': 'user_activity',
                'event_id': 'e1',
                'timestamp': datetime.now().isoformat(),
                'user_id': 'u1',
                'action': 'view'
            },
            {
                'event_type': 'purchase',
                'event_id': 'e2',
                'transaction_id': 't1',
                'timestamp': datetime.now().isoformat(),
                'user_id': 'u1',
                'total_amount': 50.0,
                'payment_method': 'credit_card',
                'items': []
            }
        ]

        self.db_handler.batch_insert_events(events)

        # Verify batch inserts
        self.assertGreaterEqual(self.mock_cursor.execute.call_count, 2)
        self.mock_conn.commit.assert_called()

    def test_batch_insert_error(self):
        """Test error handling in batch insertion"""
        self.mock_cursor.execute.side_effect = Exception("Database error")

        events = [
            {
                'event_type': 'user_activity',
                'event_id': 'e1',
                'timestamp': datetime.now().isoformat(),
                'user_id': 'u1',
                'action': 'view'
            }
        ]

        with self.assertRaises(Exception):
            self.db_handler.batch_insert_events(events)
        
        self.mock_conn.rollback.assert_called_once()

    def test_close_connection(self):
        """Test database connection closure"""
        self.db_handler.close()
        self.mock_conn.close.assert_called_once()

    @patch('src.database.db_handler.psycopg2')
    def test_connection_error(self, mock_psycopg2):
        """Test handling of connection errors"""
        mock_psycopg2.connect.side_effect = Exception("Connection failed")

        with self.assertRaises(Exception):
            DatabaseHandler()

if __name__ == '__main__':
    unittest.main()

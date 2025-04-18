"""
Unit tests for data generator
"""
import unittest
from unittest.mock import patch
import json
from src.data_generator.generator import EcommerceDataGenerator
from src.data_generator.events import UserEvent, PurchaseEvent, InventoryEvent

class TestEcommerceDataGenerator(unittest.TestCase):
    """Test cases for EcommerceDataGenerator class"""

    def setUp(self):
        """Set up test fixtures"""
        self.generator = EcommerceDataGenerator()

    def test_initialize_products(self):
        """Test product catalog initialization"""
        products = self.generator.products
        
        self.assertIsNotNone(products)
        self.assertTrue(len(products) > 0)
        
        # Test product structure
        sample_product = next(iter(products.values()))
        self.assertIn('name', sample_product)
        self.assertIn('category', sample_product)
        self.assertIn('price', sample_product)
        self.assertIn('inventory', sample_product)
        
        # Test product data types
        self.assertIsInstance(sample_product['name'], str)
        self.assertIsInstance(sample_product['category'], str)
        self.assertIsInstance(sample_product['price'], float)
        self.assertIsInstance(sample_product['inventory'], int)

    def test_initialize_users(self):
        """Test user profiles initialization"""
        users = self.generator.users
        
        self.assertIsNotNone(users)
        self.assertTrue(len(users) > 0)
        
        # Test user structure
        sample_user = next(iter(users.values()))
        self.assertIn('name', sample_user)
        self.assertIn('email', sample_user)
        self.assertIn('address', sample_user)
        
        # Test address structure
        address = sample_user['address']
        self.assertIn('street', address)
        self.assertIn('city', address)
        self.assertIn('state', address)
        self.assertIn('zip', address)

    def test_generate_user_event(self):
        """Test user event generation"""
        event = self.generator.generate_user_event()
        
        self.assertIsInstance(event, UserEvent)
        self.assertIsNotNone(event.event_id)
        self.assertIsNotNone(event.timestamp)
        self.assertEqual(event.event_type, 'user_activity')
        
        # Test action types
        valid_actions = {'view', 'search', 'add_to_cart', 'remove_from_cart'}
        self.assertIn(event.action, valid_actions)
        
        if event.action == 'search':
            self.assertIsNotNone(event.search_query)
            self.assertIsNone(event.product_id)
        else:
            self.assertIsNotNone(event.product_id)
            self.assertIsNone(event.search_query)

    def test_generate_purchase_event(self):
        """Test purchase event generation"""
        event = self.generator.generate_purchase_event()
        
        self.assertIsInstance(event, PurchaseEvent)
        self.assertIsNotNone(event.event_id)
        self.assertIsNotNone(event.timestamp)
        self.assertEqual(event.event_type, 'purchase')
        
        # Test items
        self.assertTrue(len(event.items) > 0)
        for item in event.items:
            self.assertIn('product_id', item)
            self.assertIn('quantity', item)
            self.assertIn('price', item)
            self.assertGreater(item['quantity'], 0)
            self.assertGreater(item['price'], 0)
        
        # Test total amount calculation
        expected_total = sum(item['quantity'] * item['price'] for item in event.items)
        self.assertAlmostEqual(event.total_amount, expected_total, places=2)

    def test_generate_inventory_event(self):
        """Test inventory event generation"""
        event = self.generator.generate_inventory_event()
        
        self.assertIsInstance(event, InventoryEvent)
        self.assertIsNotNone(event.event_id)
        self.assertIsNotNone(event.timestamp)
        self.assertEqual(event.event_type, 'inventory')
        
        # Test operation types
        valid_operations = {'add', 'remove', 'adjust'}
        self.assertIn(event.operation, valid_operations)
        
        # Test quantity constraints
        if event.operation == 'add':
            self.assertGreater(event.quantity, 0)
        elif event.operation == 'remove':
            self.assertGreaterEqual(event.quantity, 0)
        
        self.assertIsNotNone(event.previous_quantity)
        self.assertGreaterEqual(event.previous_quantity, 0)

    def test_generate_event(self):
        """Test random event generation"""
        event = self.generator.generate_event()
        
        self.assertIsInstance(event, dict)
        self.assertIn('type', event)
        self.assertIn('data', event)
        
        valid_types = {'user', 'purchase', 'inventory'}
        self.assertIn(event['type'], valid_types)
        
        # Verify JSON serialization
        json_data = json.loads(event['data'])
        self.assertIsInstance(json_data, dict)
        self.assertIn('event_id', json_data)
        self.assertIn('timestamp', json_data)
        self.assertIn('event_type', json_data)

    @patch('src.data_generator.generator.random.choice')
    def test_event_type_distribution(self, mock_choice):
        """Test event type distribution weighting"""
        # Test that the distribution list contains the correct proportions
        event = self.generator.generate_event()
        event_types = ['user'] * 10 + ['purchase'] * 3 + ['inventory'] * 1
        
        mock_choice.assert_called_with(event_types)
        
        # Count distribution in actual generation
        event_counts = {'user': 0, 'purchase': 0, 'inventory': 0}
        num_events = 1000
        
        for _ in range(num_events):
            event = self.generator.generate_event()
            event_counts[event['type']] += 1
        
        # Verify rough distribution (allowing for randomness)
        self.assertGreater(event_counts['user'], event_counts['purchase'])
        self.assertGreater(event_counts['purchase'], event_counts['inventory'])

if __name__ == '__main__':
    unittest.main()

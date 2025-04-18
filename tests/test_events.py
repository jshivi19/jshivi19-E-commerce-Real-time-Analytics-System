"""
Unit tests for event classes
"""
import unittest
from datetime import datetime
import json
from src.data_generator.events import BaseEvent, UserEvent, PurchaseEvent, InventoryEvent

class TestBaseEvent(unittest.TestCase):
    """Test cases for BaseEvent class"""

    def test_base_event_initialization(self):
        """Test base event initialization with and without parameters"""
        # Test with provided values
        event = BaseEvent(
            event_id="test-id",
            timestamp="2025-01-01T00:00:00",
            event_type="test"
        )
        self.assertEqual(event.event_id, "test-id")
        self.assertEqual(event.timestamp, "2025-01-01T00:00:00")
        self.assertEqual(event.event_type, "test")

        # Test with empty values (should generate event_id and timestamp)
        event = BaseEvent(
            event_id="",
            timestamp="",
            event_type="test"
        )
        self.assertIsNotNone(event.event_id)
        self.assertIsNotNone(event.timestamp)
        self.assertEqual(event.event_type, "test")

    def test_to_json(self):
        """Test JSON serialization of base event"""
        event = BaseEvent(
            event_id="test-id",
            timestamp="2025-01-01T00:00:00",
            event_type="test"
        )
        json_str = event.to_json()
        json_data = json.loads(json_str)
        
        self.assertEqual(json_data["event_id"], "test-id")
        self.assertEqual(json_data["timestamp"], "2025-01-01T00:00:00")
        self.assertEqual(json_data["event_type"], "test")

class TestUserEvent(unittest.TestCase):
    """Test cases for UserEvent class"""

    def test_user_event_initialization(self):
        """Test user event initialization"""
        event = UserEvent(
            event_id="test-id",
            timestamp="2025-01-01T00:00:00",
            event_type="",
            user_id="user-1",
            action="view",
            product_id="product-1"
        )
        
        self.assertEqual(event.event_id, "test-id")
        self.assertEqual(event.user_id, "user-1")
        self.assertEqual(event.action, "view")
        self.assertEqual(event.product_id, "product-1")
        self.assertEqual(event.event_type, "user_activity")

    def test_search_event(self):
        """Test user search event"""
        event = UserEvent(
            event_id="test-id",
            timestamp="2025-01-01T00:00:00",
            event_type="",
            user_id="user-1",
            action="search",
            search_query="test query"
        )
        
        self.assertEqual(event.action, "search")
        self.assertEqual(event.search_query, "test query")
        self.assertIsNone(event.product_id)

class TestPurchaseEvent(unittest.TestCase):
    """Test cases for PurchaseEvent class"""

    def test_purchase_event_initialization(self):
        """Test purchase event initialization"""
        items = [
            {"product_id": "p1", "quantity": 2, "price": 10.0},
            {"product_id": "p2", "quantity": 1, "price": 20.0}
        ]
        
        event = PurchaseEvent(
            event_id="test-id",
            timestamp="2025-01-01T00:00:00",
            event_type="",
            user_id="user-1",
            transaction_id="",
            items=items,
            total_amount=40.0,
            payment_method="credit_card",
            shipping_address={"street": "123 Test St", "city": "Test City"}
        )
        
        self.assertEqual(event.event_type, "purchase")
        self.assertIsNotNone(event.transaction_id)
        self.assertEqual(event.total_amount, 40.0)
        self.assertEqual(len(event.items), 2)
        self.assertEqual(event.payment_method, "credit_card")

class TestInventoryEvent(unittest.TestCase):
    """Test cases for InventoryEvent class"""

    def test_inventory_event_initialization(self):
        """Test inventory event initialization"""
        event = InventoryEvent(
            event_id="test-id",
            timestamp="2025-01-01T00:00:00",
            event_type="",
            product_id="product-1",
            quantity=10,
            operation="add",
            previous_quantity=5
        )
        
        self.assertEqual(event.event_type, "inventory")
        self.assertEqual(event.product_id, "product-1")
        self.assertEqual(event.quantity, 10)
        self.assertEqual(event.operation, "add")
        self.assertEqual(event.previous_quantity, 5)

    def test_inventory_operations(self):
        """Test different inventory operations"""
        operations = ["add", "remove", "adjust"]
        
        for op in operations:
            event = InventoryEvent(
                event_id="test-id",
                timestamp="2025-01-01T00:00:00",
                event_type="",
                product_id="product-1",
                quantity=10,
                operation=op
            )
            self.assertEqual(event.operation, op)

if __name__ == '__main__':
    unittest.main()

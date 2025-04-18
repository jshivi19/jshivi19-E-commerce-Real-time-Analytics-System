"""
Data generator for simulating e-commerce events
"""
import random
from typing import Dict, List, Any
from faker import Faker
from datetime import datetime, timedelta
import time
from .events import UserEvent, PurchaseEvent, InventoryEvent
from config.settings import (
    NUMBER_OF_PRODUCTS,
    NUMBER_OF_USERS,
    SIMULATION_INTERVAL
)

class EcommerceDataGenerator:
    """Generates simulated e-commerce events"""
    
    def __init__(self):
        self.fake = Faker()
        self.products = self._initialize_products()
        self.users = self._initialize_users()
        self.active_sessions: Dict[str, Dict[str, Any]] = {}

    def _initialize_products(self) -> Dict[str, Dict[str, Any]]:
        """Initialize product catalog"""
        products = {}
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
        
        for _ in range(NUMBER_OF_PRODUCTS):
            product_id = str(self.fake.uuid4())
            products[product_id] = {
                'name': self.fake.product_name(),
                'category': random.choice(categories),
                'price': round(random.uniform(10.0, 1000.0), 2),
                'inventory': random.randint(0, 100)
            }
        return products

    def _initialize_users(self) -> Dict[str, Dict[str, Any]]:
        """Initialize user profiles"""
        users = {}
        
        for _ in range(NUMBER_OF_USERS):
            user_id = str(self.fake.uuid4())
            users[user_id] = {
                'name': self.fake.name(),
                'email': self.fake.email(),
                'address': {
                    'street': self.fake.street_address(),
                    'city': self.fake.city(),
                    'state': self.fake.state(),
                    'zip': self.fake.zipcode()
                }
            }
        return users

    def generate_user_event(self) -> UserEvent:
        """Generate a random user activity event"""
        user_id = random.choice(list(self.users.keys()))
        actions = ['view', 'search', 'add_to_cart', 'remove_from_cart']
        action = random.choice(actions)
        
        if action == 'search':
            return UserEvent(
                event_id='',
                timestamp='',
                event_type='',
                user_id=user_id,
                action=action,
                search_query=self.fake.word()
            )
        else:
            product_id = random.choice(list(self.products.keys()))
            return UserEvent(
                event_id='',
                timestamp='',
                event_type='',
                user_id=user_id,
                action=action,
                product_id=product_id,
                session_id=str(self.fake.uuid4())
            )

    def generate_purchase_event(self) -> PurchaseEvent:
        """Generate a random purchase event"""
        user_id = random.choice(list(self.users.keys()))
        num_items = random.randint(1, 5)
        items = []
        total_amount = 0.0

        for _ in range(num_items):
            product_id = random.choice(list(self.products.keys()))
            quantity = random.randint(1, 3)
            price = self.products[product_id]['price']
            items.append({
                'product_id': product_id,
                'quantity': quantity,
                'price': price
            })
            total_amount += price * quantity

        return PurchaseEvent(
            event_id='',
            timestamp='',
            event_type='',
            user_id=user_id,
            transaction_id='',
            items=items,
            total_amount=round(total_amount, 2),
            payment_method=random.choice(['credit_card', 'debit_card', 'paypal']),
            shipping_address=self.users[user_id]['address']
        )

    def generate_inventory_event(self) -> InventoryEvent:
        """Generate a random inventory update event"""
        product_id = random.choice(list(self.products.keys()))
        operation = random.choice(['add', 'remove', 'adjust'])
        prev_quantity = self.products[product_id]['inventory']
        
        if operation == 'add':
            quantity = random.randint(1, 50)
            self.products[product_id]['inventory'] += quantity
        elif operation == 'remove':
            max_remove = min(prev_quantity, 10)
            quantity = random.randint(1, max_remove) if max_remove > 0 else 0
            self.products[product_id]['inventory'] -= quantity
        else:  # adjust
            quantity = random.randint(0, 100)
            self.products[product_id]['inventory'] = quantity

        return InventoryEvent(
            event_id='',
            timestamp='',
            event_type='',
            product_id=product_id,
            quantity=quantity,
            operation=operation,
            previous_quantity=prev_quantity
        )

    def generate_event(self) -> Dict[str, Any]:
        """Generate a random event of any type"""
        event_types = ['user'] * 10 + ['purchase'] * 3 + ['inventory'] * 1
        event_type = random.choice(event_types)
        
        if event_type == 'user':
            event = self.generate_user_event()
        elif event_type == 'purchase':
            event = self.generate_purchase_event()
        else:
            event = self.generate_inventory_event()
            
        return {'type': event_type, 'data': event.to_json()}

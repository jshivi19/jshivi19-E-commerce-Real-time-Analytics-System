"""
Event classes for e-commerce data simulation
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any, List, Optional
import json
import uuid

@dataclass
class BaseEvent:
    """Base class for all events"""
    event_id: str
    timestamp: str
    event_type: str

    def __post_init__(self):
        if not self.event_id:
            self.event_id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()

    def to_json(self) -> str:
        """Convert event to JSON string"""
        return json.dumps(asdict(self))

@dataclass
class UserEvent(BaseEvent):
    """User activity events"""
    user_id: str
    action: str  # view, search, add_to_cart, remove_from_cart
    product_id: Optional[str] = None
    search_query: Optional[str] = None
    session_id: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        if not self.event_type:
            self.event_type = 'user_activity'

@dataclass
class PurchaseEvent(BaseEvent):
    """Purchase transaction events"""
    user_id: str
    transaction_id: str
    items: List[Dict[str, Any]]  # List of {product_id, quantity, price}
    total_amount: float
    payment_method: str
    shipping_address: Dict[str, str]

    def __post_init__(self):
        super().__post_init__()
        if not self.event_type:
            self.event_type = 'purchase'
        if not self.transaction_id:
            self.transaction_id = str(uuid.uuid4())

@dataclass
class InventoryEvent(BaseEvent):
    """Inventory update events"""
    product_id: str
    quantity: int
    operation: str  # add, remove, adjust
    reason: Optional[str] = None
    previous_quantity: Optional[int] = None

    def __post_init__(self):
        super().__post_init__()
        if not self.event_type:
            self.event_type = 'inventory'

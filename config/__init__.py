"""
Configuration package initialization
"""
from .settings import (
    NUMBER_OF_PRODUCTS,
    NUMBER_OF_USERS,
    SIMULATION_INTERVAL,
    POSTGRES_CONFIG,
    settings
)

__all__ = [
    'NUMBER_OF_PRODUCTS',
    'NUMBER_OF_USERS',
    'SIMULATION_INTERVAL',
    'POSTGRES_CONFIG',
    'settings'
]

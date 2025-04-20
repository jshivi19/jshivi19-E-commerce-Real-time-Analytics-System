"""
Configuration settings for the E-commerce Analytics System
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

import pydantic
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(..., env="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topics: dict = {
        'user_activity': 'user_activity',
        'purchase_events': 'purchase_events',
        'inventory_updates': 'inventory_updates',
        'tweets_topic': 'tweets_topic',
        'user_data_topic': 'user_data_topic'
    }

    # Spark Configuration
    spark_app_name: str = "E-commerce Analytics"
    spark_master: str = "local[*]"
    spark_batch_duration: int = 15  # seconds

    # PostgreSQL Configuration
    postgres_host: str = Field(..., env="POSTGRES_HOST")
    postgres_port: int = Field(..., env="POSTGRES_PORT")
    postgres_database: str = Field(..., env="POSTGRES_DB")
    postgres_user: str = Field(..., env="POSTGRES_USER")
    postgres_password: str = Field(..., env="POSTGRES_PASSWORD")

    # Data Generator Configuration
    simulation_interval: float = 1.0  # seconds between events
    number_of_products: int = 1000
    number_of_users: int = 500

    # Window Configuration
    processing_window: int = 900  # 15 minutes in seconds

    class Config:
        env_file = ".env"

settings = Settings()

# PostgreSQL Configuration Dictionary for database.py
POSTGRES_CONFIG = {
    'host': settings.postgres_host,
    'port': settings.postgres_port,
    'database': settings.postgres_database,
    'user': settings.postgres_user,
    'password': settings.postgres_password
}

# Export settings for data generator
NUMBER_OF_PRODUCTS = settings.number_of_products
NUMBER_OF_USERS = settings.number_of_users
SIMULATION_INTERVAL = settings.simulation_interval

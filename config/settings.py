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
        'inventory_updates': 'inventory_updates'
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

"""
Script to initialize PostgreSQL database for the E-commerce Analytics System
"""
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config.settings import POSTGRES_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_database():
    """Create the database if it doesn't exist"""
    db_name = POSTGRES_CONFIG['database']
    
    # Connect to PostgreSQL server
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG['host'],
        port=POSTGRES_CONFIG['port'],
        user=POSTGRES_CONFIG['user'],
        password=POSTGRES_CONFIG['password'],
        database='postgres'  # Connect to default database first
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    try:
        with conn.cursor() as cur:
            # Check if database exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cur.fetchone()
            
            if not exists:
                cur.execute(f'CREATE DATABASE {db_name}')
                logger.info(f"Database '{db_name}' created successfully")
            else:
                logger.info(f"Database '{db_name}' already exists")
    
    finally:
        conn.close()

def create_extensions():
    """Create necessary PostgreSQL extensions"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            # Create extensions
            cur.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            cur.execute('CREATE EXTENSION IF NOT EXISTS "btree_gist"')
            conn.commit()
            logger.info("PostgreSQL extensions created successfully")
    
    finally:
        conn.close()

def create_tables():
    """Create database tables"""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        with conn.cursor() as cur:
            # User Events table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_events (
                    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    timestamp TIMESTAMP NOT NULL,
                    user_id UUID NOT NULL,
                    action VARCHAR(50) NOT NULL,
                    product_id UUID,
                    search_query TEXT,
                    session_id UUID,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT user_events_action_check 
                        CHECK (action IN ('view', 'search', 'add_to_cart', 'remove_from_cart'))
                )
            """)

            # Create index on user_events
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_events_timestamp 
                ON user_events USING BRIN (timestamp)
            """)

            # Transactions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    event_id UUID NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    user_id UUID NOT NULL,
                    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
                    payment_method VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create index on transactions
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_transactions_timestamp 
                ON transactions USING BRIN (timestamp)
            """)

            # Transaction Items table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transaction_items (
                    id SERIAL PRIMARY KEY,
                    transaction_id UUID REFERENCES transactions(transaction_id),
                    product_id UUID NOT NULL,
                    quantity INTEGER NOT NULL CHECK (quantity > 0),
                    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT analytics_window_check 
                        CHECK (window_end > window_start),
                    CONSTRAINT analytics_results_unique 
                        UNIQUE (window_start, window_end, metric_name, dimensions)
                )
            """)

            # Create index on analytics_results
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_analytics_results_window 
                ON analytics_results USING GIST (
                    tstzrange(window_start, window_end, '[]')
                )
            """)

            conn.commit()
            logger.info("Database tables created successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    """Main entry point"""
    try:
        logger.info("Starting database setup...")
        create_database()
        create_extensions()
        create_tables()
        logger.info("Database setup completed successfully")
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()

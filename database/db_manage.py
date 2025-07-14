# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()

def start_database() -> tuple:
    """
    Creates both a psycopg2 connection/cursor and an SQLAlchemy engine.
    
    Returns:
        tuple: (db_connection, cursor, sqlalchemy_engine)
    """
    # Create PostgreSQL connection
    db = psycopg2.connect(
        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT'),
        user=os.getenv('PG_USER'),
        database=os.getenv('PG_DATABASE'),
        password=os.getenv('PG_PASSWORD')
    )
    
    cursor = db.cursor()
    
    return db, cursor

def close_database(db: psycopg2.extensions.connection):
    """Closing the database connection"""
    db.close()
     
if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    db, cursor = start_database()
    close_database(db)
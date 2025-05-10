# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def start_database():
    """Starting up the mySQL database so it can be used by the rest of the program"""
    db = psycopg2.connect(
        host = "ballast.proxy.rlwy.net",
        port = 56208,
        user = "postgres",
        password = POSTGRES_PASSWORD,
        database = "railway"
    )
    
    cursor = db.cursor()
    
    return db, cursor

def close_database(db: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor):
    """Closing the database connection"""
    db.close()
    cursor.close()
     
if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    db, cursor = start_database()
    close_database(db, cursor)
    
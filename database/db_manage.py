# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sqlite3

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()

def start_database():
    db = sqlite3.connect('BeneluxCS.db')
    cursor = db.cursor()
    
    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")
    return db, cursor

def close_database(db: sqlite3.Connection, cursor: sqlite3.Cursor):
    """Closing the database connection"""
    db.close()
     
if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    db, cursor = start_database()
    close_database(db, cursor)
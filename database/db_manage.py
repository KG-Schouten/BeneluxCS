# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2

from functions import load_api_keys

def start_database():
    """Starting up the mySQL database so it can be used by the rest of the program"""

    api_keys = load_api_keys()
    ## Load password for MySQL database
    postgres_password = api_keys["POSTGRES_PASSWORD"]
    
    db = psycopg2.connect(
        host = "nozomi.proxy.rlwy.net",
        port = 20571,
        user = "postgres",
        password = postgres_password,
        database = "railway"
    )
    
    cursor = db.cursor()
    
    return db, cursor

def close_database(db, cursor):
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
    
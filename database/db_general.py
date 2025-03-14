import mysql.connector
import json
import re
import datetime
import pandas as pd
import numpy as np
from typing import *

from functions import load_api_keys
from data_processing import process_hub_data

from database.db_down import *
from database.db_up import upload_hub_data, calculate_hltv
from database.db_manage import start_database, close_database

# Constants
db_name = "BeneluxCS"

def reset_database():
    """Completely resetting the database from scratch (resetting all tables)"""
    try:
        # Delete all tables
        delete_tables()

        # Create the tables again
        create_tables()

        # Append all match data of the hub back to the database
        batch_data_match, batch_data_player, batch_data_team = process_hub_data(return_items='ALL')
        upload_hub_data(batch_data_match, batch_data_player, batch_data_team)

        # Add the HLTV ratings for all of the matches
        calculate_hltv()

    except Exception as e:
        print(f"Error while resetting database: {e}")

def delete_tables():
    """Completely resetting the database from scratch (resetting all tables)"""
    db, cursor = start_database()

    try:
        # Disable foreign key checks temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Retrieve all table names in the database
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();")
        tables = cursor.fetchall()

        # Loop through and drop each table
        for table in tables:
            table_name = table[0]
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            print(f"Dropped table: {table_name}")

    finally:
        # Enable the foreign key checks again
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        db.commit()

        close_database(db, cursor)

def create_table_query(table_name: str, data: dict, primary_keys=[], foreign_keys=[]) -> str:
    """
    Creating the queries used for creating the tables based on the keys in the dictionary

    Args:
        table_name (str)    : Name of the table.
        data (dict)         : Single level dictionary with keys-values.
        primary_keys (list) : List of primary keys (creates tuple of keys if multiple).
        foreign_keys (list) : List of tuples -> [(key, reference_table), ...]. *Make sure foreign keys have same order as primary keys
        
    Returns:
        table_query (str)   : String of the table query to use in the cursor.execute command
    """

    ## Creating the columns list
    columns = list(data.keys()) # Add keys from the dictionary to the list

    ## Creating the column_types list
    column_types = []
    for col in columns:
        sample_value = data.get(col, None) # Get a sample value
        if isinstance(sample_value, float): # Check if it is a float
            column_type = "FLOAT"
        elif isinstance(sample_value, int): # Check if it is an integer
            column_type = "INT"
        else:
            column_type = "VARCHAR(255)" # Default to VARCHAR
        column_types.append(f"`{col}` {column_type}")

    table_keys = []
    
    if primary_keys:
        primary_string = f"PRIMARY KEY ({",".join(primary_keys)})"
        table_keys.append(primary_string)
    
    if foreign_keys:
        # Sort dict based on called table
        foreign_key_dict = {}
        for key, table in foreign_keys:
            if table not in foreign_key_dict:
                foreign_key_dict[table] = []
            foreign_key_dict[table].append(key)
        
        # Create the strings
        for table in foreign_key_dict:
            foreign_string = f"FOREIGN KEY ({", ".join(foreign_key_dict[table])}) REFERENCES {table}({", ".join(foreign_key_dict[table])})"
            table_keys.append(foreign_string)

    # Create the table query
    table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join(column_types)}, {", ".join(table_keys)}
    );
    """

    return table_query

def create_tables():
    """Creating the tables in the MySQL database"""

    db, cursor = start_database()
    
    batch_data_match, batch_data_player, batch_data_team = process_hub_data(return_items=1)

    try:
        ## Create the queries    
        sql_matches = create_table_query("matches", batch_data_match[0], ["match_id"])
        sql_teams = create_table_query("teams", batch_data_team[0], ["match_id", "team_id"], [("match_id", "matches")])
        sql_player_statistics = create_table_query("player_statistics", batch_data_player[0], ["player_id", "match_id"], [("match_id", "teams"), ("team_id", "teams"), ("match_id", "matches")])

        # Create the tables
        cursor.execute(sql_matches)
        cursor.execute(sql_teams)
        cursor.execute(sql_player_statistics)

    finally:
        db.commit()
        close_database(db,cursor)


if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

    reset_database()





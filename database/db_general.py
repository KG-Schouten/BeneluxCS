# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from typing import *

from data_processing import process_hub_data, process_esea_data, gather_team_ids_json

from database.db_down import *
from database.db_up import calculate_hltv, upload_data
from database.db_manage import start_database, close_database
from database.db_config import table_names_esea, table_names_hub


def reset_database():
    """Completely resetting the database from scratch (resetting all tables)"""
    global table_names_esea, table_names_hub
    try:
        confirm = input("Are you sure you want to reset the database? y/n")
        if confirm.lower() == 'y': # Reset database
            # Delete all tables
            delete_tables()

            # Create the tables again
            create_tables(table_names_esea, table_names_hub)

            # Append all of the esea data back to the database
            team_ids = gather_team_ids_json()
            esea_data = process_esea_data(team_ids, "ALL")
            upload_data(table_names_esea, esea_data)
            calculate_hltv("esea_player_stats", "esea_maps")
            
            # Append all match data of the hub back to the database
            batch_data = process_hub_data(return_items='ALL')
            upload_data(table_names_hub, batch_data)
            calculate_hltv("hub_player_stats", "hub_maps")
        else:
            print("Database reset aborted")
    except Exception as e:
        print(f"Error while resetting database: {e}")

def delete_tables():
    """Completely resetting the database from scratch (resetting all tables)"""
    print(
    """
    
    ---------------------------------------
            Deleting the tables:
    ---------------------------------------
    
    """
    )
    
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
            if sample_value > 2147483647:  # Max INT limit (32-bit)
                column_type = "BIGINT"
            else:
                column_type = "INT"
        elif isinstance(sample_value, str) and len(sample_value) == 36 and '-' in sample_value: # Check if UUID (string format with hyphens)
            column_type = "VARCHAR(50)"
        elif isinstance(sample_value, str) and sample_value.isdigit():
            column_type = "BIGINT"
        else:
            column_type = "VARCHAR(255)" # Default to VARCHAR
        column_types.append(f"`{col}` {column_type}")

    table_keys = []
    
    if primary_keys:
        primary_string = f"PRIMARY KEY ({",".join(primary_keys)})"
        table_keys.append(primary_string)
    
    if foreign_keys:
        for foreign_key in foreign_keys:
            col_name = foreign_key[1]
            if (isinstance(foreign_key[0], tuple) or isinstance(foreign_key[0], list)): # composite foreign key
                foreign_key_columns = ', '.join(foreign_key[0])
                foreign_string = f"FOREIGN KEY ({foreign_key_columns}) REFERENCES {col_name}({foreign_key_columns})"
            else:
                foreign_string = f"FOREIGN KEY ({foreign_key[0]}) REFERENCES {col_name}({foreign_key[0]})"
            table_keys.append(foreign_string)

    # Create the table query
    table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join(column_types)}, {", ".join(table_keys)}
    );
    """

    return table_query

def create_tables(table_names_esea, table_names_hub):
    """
    Creating the tables in the MySQL database
    
    Args:
        table_names_esea (list): List of table name strings for the esea data processing return
        table_names_hub (list): List of table name string for the hub data processing return
    """
    print(
    """
        
    ---------------------------------------
            Creating the tables:
    ---------------------------------------
        
    """
    )
    
    # Starting the database
    db, cursor = start_database()
    
    # Gather data for a single tean or hub match
    team_ids = gather_team_ids_json()
    esea_data = process_esea_data(team_ids, teams_to_return="SINGLE")
    hub_data = process_hub_data(return_items=1)

    try:
        ## Creating ESEA Tables 
        for (table_name, args), data in zip(table_names_esea.items(), esea_data):
            print(f'Creating table for: {table_name}')
            
            sql_queries_esea = create_table_query(table_name, data[0], *args) # Creating the query
            
            cursor.execute(sql_queries_esea) # Creating the table
        
        ## Creating HUB Tables
        for (table_name, args), data in zip(table_names_hub.items(), hub_data):
            print(f'Creating table for: {table_name}')
            
            sql_queries_hub = create_table_query(table_name, data[0], *args) # Creating the query
            
            cursor.execute(sql_queries_hub) # Creating the table
            
    finally:
        db.commit()
        close_database(db,cursor)


if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    





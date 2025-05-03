# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
import psycopg2

from database.db_down import *
from database.db_up import *
from database.db_manage import start_database, close_database
from database.db_config import *

from data_processing.dp_events import process_esea_teams_data

def delete_table(table_names: str | list, confirm: bool = True):
    """
    Removing the table(s) from the database
    
    Args:
        table_names (str | list): Name of the table(s) to be removed:
            - str: Name of the table to be removed
            - list: List of table names to be removed
            - ALL: All tables will be removed
        confirm (bool): Whether to ask for confirmation for deletion of the tables. Default is True (ask confirmation).
    """
    print(
    """
    
    ---------------------------------------
            Deleting the tables:
    ---------------------------------------
    """
    )
    if isinstance(table_names, str) or isinstance(table_names, list):
        if confirm:
            confirm = input("Are you sure you want to delete the tables in the database? y/n")
            if confirm.lower() == 'y': # Reset database
                pass
            else:
                print("Database table deletion aborted")
                return
            
        try:
            db, cursor = start_database()
            # Disable foreign key checks temporarily
            cursor.execute("SET session_replication_role = 'replica';")
            
            # Retrieve all table names
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = [table[0] for table in cursor.fetchall()]

            if not tables:
                print("No tables found in the database.")
                return
            
            if table_names == "ALL": # If "ALL" is specified, drop all tables
                tables_to_drop = tables
            elif isinstance(table_names, str): # If a single table name is specified, drop that table
                tables_to_drop = [table_names]
            elif isinstance(table_names, list): # If a list of table names is specified, drop those tables
                tables_to_drop = table_names
            else:
                print("Invalid table name(s) specified.")
                return

            for table in tables_to_drop:
                print(f"Dropping table: {table}")
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                except psycopg2.Error as e:
                    print(f"Error dropping table {table}: {e}")
                    db.rollback() # Rollback the failed command, continue

        finally:
            # Enable the foreign key checks again
            cursor.execute("SET session_replication_role = 'origin'")
            db.commit()
            close_database(db, cursor)

def create_tables():
    
    ## Gather esea data for one team in one season
    season_number = 52
    results = asyncio.run(process_esea_teams_data(season_number=season_number, team_id="6bb0008b-e18f-4723-a265-cc6f8538efe4")) # takes the data from myth in intermediate season 52
    if results:
        df_seasons, df_events, df_teams_benelux, df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = results
    else:
        print("No data found for the specified team and season.")
        return
    
    # Create the table_columns dictionary for each dataframe
    for table_name in table_names.keys():
        df = eval(f"df_{table_name}")
        if df is None:
            print(f"Dataframe {table_name} is None. Skipping...")
            continue
        table_columns = create_table_columns(df)
        primary_keys = table_names[table_name][0]
        if len(table_names[table_name]) > 1:
            foreign_keys = table_names[table_name][1]
        else:
            foreign_keys = []
        
        # Create the table in the database
        create_table(table_name=table_name, table_columns=table_columns, primary_keys=primary_keys, foreign_keys=foreign_keys)
    
def create_table_columns(df: pd.DataFrame) -> dict:
    """
    Creating the table_columns dictionary used in the create_table function for a specific dataframe
    """
    
    # List of column names
    column_names = df.columns.tolist()
    
    # List of column values (first row expect if value is None, use the first non-null value)
    # Get largest or longest value for each column
    column_values = []
    for column_name in column_names:
        if isinstance(df[column_name].iloc[0], str):
            # Get longest string in the column
            column_values.append(df[column_name].loc[df[column_name].str.len().idxmax()])
            
        elif isinstance(df[column_name].iloc[0], (int, float)):
            # Get largest number in the column
            column_values.append(df[column_name].loc[df[column_name].idxmax()])
        else:
            # Get first non-null value in the column
            column_values.append(df[column_name].loc[df[column_name].first_valid_index()])
    
    # List of column types
    column_types = [determine_column_type(column_value) for column_value in column_values] 
    
    table_columns = {
        column_name: column_type for column_name, column_type in zip(column_names, column_types)
    }
    
    return table_columns
    
def create_table(table_name: str, table_columns: dict = {}, primary_keys: list = [], foreign_keys: list = []):
    """
    Creating the tables in the Postgresql database
    
    Args:
        table_name (str): Name of the table to be created:
        table_columns (dict): Dictionary of columns to be created containing column_name : column_type.
        primary_keys (list) : List of primary keys (creates tuple of keys if multiple).
        foreign_keys (list) : List of tuples -> [(key, reference_table), ...]. *Make sure foreign keys have same order as primary keys
    """
    print(f"""
        
    ------------------------------------------------
        Creating table: {table_name}:
    -------------------------------------------------
    """)
    try:
        # Check for correct type of table_name
        if not isinstance(table_name, str):
            print("Invalid type for table_name. Must be str or list.")
            return

        # Check for correct type of table_name
        if not isinstance(table_name, str):
            print("Invalid type for table_name. Must be str or list.")
            return
        
        # Create query for the table creation
        query = create_table_query(table_name, table_columns, primary_keys, foreign_keys)

        db, cursor = start_database()
        cursor.execute(query)
    except Exception as e:
        print(f"Error while creating table {table_name}: {e}")
        return
    finally:
        db.commit()
        close_database(db, cursor)

def create_table_query(table_name: str, table_columns: dict = {}, primary_keys: list = [], foreign_keys: list = []) -> str:
    """
    Creating the queries used for creating the tables based on the keys in the dictionary

    Args:
        table_name (str)    : Name of the table.
        table_columns (dict): Dictionary of columns to be created containing column_name : column_type (or example value).
        primary_keys (list) : List of primary keys (creates tuple of keys if multiple).
        foreign_keys (list) : List of tuples -> [(key, reference_table), ...]. *Make sure foreign keys have same order as primary keys
        
    Returns:
        table_query (str)   : String of the table query to use in the cursor.execute command
    """
    
    # Check if table_columns dict is valid (name is string, and type is a correct type for postgresQL. If not, run through check_column_type() function)
    if not isinstance(table_columns, dict):
        print("Invalid type for table_columns. Must be dict.")
        return
    if not all(isinstance(col, str) for col in table_columns.keys()):
        print("Invalid type for table_columns. All keys must be str.")
        return
    if not isinstance(primary_keys, list or tuple):
        print("Invalid type for primary_keys. Must be list or tuple.")
        return
    if not isinstance(foreign_keys, list or tuple):
        print("Invalid type for foreign_keys. Must be list or tuple.")
        return

    # Create the table column types list
    column_definitions = []
    for col_name, col in table_columns.items():
        base_type = col.split("(")[0].upper()
        if base_type not in VALID_PG_TYPES:
            column_type = determine_column_type(col)
            table_columns[col_name] = column_type
        else:
            column_type = col
        column_definitions.append(f"{col_name} {column_type}")     

    # Create the table column keys list # ADD SOME TYPE CHECKS HERE
    table_keys = []
    if primary_keys:
        if all(isinstance(key, str) for key in primary_keys): # Check if all keys are strings
            primary_string = f"PRIMARY KEY ({",".join(primary_keys)})"
            table_keys.append(primary_string)
        else:
            print("Invalid type for primary_keys. All keys must be str.")
            return
    if foreign_keys:
        for foreign_key in foreign_keys:
            if all(isinstance(key, str) for key in foreign_key): # Check if all keys are strings
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
        {", ".join(column_definitions + table_keys)}
    );
    """
    # print(table_query) # Uncomment to see the query
    return table_query

def determine_column_type(column_value) -> str:
    """ Determines the column type based on the input example value"""
    if isinstance(column_value, float):
        return "FLOAT"
    elif isinstance(column_value, int):
        if column_value > 2147483647:
            return "BIGINT"
        else:
            return "INT"
    elif isinstance(column_value, str) and len(column_value) == 36 and '-' in column_value:
        return "VARCHAR(100)"
    elif isinstance(column_value, str) and column_value.isdigit():
        return "BIGINT"
    elif isinstance(column_value, str) and len(column_value) > 255:
        return "TEXT"
    elif isinstance(column_value, list or tuple):
        return "TEXT[]"
    else:
        return "VARCHAR(255)" # Default to VARCHAR

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    delete_table("ALL", confirm=True) # Delete all tables in the database
    create_tables()
    # update_esea_data(update_type="ALL")



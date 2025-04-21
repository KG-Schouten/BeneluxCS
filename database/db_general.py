# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.db_down import *
from database.db_up import *
from database.db_manage import start_database, close_database
from database.db_config import table_names_esea, table_names_hub, VALID_PG_TYPES

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
            
            # Retrieve all table names in the database
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = cursor.fetchall()

            if tables == []: # Check if there are tables in the database
                print("No tables found in the database.")
                return
            
            if table_names == "ALL": # If "ALL" is specified, drop all tables
                tables_to_drop = [table[0] for table in tables]
            elif isinstance(table_names, str): # If a single table name is specified, drop that table
                tables_to_drop = [table_names]
            elif isinstance(table_names, list): # If a list of table names is specified, drop those tables
                tables_to_drop = table_names
            else:
                print("Invalid table name(s) specified.")
                return

            for table in tables_to_drop:
                # Check if the table exists in the database
                if table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table};")
                    print(f"Dropped table: {table}")
                else:
                    print(f"Table {table} does not exist in the database. Continuing...")

        finally:
            # Enable the foreign key checks again
            cursor.execute("SET session_replication_role = 'origin'")
            db.commit()
            close_database(db, cursor)

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
        print(f"Error while starting database: {e}")
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
    column_types = []
    for col_name, col in table_columns.items():
        base_type = col.split("(")[0].upper()
        if base_type not in VALID_PG_TYPES:
            column_type = determine_column_type(col)
            table_columns[col_name] = column_type
        else:
            column_type = col
        column_types.append(column_type)     

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
        {", ".join(column_types)}, {", ".join(table_keys)}
    );
    """
    # print(table_query) # Uncomment to see the query
    return table_query

def determine_column_type(column_type) -> str:
    """ Determines the column type based on the input example value"""
    if isinstance(column_type, float):
        return "FLOAT"
    elif isinstance(column_type, int):
        if column_type > 2147483647:
            return "BIGINT"
        else:
            return "INT"
    elif isinstance(column_type, str) and len(column_type) == 36 and '-' in column_type:
        return "VARCHAR(50)"
    elif isinstance(column_type, str) and column_type.isdigit():
        return "BIGINT"
    else:
        return "VARCHAR(255)" # Default to VARCHAR

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    delete_table()



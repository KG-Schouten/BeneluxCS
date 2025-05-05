# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import datetime
import pandas as pd
import asyncio

from database.db_manage import start_database, close_database
from database.db_down import *
from database.db_config import *

from data_processing.dp_events import process_esea_teams_data, process_hub_data, process_championship_data

# Mapping dictionary base
event_type_mapping = {
    "esea": process_esea_teams_data,
    "hub": process_hub_data,
    "championship": process_championship_data,
    "championship_hub": process_championship_data
}

### -----------------------------------------------------------------
### Updating the database
### -----------------------------------------------------------------

def update_data(update_type: str, event_type: str, **kwargs) -> None:
    validate_update_inputs(update_type, event_type, kwargs)
    event_id = kwargs.get("event_id", None)

    last_match_time = None
    if update_type == "new":
        df_existing = gather_update_matches(event_type, event_id)
        last_match_time = gather_last_match_time(df_existing)
    
    try:
        results = asyncio.run(
            gather_mapping_args(update_type, event_type, event_id, last_match_time)
        )
    except Exception as e:
        print(f"Error while gathering results: {e}")
        return

    if not results:
        print("No results found. Skipping...")
        return
    
    for df in results:
        if not isinstance(df, pd.DataFrame):
            print(f"Error: {df} is not a DataFrame. Skipping...")
            continue
        if df is None:
            print("Dataframe is None. Skipping...")
            continue
        if not hasattr(df, 'name') or df.name is None:
            print("DataFrame does not have a name. Skipping...")
            continue
        
        upload_data(df.name, df)

def validate_update_inputs(update_type: str, event_type: str, kwargs: dict) -> None:
    valid_update_types = ["all", "new", "single"]
    valid_event_types = ["esea", "hub", "championship", "championship_hub"]

    if update_type not in valid_update_types:
        raise ValueError(f"Invalid update_type. Use one of {valid_update_types}.")
    
    if event_type not in valid_event_types:
        raise ValueError(f"Invalid event_type. Use one of {valid_event_types}.")
    
    if event_type in ["hub", "championship", "championship_hub"] and "event_id" not in kwargs:
        raise ValueError("event_id is required for event_type 'hub', 'championship' or 'championship_hub'.")

def gather_mapping_args(update_type: str, event_type: str, event_id: str, last_match_time: int) -> list[pd.DataFrame]:
    args = {}
    if event_type == "esea":
        if update_type == "all":
            args['season_number'] = "ALL"
        elif update_type == "single":
            args.update({"season_number": 52, "match_amount": 1})
        elif update_type == "new":
            args.update({"season_number": "ALL", "from_timestamp": last_match_time})
    elif event_type in ["hub", "championship", "championship_hub"]:
        args.update({
                "hub_id" if event_type == "hub" else "championship_id": event_id,
                "items_to_return": "ALL" if update_type == "all" else 1,
            })
        if update_type == "new":
            args["from_timestamp"] = last_match_time
        if event_type in ["championship", "championship_hub"]:
            args["event_type"] = event_type
    
    return event_type_mapping[event_type](**args)

def gather_last_match_time(df: pd.DataFrame) -> int:
    """ Gathers the last match time from the matches table in the database """
    # Check if the DataFrame is empty
    if df.empty:
        print("No matches found in the database.")
        return 0

    # Try to set the last_match_time to the match_time of the furthest away scheduled match in the database
    if df[df['status'] == 'SCHEDULED'].empty:
        last_match_time = df['match_time'].min()
    # Else set the last_match_time to the last finished match in the database
    else:
        last_match_time = df[df['status'] == 'FINISHED']['match_time'].max()

    # Cap the last_match_time to the current time
    current_time = datetime.datetime.now().timestamp()
    if last_match_time > current_time:
        last_match_time = current_time
    
    # Round the last_match_time down to the start of the day
    return int(safe_convert_to_datetime(last_match_time))

### -----------------------------------------------------------------
### General Functions
### -----------------------------------------------------------------

def upload_data(table_name, df: pd.DataFrame) -> None:
    """ 
    Upload data from the provided dictionaries to their corresponding database tables

    Args:
        table_name (str)    : Name of the table to upload data to
        table_config (list) : List of keys for the table
        df (pd.DataFrame)   : DataFrame containing the data to upload
    """
    print(
        f"""
        ----------------------------------------------
            Uploading data to {table_name} table:
        ----------------------------------------------
        """
    )
    try:
        db, cursor = start_database()
        
        keys, primary_keys = gather_keys(table_name)
        if not keys or not primary_keys:
            print(f"Error: No keys found for table {table_name} - {keys} {primary_keys}.")
            return
        
        sql = upload_data_query(table_name, keys, primary_keys)
        
        ## Preparing the data as tuples with None for the missing keys in the database
        data = [
            tuple(d.get(col, None) for col in keys)
            for d in df.to_dict(orient='records')
        ]
        
        ## Uploading the data to the database 
        if not data:
            print(f"Error: No data to upload for table {table_name}.")
        else:
            # Start the database connection and cursor
            psycopg2.extras.execute_values(
                cursor, sql, data, page_size=1000, template=None, fetch=False
            )
            # cursor.executemany(sql, data)
      
    except Exception as e:
        print(f"Error while uploading data for table: {table_name}: {e}")
    
    finally:
        # Commit the transaction and close the database connection
        db.commit()
        close_database(db, cursor)

def gather_keys(table_name: str) -> tuple[list[str], list[str]]:
    """
    Gather all keys and primary keys from the specified table in the database
    """
    # SQL query to get all column names (keys) in the specified table
    query_all_keys = """
        SELECT 
            COLUMN_NAME
        FROM
            information_schema.COLUMNS
        WHERE
            TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
    """

    # SQL query to get primary key columns from the specified table
    query_primary_keys = """
        SELECT
            COLUMN_NAME
        FROM
            information_schema.KEY_COLUMN_USAGE
        WHERE
            TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND CONSTRAINT_NAME IN (
                SELECT CONSTRAINT_NAME
                FROM information_schema.TABLE_CONSTRAINTS
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                AND CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
           );
    """
    try:
        # Start the database connection and cursor
        db, cursor = start_database()
        
        # Execute the query for all keys (column names) in the table
        cursor.execute(query_all_keys, ('public', table_name))
        all_keys_list = cursor.fetchall()
        all_keys = [ak[0] for ak in all_keys_list]
        
        # Execute the query for primary keys in the table
        cursor.execute(query_primary_keys, ('public', table_name, 'public', table_name))
        primary_keys_list = cursor.fetchall()
        primary_keys = [pk[0] for pk in primary_keys_list]
        
    except Exception as e:
        print(f"Error while connecting to the database: {e}")
        return [], []
    finally:
        # Close the database connection
        close_database(db, cursor)
    
    # Return both all keys and primary keys as tuples of lists
    return all_keys, primary_keys

def upload_data_query(table_name: str, keys: list, primary_keys: list) -> str:
    """
    Creating the queries used for creating the tables based on the keys in the dictionary

    Args:
        table_name (str)    : Name of the table
        keys (list)         : list of all keys
        primary_keys (list) : list of all primary keys
        
    Returns:
        upload_query (str)   : String of the upload query to use in the cursor.executemany command
    """
    # Go through all keys and check if they are all primary keys. If so, create a query without the ON DUPLICATE KEY UPDATE
    if all(key in primary_keys for key in keys):
        # upload_query = f"""
        #     INSERT INTO {table_name} ({", ".join(keys)})
        #     VALUES ({", ".join(["%s"] * len(keys))})
        # """
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(keys)})
            VALUES %s
        """
        return upload_query
    else:
        # Creating the upload query
        # upload_query = f"""
        #     INSERT INTO {table_name} ({", ".join(keys)})
        #     VALUES ({", ".join(["%s"] * len(keys))})
        #     ON CONFLICT ({', '.join(primary_keys)})
        #     DO UPDATE SET
        #         {", ".join([f"{key} = EXCLUDED.{key}" for key in keys if key not in primary_keys])};
        # """
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(keys)})
            VALUES %s
            ON CONFLICT ({', '.join(primary_keys)})
            DO UPDATE SET
                {", ".join([f"{key} = EXCLUDED.{key}" for key in keys if key not in primary_keys])};
        """
        return upload_query   

def safe_convert_to_datetime(last_match_time):
    # Ensure last_match_time is a number (either int or float)
    if not isinstance(last_match_time, (int, float)):
        raise ValueError("Invalid timestamp provided")

    # Special case for 0 timestamp (Unix epoch)
    if last_match_time == 0:
        return 0  # Directly return 0 as timestamp

    # Check if last_match_time is a valid timestamp (should be non-negative and not too large)
    if last_match_time < 0 or last_match_time > 253402300800:  # Maximum valid timestamp (year 9999)
        raise ValueError("Timestamp out of valid range")

    # If in milliseconds, convert to seconds
    if last_match_time > 9999999999:  # If it's a timestamp in milliseconds
        last_match_time /= 1000

    # Convert to datetime
    return datetime.datetime.fromtimestamp(last_match_time).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
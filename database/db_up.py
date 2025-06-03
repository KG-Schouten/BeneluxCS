# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sqlite3

import datetime
import pandas as pd
import asyncio
import json

from database.db_manage import start_database, close_database
from database.db_config import db_name, table_names

from data_processing.dp_events import process_esea_teams_data, process_hub_data, process_championship_data
from data_processing.faceit_api.logging_config import function_logger

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
    """
    Function to update the database with new data based on the provided update type and event type.
    
    Args:
        update_type (str)  : Type of update to perform. Options: "all", "new", "single"
        event_type (str)   : Type of event to update. Options: "esea", "hub", "championship", "championship_hub"
        **kwargs (dict)     : Additional arguments for the update. For "hub", "championship", and "championship_hub", provide "event_id".
            - event_id (str) : ID of the event to update. Required for "hub", "championship", and "championship_hub" event types.

    """
    validate_update_inputs(update_type, event_type, kwargs)
    event_id = kwargs.get("event_id", None)
        
    last_match_time = None
    if update_type == "new":
        df_existing = gather_update_matches(event_type, event_id)
        last_match_time = gather_last_match_time(df_existing)
    else:
        last_match_time = 0  # Default to 0 if not updating new matches
    try:
        results = asyncio.run(gather_mapping_args(update_type, event_type, event_id, last_match_time))
    except Exception as e:
        print(f"Error while gathering results: {e}")
        return

    if not results:
        print("No results found. Skipping...")
        return
    
    name_to_df = {}
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
        
        name_to_df[df.name] = df
    
    for table_name in table_names.keys():
        df = name_to_df.get(table_name)
        if df is None:
            print(f"DataFrame for table {table_name} not found. Skipping...")
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

def gather_update_matches(event_type, event_id: str | None) -> pd.DataFrame:
    ## Create the query to get the matches
    if event_type == 'esea':
        query = """
            SELECT 
                m.match_id, 
                m.match_time, 
                m.status, 
                m.event_id
            FROM matches m
            INNER JOIN seasons s ON m.event_id = s.event_id;
        """
    elif event_type in ['hub', 'championship', 'championship_hub'] and event_id is not None:
        query = f"""
            SELECT 
                m.match_id, 
                m.match_time, 
                m.status, 
                m.internal_event_id
            FROM matches m
            LEFT JOIN events e ON m.internal_event_id = e.internal_event_id
            WHERE e.event_id = {event_id};
        """
    else:
        print(f"Invalid event_type: {event_type} or event_id: {event_id}")
        return pd.DataFrame()
    
    db, cursor = start_database()
    try:
        cursor.execute(query)
        res = cursor.fetchall()
        df = pd.DataFrame(res, columns=['match_id', 'match_time', 'status', 'event_id'])
        df['match_time'] = pd.to_numeric(df['match_time'], errors='coerce')
        df = df.dropna(subset=['match_time'])
        df = df.sort_values(by='match_time', ascending=False)
    except sqlite3.Error as e:
        print(f"Error gathering matches: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error gathering matches: {e}")
        return pd.DataFrame()
    finally:
        # Close the database connection
        close_database(db, cursor)
    
    return df

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
    if int(last_match_time) > int(current_time):
        last_match_time = current_time
    
    # Round the last_match_time down to the start of the day
    return int(safe_convert_to_datetime(last_match_time))

async def gather_mapping_args(update_type: str, event_type: str, event_id: str | None, last_match_time: int) -> list[pd.DataFrame]:
    """
    Gathers the arguments for the event type and update type to be used in the mapping function
    Args:
        update_type (str)  : Type of update to perform. Options: "all", "new", "single"
        event_type (str)   : Type of event to update. Options: "esea", "hub", "championship", "championship_hub"
        event_id (str)     : ID of the event to update. Required for "hub", "championship", and "championship_hub" event types.
        last_match_time (int): Last match time in seconds since epoch.
    """
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
    
    return await event_type_mapping[event_type](**args)

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
    db, cursor = start_database()
    try:
        keys, primary_keys = gather_keys(table_name, db_name)
        if not keys or not primary_keys:
            function_logger.info(f"No keys found for table {table_name}. Skipping upload.")
            return
        
        sql = upload_data_query(table_name, keys, primary_keys)
        
        df = clean_invalid_foreign_keys(df, table_name)
        
        ## Preparing the data as tuples with None for the missing keys in the database
        data = [
            tuple(
                json.dumps(val) if isinstance(val, (dict, list, tuple)) else val
                for val in (d.get(col, None) for col in keys)
            )
            for d in df.to_dict(orient='records')
        ]
        
        ## Uploading the data to the database 
        if not data:
            function_logger.info(f"No data to upload for table {table_name}. Skipping upload.")
        else:
            # Start the database connection and cursor
            cursor.executemany(sql, data)
      
    except Exception as e:
        function_logger.error(f"Error while uploading data to {table_name}: {e}")
    
    finally:
        # Commit the transaction and close the database connection
        db.commit()
        close_database(db, cursor)

def gather_keys(table_name: str, db_name: str) -> tuple[list[str], list[str]]:
    """
    Gather all keys and primary keys from the specified table in the database
    """
    
    all_keys = []
    primary_keys = []
    
    db, cursor = start_database()
    try:
        # Get all column info
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        
        # PRAGMA table_info returns:
        # cid, name, type, notnull, dflt_value, pk
        
        for col in columns:
            col_name = col[1]
            is_pk = col[5]  # 1 if part of the PK, 0 otherwise

            all_keys.append(col_name)
            if is_pk:
                primary_keys.append(col_name)
    
    except sqlite3.Error as e:
        print(f"Error while gathering keys for table {table_name}: {e}")
        return [], []
    except Exception as e:
        print(f"Unexpected error while gathering keys for table {table_name}: {e}")
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
    placeholders = ', '.join(['?'] * len(keys))
    
    if all(key in primary_keys for key in keys):
        # Insert without ON CONFLICT if all keys are primary keys
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(keys)})
            VALUES ({placeholders})
        """
    else:
        # Insert with ON CONFLICT for partial primary keys
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(keys)})
            VALUES ({placeholders})
            ON CONFLICT ({', '.join(primary_keys)})
            DO UPDATE SET
                {", ".join([f"{key} = EXCLUDED.{key}" for key in keys if key not in primary_keys])};
        """
    return upload_query.strip()

def clean_invalid_foreign_keys(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Removes rows with invalid foreign keys from the DataFrame before uploading to the database
    """
    db, cursor = start_database()
    
    # Ensure foreign key enforcement
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # Get foreign key constraints
    fk_info = pd.read_sql_query(f"PRAGMA foreign_key_list({table_name});", db)
    
    if fk_info.empty:
        function_logger.info(f"No foreign keys found for table {table_name}. Skipping foreign key validation.")
        close_database(db, cursor)
        return df
    
    original_df = df.copy()  # Keep a copy of the original DataFrame for logging
    valid_mask = pd.Series([True] * len(df), index=df.index)
    
    # For each foreign key, check if the values in the df match the existing values in the referenced table
    for fk_id, fk_group in fk_info.groupby('id'):
        local_cols = fk_group['from'].tolist()  # Local columns in the current table
        ref_table = fk_group['table'].iloc[0]  # Referenced table
        ref_cols = fk_group['to'].tolist()
        
        # Build query to get valid key tuples from referenced table
        ref_query = f"SELECT {', '.join(ref_cols)} FROM {ref_table};"
        
        try:
            valid_tuples = pd.read_sql_query(ref_query, db)
            valid_set = set([tuple(map(str, row)) for row in valid_tuples[ref_cols].itertuples(index=False, name=None)])
            
            df_tuples = df[local_cols].astype(str).apply(lambda row: tuple(row), axis=1)
            this_valid_mask = df_tuples.isin(valid_set)
            
            removed_mask = valid_mask & ~this_valid_mask
            removed_rows = df[removed_mask]
            
            if not removed_rows.empty:
                function_logger.warning(f"Removing {len(removed_rows)} rows with invalid foreign keys in table {table_name} for FK {fk_id}.")
                function_logger.debug(f"Invalid rows: {removed_rows.to_dict(orient='records')}")

            valid_mask &= this_valid_mask
            
        except sqlite3.OperationalError as e:
            function_logger.error(f"Operational error while checking foreign keys for table {table_name}: {e}")
            close_database(db, cursor)
            return df
        except sqlite3.Error as e:
            function_logger.error(f"Error while checking foreign keys for table {table_name}: {e}")
            close_database(db, cursor)
            return df
        except Exception as e:
            function_logger.error(f"Unexpected error while checking foreign keys for table {table_name}: {e}")
            close_database(db, cursor)
            return df
    
    clean_df = original_df[valid_mask].copy()
    close_database(db, cursor)
    return clean_df

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
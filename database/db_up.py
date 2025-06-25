# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sqlite3

import datetime
import pandas as pd
import json

from database.db_manage import start_database, close_database
from database.db_config import db_name

from data_processing.faceit_api.logging_config import function_logger

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
    print(f" --- Uploading data to {table_name} table --- ")
    
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
            function_logger.info(f"Successfully uploaded {len(data)} rows to {table_name} table.")
      
    except Exception as e:
        function_logger.error(f"Error while uploading data to {table_name}: {e}")
        db.rollback()
    
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
    
    if not keys or not primary_keys:
        raise ValueError(f"Cannot build upload query without keys or primary keys for table {table_name}")
    
    # SQL injection prevention: sanitize keys and primary keys
    keys = [key.replace('"', '""') for key in keys]  # Escape double quotes
    sanitized_keys = [f'"{key}"' for key in keys]
    sanitized_primary = [f'"{pk}"' for pk in primary_keys]
    
    placeholders = ', '.join(['?'] * len(keys))
    
    if all(key in sanitized_primary for key in sanitized_keys):
        # Insert without ON CONFLICT if all keys are primary keys
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(sanitized_keys)})
            VALUES ({placeholders})
        """
    else:
        # Insert with ON CONFLICT for partial primary keys
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(sanitized_keys)})
            VALUES ({placeholders})
            ON CONFLICT ({', '.join(sanitized_primary)})
            DO UPDATE SET
                {", ".join([f"{key} = EXCLUDED.{key}" for key in sanitized_keys if key not in sanitized_primary])};
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
        
        missing_fk_cols = [col for col in local_cols if col not in df.columns]
        if missing_fk_cols:
            function_logger.warning(f"Skipping FK check {fk_id} due to missing local columns: {missing_fk_cols}")
            continue
        
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
        raise ValueError(f"Invalid timestamp type: {type(last_match_time).__name__}")
    elif isinstance(last_match_time, str):
        try:
            dt = datetime.datetime.fromisoformat(last_match_time)
            return dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        except ValueError:
            raise ValueError("Invalid ISO datetime string")
    
    # Special case for 0 timestamp (Unix epoch)
    if last_match_time == 0:
        return 0  # Directly return 0 as timestamp

    # Check if last_match_time is a valid timestamp (should be non-negative and not too large)
    if not (0 <= last_match_time <= 253402300800):
        raise ValueError(f"Timestamp {last_match_time} out of valid range (0 to year 9999)")

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
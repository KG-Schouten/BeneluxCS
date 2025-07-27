# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from psycopg2 import sql
from psycopg2.extras import execute_values


import datetime
import pandas as pd
import json

from database.db_manage import start_database, close_database

from data_processing.faceit_api.logging_config import function_logger

### -----------------------------------------------------------------
### General Functions
### -----------------------------------------------------------------

def upload_data(table_name, df: pd.DataFrame, clear=False) -> None:
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
        if clear:
            # Safely construct DELETE query
            cursor.execute(f'DELETE FROM "{table_name}"')
            function_logger.info(f"Cleared data from {table_name} table.")
        
        if df.empty:
            function_logger.info(f"No data to upload for table {table_name}. DataFrame is empty.")
            return
           
        keys, primary_keys = gather_keys(table_name)
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
            execute_values(cursor, sql, data)
            updated_rows = cursor.fetchall()
            function_logger.info(f"Successfully uploaded {len(updated_rows)}/{len(data)} rows to {table_name} table.")
      
    except Exception as e:
        function_logger.error(f"Error while uploading data to {table_name}: {e}")
        db.rollback()
    
    finally:
        # Commit the transaction and close the database connection
        db.commit()
        close_database(db)

def gather_keys(table_name: str) -> tuple[list[str], list[str]]:
    """
    Gather all column names and primary key column names from the specified PostgreSQL table.
    """
    all_keys = []
    primary_keys = []

    db, cursor = start_database()
    try:
        # Fetch all column names
        cursor.execute(sql.SQL("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """), (table_name,))
        all_keys = [row[0] for row in cursor.fetchall()]

        # Fetch primary key column names
        cursor.execute(sql.SQL("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """), (table_name,))
        primary_keys = [row[0] for row in cursor.fetchall()]

    except Exception as e:
        print(f"Error while gathering keys for table {table_name}: {e}")
        return [], []
    finally:
        close_database(db)

    return all_keys, primary_keys

def upload_data_query(table_name: str, keys: list[str], primary_keys: list[str]) -> str:
    """
    Create the SQL insert (with optional upsert) query for PostgreSQL using execute_values.

    Args:
        table_name (str)    : Name of the table
        keys (list)         : List of all column keys
        primary_keys (list) : List of primary key columns

    Returns:
        str : SQL query string compatible with psycopg2.extras.execute_values()
    """

    if not keys or not primary_keys:
        raise ValueError(f"Cannot build upload query without keys or primary keys for table {table_name}")

    # Sanitize/quote identifiers
    escaped_keys = [f'"{key.replace("\"", "\"\"")}"' for key in keys]
    escaped_pks = [f'"{pk.replace("\"", "\"\"")}"' for pk in primary_keys]

    if all(k in primary_keys for k in keys):
        # No upsert, just INSERT
        query = f"""
            INSERT INTO "{table_name}" ({', '.join(escaped_keys)})
            VALUES %s;
        """
    else:
        # INSERT with ON CONFLICT DO UPDATE
        update_clause = ', '.join([
            f'{key} = EXCLUDED.{key}' for key in escaped_keys if key not in escaped_pks
        ])
        query = f"""
            INSERT INTO "{table_name}" ({', '.join(escaped_keys)})
            VALUES %s
            ON CONFLICT ({', '.join(escaped_pks)})
            DO UPDATE SET {update_clause}
            RETURNING {', '.join(escaped_keys)};
        """
    return query.strip()

def clean_invalid_foreign_keys(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Removes rows from a DataFrame that would violate foreign key constraints in PostgreSQL.
    """
    db, cursor = start_database()

    try:
        # Get foreign key relationships from PostgreSQL catalog
        query = """
        SELECT
            tc.constraint_name,
            kcu.column_name AS local_column,
            ccu.table_name AS ref_table,
            ccu.column_name AS ref_column
        FROM 
            information_schema.table_constraints AS tc
        JOIN 
            information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
           AND tc.table_schema = kcu.table_schema
        JOIN 
            information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
           AND ccu.table_schema = tc.table_schema
        WHERE 
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = %s
        """
        cursor.execute(query, (table_name,))
        fk_info = cursor.fetchall()

        if not fk_info:
            function_logger.info(f"No foreign keys found for table {table_name}. Skipping foreign key validation.")
            return df

        # Build a DataFrame from the results
        fk_df = pd.DataFrame(fk_info, columns=["constraint_name", "local_column", "ref_table", "ref_column"])

        original_df = df.copy()
        valid_mask = pd.Series([True] * len(df), index=df.index)

        for constraint_name, fk_group in fk_df.groupby("constraint_name"):
            local_cols = fk_group['local_column'].tolist()
            ref_table = fk_group['ref_table'].iloc[0]
            ref_cols = fk_group['ref_column'].tolist()

            # Skip if missing required local columns
            missing_cols = [col for col in local_cols if col not in df.columns]
            if missing_cols:
                function_logger.warning(f"Skipping FK check {constraint_name} due to missing columns: {missing_cols}")
                continue

            try:
                # Get valid referenced values
                ref_query = f'SELECT {", ".join(ref_cols)} FROM "{ref_table}";'
                cursor.execute(ref_query)
                rows = cursor.fetchall()
                valid_tuples = pd.DataFrame(rows, columns=ref_cols)

                valid_set = set([tuple(map(str, row)) for row in valid_tuples.itertuples(index=False, name=None)])
                df_tuples = df[local_cols].astype(str).apply(lambda row: tuple(row), axis=1)

                this_valid_mask = df_tuples.isin(valid_set)
                removed_rows = df[valid_mask & ~this_valid_mask]

                if not removed_rows.empty:
                    function_logger.warning(
                        f"Removing {len(removed_rows)} rows violating foreign key {constraint_name} "
                        f"from table {table_name}."
                    )
                    function_logger.debug(f"Invalid rows: {removed_rows.to_dict(orient='records')}")

                valid_mask &= this_valid_mask

            except Exception as e:
                function_logger.error(f"Error validating FK {constraint_name} for table {table_name}: {e}")
                return df

        return original_df[valid_mask].copy()

    except Exception as e:
        function_logger.error(f"Unexpected error gathering FKs for table {table_name}: {e}")
        return df

    finally:
        close_database(db)

def safe_convert_to_datetime(last_match_time):
    """
    Safely converts a timestamp or ISO datetime string to a Unix timestamp at 00:00:00 UTC.
    Accepts:
        - int or float Unix timestamps (in seconds or milliseconds)
        - ISO 8601 datetime strings
    Returns:
        float: Unix timestamp at midnight
    Raises:
        ValueError if input is invalid or out of range.
    """

    # Handle ISO 8601 strings
    if isinstance(last_match_time, str):
        try:
            dt = datetime.datetime.fromisoformat(last_match_time)
            return dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        except ValueError:
            raise ValueError(f"Invalid ISO datetime string: {last_match_time}")

    # Handle numeric input
    elif isinstance(last_match_time, (int, float)):
        if last_match_time == 0:
            return 0

        # Convert milliseconds to seconds if needed
        if last_match_time > 9999999999:
            last_match_time /= 1000

        # Validate range (years 1970 to 9999)
        if not (0 <= last_match_time <= 253402300800):
            raise ValueError(f"Timestamp {last_match_time} out of valid range (0 to year 9999)")

        return datetime.datetime.fromtimestamp(last_match_time).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp()

    else:
        raise ValueError(f"Invalid input type: {type(last_match_time).__name__}")

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
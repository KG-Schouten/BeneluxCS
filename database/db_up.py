# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2.extras

import re
import datetime
import pandas as pd
import numpy as np
from typing import *
import asyncio

from database.db_manage import start_database, close_database
from database.db_down import *
from database.db_config import *

from data_processing.dp_events import process_esea_teams_data, process_hub_data, process_championship_data

# Constants
db_name = "BeneluxCS"

### -----------------------------------------------------------------
### FACEIT Ranking
### -----------------------------------------------------------------


### -----------------------------------------------------------------
### ESEA League Data 
### -----------------------------------------------------------------

def update_esea_data(**kwargs) -> None:
    """
    Updates the ESEA league data by gathering the latest matches from an external API and appending them to the database.
    
    Args:
        **kwargs: Additional keyword arguments for the function.
            - update_type (str): Type of update to perform. Options are 'ALL', 'NEW', "SINGLE" (default: 'ALL').
    """
    ## Gather the kwargs
    update_type = kwargs.get("update_type", "ALL")
    if update_type not in ["ALL", "NEW", "SINGLE"]:
        print("Invalid update_type. Use 'ALL' or 'NEW'.")
        return
    
    ## Gather the data from the data processing function
    if update_type == "ALL":
        # Gather all of the data from ESEA
        results = asyncio.run(process_esea_teams_data(season_number="ALL"))
    elif update_type == "SINGLE":
        results = asyncio.run(process_esea_teams_data(season_number=52, match_amount=1))
    elif update_type == "NEW":
        ## Gather the columns ['match_id', 'match_time', 'status', 'event_id'] from matches from the matches table in the database where the event_id is in the seasons table
        try:
            db, cursor = start_database()
            query = """
                SELECT m.match_id, m.match_time, m.status, m.event_id
                FROM matches m
                INNER JOIN seasons s ON m.event_id = s.event_id;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            # Make results into a dataframe
            df = pd.DataFrame(results, columns=['match_id', 'match_time', 'status', 'event_id'])
            df = df.sort_values(by='match_time', ascending=False)
        except Exception as e:
            print(f"Error while fetching data from the matches table: {e}")
            return
        finally:
            close_database(db, cursor)
        

        # if df is empty, set the last_match_time to 0
        if df.empty:
            print("No matches found in the database.")
            last_match_time = 0
        # Try to set the last_match_time to the match_time of the furthest away scheduled match in the database
        elif df[df['status'] == 'SCHEDULED'].empty:
            last_match_time = df['match_time'].min()
        # Else set the last_match_time to the last finished match in the database
        else:
            last_match_time = df[df['status'] == 'FINISHED']['match_time'].max()
        
        # Cap the last_match_time to the current time
        current_time = datetime.datetime.now().timestamp()
        if last_match_time > current_time:
            last_match_time = current_time
        
        # Round the last_match_time down to the start of the day
        last_match_time = safe_convert_to_datetime(last_match_time)
        
        results = asyncio.run(process_esea_teams_data(season_number="ALL", from_timestamp=last_match_time))
        
    df_seasons, df_events, df_teams_benelux, df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = results

    for table_name in table_names.keys():
        df = eval(f"df_{table_name}")
        if df is None:
            print(f"Dataframe {table_name} is None. Skipping...")
            continue
        else:
            upload_data(table_name, df)

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

### -----------------------------------------------------------------
### Benelux Hub Data
### -----------------------------------------------------------------

def update_hub_data(table_names) -> None:
    """
    Updates the hub data by gathering the latest matches from an external API and appending them to the database.
    
    Args:
        table_names (Dict): dictionary containing table names as keys in order of the data processing returns (db_general.py)
    """
    # Start the database connection
    db, cursor = start_database()

    # Gather the ID of the latest match in the database to know where to start fetching new data
    query = """
        SELECT
            m.match_id
        FROM matches m
        ORDER BY m.start_time DESC
        LIMIT 1
    """
    cursor.execute(query)
    res = cursor.fetchall()

    # Assuming there is at least one match, extract the latest match_id
    last_match_id = res[0][0] if res else None
    if last_match_id is None:
        print("No matches found in the database.")
        return

    # Initialize variables for batch processing of matches, players, and teams
    LOOP = True
    last_batch = 0
    batch = 5

    # Initialize lists to store data for matches, players, and teams
    batch_data_match: List[Dict[str, any]] = []
    batch_data_player: List[Dict[str, any]] = []
    batch_data_team: List[Dict[str, any]] = []

    # Loop to gather new match data from the external API
    while LOOP:
        # Fetch a batch of match, player, and team data starting from the current batch position
        single_batch_data_match, single_batch_data_player, single_batch_data_team = process_hub_data(starting_item_position=int(last_batch), return_items=int(batch))

        # Check if any match in the batch matches the latest match ID in the database
        index = next((i for i, d in enumerate(single_batch_data_match) if d["match_id"] == last_match_id), -1)
        if index != -1:
            LOOP = False  # Stop looping once the latest match is found
        else:
            last_batch += batch  # Increment the batch position to fetch the next set of matches

        # Append the fetched data to the corresponding lists
        batch_data_match.extend(single_batch_data_match)
        batch_data_player.extend(single_batch_data_player)
        batch_data_team.extend(single_batch_data_team)

    # Upload the gathered match data to the database
    
    upload_data(table_names, (batch_data_match, batch_data_player, batch_data_team))
    
    calculate_hltv()
    
    # Print the number of matches added to the database
    matches_added = next(i for i, d in enumerate(batch_data_match) if d["match_id"] == last_match_id)
    print(f"{matches_added} Matches added to the database")
    
    # Commit the transaction and close the database connection
    db.commit()
    close_database(db, cursor)

### -----------------------------------------------------------------
### Player Country Data
### -----------------------------------------------------------------

def update_players_country_data(data: Union[pd.DataFrame, List[Union[Tuple, List]], Dict[str, List]]) -> None:
    """
    Update the players_country table in the database with players and their country codes.
    
    Args:
        data: A DataFrame, list of tuples/lists, or a dict with keys 'player_id', 'player_name', 'country'.
    """
    expected_keys = {'player_id', 'player_name', 'country'}
    
    # === Normalize the data to a dataframe ===
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    elif isinstance(data, dict):
        if not expected_keys.issubset(data.keys()):
            print(f"Data dictionary must contain keys: {expected_keys}")
            return
        df = pd.DataFrame(data)
    elif isinstance(data, list):
        try:
            df = pd.DataFrame(data, columns=['player_id', 'player_name', 'country'])
        except Exception as e:
            print(f"List input must contain tuples/lists with keys: {expected_keys}. Error: {e}")
            return
    else:
        print("Input data must be a DataFrame, list of tuples/lists, or a dict.")
        return
    
    # === Validate the DataFrame ===
    if df['player_id'].duplicated().any():
        print("Error: 'player_id' values must be unique.")
        return

    if not pd.api.types.is_string_dtype(df['player_name']):
        print("Error: 'player_name' must be of string type.")
        return

    if not df['country'].apply(lambda x: isinstance(x, str) and re.match(r'^[A-Z]{2}$', x)).all():
        print("Error: 'country' must be valid 2-letter ISO country codes.")
        return

    # === Upload the DataFrame to the Database ===
    db, cursor = start_database()
    
    # Create the query to insert or update player data in the players_country table
    query = """
        INSERT INTO players_country (player_id, player_name, country)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE player_name = VALUES(player_name), country = VALUES(country)
    """
    
    try:
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        cursor.executemany(query, values)
        db.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(f"Error while uploading players_country data: {e}")
    finally:
        close_database(db, cursor)
    
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

def gather_keys(table_name: str) -> Tuple[List[str], List[str]]:
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

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
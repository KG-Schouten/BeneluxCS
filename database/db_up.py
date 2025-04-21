# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mysql.connector
import json
import re
import datetime
import pandas as pd
import numpy as np
from typing import *

from database.db_manage import start_database, close_database
from database.db_down import *
from database.db_config import table_names_esea, table_names_hub

# Constants
db_name = "BeneluxCS"

### -----------------------------------------------------------------
### FACEIT Ranking
### -----------------------------------------------------------------


### -----------------------------------------------------------------
### ESEA League Data 
### -----------------------------------------------------------------

def update_esea_data(table_names) -> None:
    
    pass

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

def upload_data(table_names, data) -> None:
    """ 
    Upload data from the provided dictionaries to their corresponding database tables

    Args:
        table_names (Dict): dictionary containing table names as keys in order of the data processing returns (db_general.py)
        data (Tuple[ List[ Dict[str, any] ] ]): The tuple returned by the data processing containing lists of data dictionaries (data_processing.py)
    """
    print(
        """
        
        ---------------------------------------
                Uploading data to tables:
        ---------------------------------------
        
        """
    )
    
    # Start the database connection and cursor
    db, cursor = start_database()
    
    try:
        ## Process and upload the data
        for table_name, data in zip(table_names.keys(), data): 
            ## Creating the query for the table
            print(f"Creating query for table: {table_name}")
            
            keys, primary_keys = gather_keys(cursor, table_name)
            sql = upload_data_query(table_name, keys, primary_keys)
            
            ## Preparing the data as tuples with None for the missing keys in the database
            data_prepped = [
                tuple(d.get(col, None) for col in keys)
                for d in data
            ]
            
            ## Upload the data
            print(f"Adding the data to: {table_name}")  
            cursor.executemany(sql, data_prepped)
      
    except Exception as e:
        print(f"Error while uploading data for table: {table_name}: {e}")
    
    finally:
        # Commit the transaction and close the database connection
        db.commit()
        close_database(db, cursor)

def gather_keys(cursor: mysql.connector.cursor, table_name: str) -> Tuple[List[str], List[str]]:
    """
    Gather all keys and primary keys from a specific table in the database.

    This function retrieves the list of all column names (keys) and primary keys 
    from the specified table using the information_schema views in the database.

    Args:
        cursor (mysql.connector.cursor): The active cursor object used to execute queries.
        table_name (str): The name of the table to retrieve keys from.
    
    Returns:
        Tuple[List[str], List[str]]:
            - all_keys (List[str]): A list of all column names (keys) in the table.
            - primary_keys (List[str]): A list of primary key column names in the table.
    """
    global db_name

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
            AND CONSTRAINT_NAME IN ('PRIMARY', 'FOREIGN KEY');
    """

    # Execute the query for all keys (column names) in the table
    cursor.execute(query_all_keys, (db_name, table_name))
    all_keys_list = cursor.fetchall()

    # Execute the query for primary keys in the table
    cursor.execute(query_primary_keys, (db_name, table_name))
    primary_keys_list = cursor.fetchall()

    # Extract column names from the query results
    all_keys = [ak[0] for ak in all_keys_list]
    primary_keys = [pk[0] for pk in primary_keys_list]

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
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(keys)})
            VALUES ({", ".join(['%s'] * len(keys))})
            """
        return upload_query
    else:
        # Creating the upload query
        upload_query = f"""
            INSERT INTO {table_name} ({", ".join(keys)})
            VALUES ({", ".join(['%s'] * len(keys))})
            ON DUPLICATE KEY UPDATE
            {", ".join([f"`{key}` = VALUES(`{key}`)" for key in keys if key not in primary_keys])}
        """
        return upload_query

def calculate_hltv(table_name_stats, table_name_matches):
    """
    Calculates and uploads the HLTV 1.0 ratings for player stats of matches in the database
    
    Args:
        table_name_stats: The table name of the player statistics in mySQL
        table_name_matches: The table name of the match table in mySQL
    """
    print(
        f"""
        
        ---------------------------------------
            Calculating HLTV for {table_name_stats}:
        ---------------------------------------
        
        """
    )
    
    db, cursor = start_database()
    
    ## Preventing SQL injection
    allowed_tables = {"esea_player_stats", "hub_player_stats"}  # Defining safe table names
    if table_name_stats not in allowed_tables:
        raise ValueError("Invalid table name!")
    
    ## Check if the HLTV column exists in the player_stats table
    cursor.execute(
        f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table_name_stats}'
            AND column_name = 'hltv_rating'
        """
    )
    column_exists = cursor.fetchone()[0] > 0 # If it exists, column_exists > 0

    if not column_exists:
        cursor.execute(
            f"""
                ALTER TABLE {table_name_stats}
                ADD COLUMN hltv_rating VARCHAR(100)
            """
        )

    ## Calculate HLTV 1.0 rating
    avg_kpr = 0.679 # avg kill per round
    avg_spr = 0.317 # avg survived rounds per round
    avg_rmk = 1.277 # avg value calculated from rounds with multi-kills

    hltv_query = f"""
        SELECT
            p.player_id,
            p.match_id,
            p.match_round,
            p.kills,
            p.deaths,
            p.Double_Kills,
            p.Triple_Kills,
            p.Quadro_Kills,
            p.Penta_Kills,
            m.Rounds
        FROM {table_name_stats} p
        JOIN {table_name_matches} m ON m.match_id = p.match_id
    """

    cursor.execute(hltv_query)
    res = cursor.fetchall()
    columns = ['player_id', 'match_id', 'match_round', 'kills', 'deaths', 'double', 'triple', 'quadro', 'penta', 'rounds']
    df = pd.DataFrame(res, columns=columns)

    # print(df)
    ratings = []
    for index, row in df.iterrows():
        # print(row)
        player_id = row['player_id']
        match_id = row['match_id']
        match_round = row['match_round']
        kills = row['kills']
        deaths = row['deaths']
        rounds = int(row['rounds'])
        double = row['double']
        triple = row['triple']
        quadro = row['quadro']
        penta = row['penta']
        single = kills - (2*double + 3*triple + 4*quadro + 5*penta)

        kill_rating = kills / rounds / avg_kpr
        survival_rating = (rounds - deaths) / rounds / avg_spr
        rounds_with_multi_kill_rating = (single + 4*double + 9*triple + 16*quadro + 25*penta) / rounds / avg_rmk

        hltv_rating = (kill_rating + 0.7*survival_rating + rounds_with_multi_kill_rating) / 2.7
        
        ratings.append((player_id, match_id, match_round, round(hltv_rating,2)))

        # print(hltv_rating)
    
    query_insert = f"""
        INSERT INTO {table_name_stats}(player_id, match_id, match_round, hltv_rating)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE hltv_rating = VALUES(hltv_rating)
    """
    cursor.executemany(query_insert, ratings)
    db.commit()
    
    close_database(db, cursor)

    return ratings   

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    calculate_hltv("esea_player_stats", "esea_maps")
    pass
import mysql.connector
import json
import re
import datetime
import pandas as pd
import numpy as np
from typing import *

from functions import load_api_keys
from data_processing import process_hub_data

from database.db_manage import start_database, close_database
from database.db_down import *

# Constants
db_name = "BeneluxCS"


def upload_data_query(table_name: str, data: dict, keys: list, primary_keys: list) -> str:
    """
    Creating the queries used for creating the tables based on the keys in the dictionary

    Args:
        table_name (str)    : Name of the table.
        data (dict)         : Single level dictionary with keys-values.
        keys (list)         : list of all keys
        primary_keys (list) : list of all primary keys
        
    Returns:
        upload_query (str)   : String of the upload query to use in the cursor.executemany command
    """

    # Creating the upload query
    upload_query = f"""
        INSERT INTO {table_name} ({", ".join(keys)})
        VALUES ({", ".join(['%s'] * len(keys))})
        ON DUPLICATE KEY UPDATE
        {", ".join([f"`{key}` = VALUES(`{key}`)" for key in keys if key not in primary_keys])}
    """

    return upload_query

def gather_keys(db: mysql.connector.connect, cursor: mysql.connector.cursor, table_name: str) -> Tuple[List[str], List[str]]:
    """
    Gather all keys and primary keys from a specific table in the database.

    This function retrieves the list of all column names (keys) and primary keys 
    from the specified table using the information_schema views in the database.

    Args:
        db (mysql.connector.connect): The active database connection.
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

def upload_hub_data(batch_data_match: List[Dict[str, any]], batch_data_player: List[Dict[str, any]], batch_data_team: List[Dict[str, any]]) -> None:
    """ 
    Upload data from the provided dictionaries to their corresponding database tables: 'matches', 'teams', and 'player_statistics'.

    This function processes and uploads data for three tables:
    - 'matches' for match-related data
    - 'teams' for team-related data
    - 'player_statistics' for player-related statistics

    Args:
        batch_data_match (List[Dict[str, any]]): List of dictionaries where each dictionary represents match data.
        batch_data_player (List[Dict[str, any]]): List of dictionaries where each dictionary represents player data.
        batch_data_team (List[Dict[str, any]]): List of dictionaries where each dictionary represents team data.
        
    Returns:
        None: This function does not return any value but commits the data to the database.
    """
    # Start the database connection and cursor
    db, cursor = start_database()

    try:
        # Process and upload "matches" data
        table_name = "matches"
        keys, primary_keys = gather_keys(db, cursor, table_name)
        sql_matches = upload_data_query(table_name, batch_data_match, keys, primary_keys)
        print(f"Created query for table: {table_name}")

        # Prepare match data as tuples with None for missing keys in the database
        batch_data_match = [
            tuple(d.get(col, None) for col in keys) 
            for d in batch_data_match
        ]
        
        # Process and upload "teams" data
        table_name = "teams"
        keys, primary_keys = gather_keys(db, cursor, table_name)
        sql_teams = upload_data_query(table_name, batch_data_team, keys, primary_keys)
        print(f"Created query for table: {table_name}")

        # Prepare team data as tuples with None for missing keys in the database
        batch_data_team = [
            tuple(d.get(col, None) for col in keys) 
            for d in batch_data_team
        ]

        # Process and upload "player_statistics" data
        table_name = "player_statistics"
        keys, primary_keys = gather_keys(db, cursor, table_name)
        sql_player_statistics = upload_data_query(table_name, batch_data_player, keys, primary_keys)
        print(f"Created query for table: {table_name}")

        # Prepare player data as tuples with None for missing keys in the database
        batch_data_player = [
            tuple(d.get(col, None) for col in keys) 
            for d in batch_data_player
        ]

        # Upload the processed data to their respective tables using executemany
        cursor.executemany(sql_matches, batch_data_match)
        print("Added the data to 'matches'")  
        cursor.executemany(sql_teams, batch_data_team)
        print("Added the data to 'teams'")  
        cursor.executemany(sql_player_statistics, batch_data_player)
        print("Added the data to 'player_statistics'")  

    finally:
        # Commit the transaction and close the database connection
        db.commit()
        close_database(db, cursor)

def update_hub_data() -> None:
    """
    Updates the hub data by gathering the latest matches from an external API and appending them to the database.
    
    Args:
        None
    
    Returns:
        None
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
    upload_hub_data(batch_data_match, batch_data_player, batch_data_team)
    
    # Print the number of matches added to the database
    matches_added = next(i for i, d in enumerate(batch_data_match) if d["match_id"] == last_match_id)
    print(f"{matches_added} Matches added to the database")

    # Commit the transaction and close the database connection
    db.commit()
    close_database(db, cursor)

def calculate_hltv():
    db, cursor = start_database()
    
    ## Check if the HLTV column exists in the 'matches' table
    cursor.execute(
        """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'player_statistics'
            AND column_name = 'hltv_rating'
        """
    )
    column_exists = cursor.fetchone()[0] > 0 # If it exists, column_exists > 0

    if not column_exists:
        cursor.execute(
            """
                ALTER TABLE player_statistics
                ADD COLUMN hltv_rating VARCHAR(100)
            """
        )

    ## Calculate HLTV 1.0 rating
    avg_kpr = 0.679 # avg kill per round
    avg_spr = 0.317 # avg survived rounds per round
    avg_rmk = 1.277 # avg value calculated from rounds with multi-kills

    hltv_query = """
        SELECT
            p.player_id,
            p.match_id,
            p.kills,
            p.deaths,
            p.Double_Kills,
            p.Triple_Kills,
            p.Quadro_Kills,
            p.Penta_Kills,
            matches.Rounds
        FROM player_statistics p
        JOIN matches ON matches.match_id = p.match_id
    """

    cursor.execute(hltv_query)
    res = cursor.fetchall()
    columns = ['player_id', 'match_id', 'kills', 'deaths', 'double', 'triple', 'quadro', 'penta', 'rounds']
    df = pd.DataFrame(res, columns=columns)

    # print(df)
    ratings = []
    for index, row in df.iterrows():
        # print(row)
        player_id = row['player_id']
        match_id = row['match_id']
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
        
        ratings.append((player_id, match_id, round(hltv_rating,2)))

        # print(hltv_rating)



    query_insert = """
        INSERT INTO player_statistics(player_id, match_id, hltv_rating)
        VALUES (%s, %s, %s)
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

    pass
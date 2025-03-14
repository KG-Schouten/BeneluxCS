import mysql.connector
from functions import load_api_keys
import json
import re
import datetime
import pandas as pd
import numpy as np
from typing import *


from data_processing import process_hub_data

db_name = "BeneluxCS"

def start_database():
    """Starting up the mySQL database so it can be used by the rest of the program"""

    global db_name
    ## Load password for MySQL database
    api_keys = load_api_keys()
    mysql_password = api_keys["MYSQL_PASSWORD"]

    db = mysql.connector.connect(
        host="localhost",
        user="Koen",
        passwd=mysql_password,
        database=db_name
    )
    cursor = db.cursor()

    return db, cursor

def close_database(db, cursor):
    """Closing the database connection"""
    db.close()
    cursor.close()

def reset_database():
    """Completely resetting the database from scratch (resetting all tables)"""
    try:
        # Delete all tables
        delete_tables()

        # Create the tables again
        create_tables()

        # Append all match data of the hub back to the database
        batch_data_match, batch_data_player, batch_data_team = process_hub_data(return_items="ALL")
        upload_hub_data(batch_data_match, batch_data_player, batch_data_team)
    except Exception as e:
        print(f"Error while resetting database: {e}")

def delete_tables():
    """Completely resetting the database from scratch (resetting all tables)"""
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
            column_type = "INT"
        else:
            column_type = "VARCHAR(255)" # Default to VARCHAR
        column_types.append(f"`{col}` {column_type}")

    table_keys = []
    
    if primary_keys:
        primary_string = f"PRIMARY KEY ({",".join(primary_keys)})"
        table_keys.append(primary_string)
    
    if foreign_keys:
        # Sort dict based on called table
        foreign_key_dict = {}
        for key, table in foreign_keys:
            if table not in foreign_key_dict:
                foreign_key_dict[table] = []
            foreign_key_dict[table].append(key)
        
        # Create the strings
        for table in foreign_key_dict:
            foreign_string = f"FOREIGN KEY ({", ".join(foreign_key_dict[table])}) REFERENCES {table}({", ".join(foreign_key_dict[table])})"
            table_keys.append(foreign_string)

    # Create the table query
    table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {", ".join(column_types)}, {", ".join(table_keys)}
    );
    """

    return table_query

def create_tables():
    """Creating the tables in the MySQL database"""

    db, cursor = start_database()
    
    batch_data_match, batch_data_player, batch_data_team = process_hub_data(return_items=1)

    try:
        ## Create the queries    
        sql_matches = create_table_query("matches", batch_data_match[0], ["match_id"])
        sql_teams = create_table_query("teams", batch_data_team[0], ["match_id", "team_id"], [("match_id", "matches")])
        sql_player_statistics = create_table_query("player_statistics", batch_data_player[0], ["player_id", "match_id"], [("match_id", "teams"), ("team_id", "teams"), ("match_id", "matches")])

        # Create the tables
        cursor.execute(sql_matches)
        cursor.execute(sql_teams)
        cursor.execute(sql_player_statistics)

    finally:
        db.commit()
        close_database(db,cursor)

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

def calculate_hltv():
    db, cursor = start_database()
    
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

if __name__ == "__main__":
    # reset_database()
    
    update_hub_data()

    # db, cursor = start_database()


    # ### HLTV RATING CALCULATOR AND APPENDING TO DB
    # ratings = calculate_hltv()

    # start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).timestamp()

    # query = """
    #     SELECT
    #         p.player_id,
    #         (
    #             SELECT p2.nickname
    #             FROM player_statistics p2
    #             WHERE p2.player_id = p.player_id
    #             ORDER BY p2.match_id DESC
    #             LIMIT 1
    #         ) AS latest_nickname,

    #         COUNT(p.match_id) AS matches_played,
    #         SUM(p.result) AS wins,
    #         ROUND(AVG(p.Kills), 0) AS avg_kills,
    #         ROUND(AVG(p.Assists), 0) AS avg_assists,
    #         ROUND(AVG(p.Deaths), 0) AS avg_deaths,
    #         ROUND(AVG(p.ADR), 1) AS avg_adr,
    #         ROUND(AVG(p.K_D_Ratio), 2) AS avg_kdr,
    #         ROUND(AVG(p.hltv_rating), 2) AS avg_hltv,
    #         SUM(p.Knife_Kills) AS knife_total,
    #         SUM(p.Zeus_Kills) AS zeus_total
    #     FROM player_statistics p
    #     JOIN matches ON matches.match_id = p.match_id
    #     WHERE matches.start_time > %s
    #     GROUP BY p.player_id
    # """

    # # Get player data into dataframe
    # cursor.execute(query, (start_date,))
    # res = cursor.fetchall()
    # columns = ['player_id','nickname', 'matches_played', 'wins', 'kills', "assists", 'deaths', 'adr','kdr', 'hltv', 'knife_kills', 'zeus_kills']
    # df = pd.DataFrame(res, columns=columns)

    # # Get total matches played
    # query = """
    #     SELECT
    #         COUNT(DISTINCT(matches.match_id)) AS total_matches
    #     FROM matches
    #     WHERE matches.start_time > %s
    # """
    # cursor.execute(query, (start_date,))
    # res = cursor.fetchall()
    # total_matches = res[0][0]

    # # Add match_percentage to df
    # idx = 3
    # df.insert(loc=idx, column='match_percentage', value=[int((matches_played / total_matches)*100) for matches_played in df.matches_played])

    # # Add the win % to df
    # df['wins'] = df['wins'].div(df.matches_played, axis=0).mul(100)
    # df['wins'] = df['wins'].astype(float).round(1)
    # # df = df.round({'wins': 1, 'kills': 0, 'assists': 0, 'deaths': 0, 'adr': 0, 'kdr': 2, 'hltv': 2})

    # print(df[df['matches_played'] > 5].sort_values(by='wins', ascending=False).reset_index(drop=True))
        

import mysql.connector
from functions import load_api_keys
import json
import re
import datetime
import pandas as pd

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

def gather_keys(db, cursor, table_name: str) -> list:
    """
    Gather all primary keys from a specific table

    Args:
        db (mysql.connector.connect)    : The active database connection.
        cursor (mysql.connector.cursor) : The active cursor object.
        table_name (str)                : name of the table.
    
    Returns:
        all_keys (list): list of all keys.
        
        primary_keys (list): list of all primary keys.
    """
    global db_name

    query_all_keys = """
        SELECT 
            COLUMN_NAME
        FROM
            information_schema.COLUMNS
        WHERE
            TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
    """

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

    # Execute the query for all keys
    cursor.execute(query_all_keys, (db_name, table_name))
    all_keys_list = cursor.fetchall()

    # Execute the query for primary keys
    cursor.execute(query_primary_keys, (db_name, table_name))
    primary_keys_list = cursor.fetchall()

    # Extract column names from the key lists
    all_keys = [ak[0] for ak in all_keys_list]
    primary_keys = [pk[0] for pk in primary_keys_list]

    return all_keys, primary_keys

def upload_hub_data(batch_data_match: list, batch_data_player: list, batch_data_team: list):
    """ 
    Uploading the data dictionary of the hub matches to the database tables

    Args:
        batch_data_match (lst)  : List of dicts with match data
        batch_data_player (lst) : List of dicts with player data
        batch_data_team (lst)   : List of dicts with team data
    """
    
    db, cursor = start_database()

    try:
        # Get "matches" table query and tuple_data
        table_name = "matches"
        keys, primary_keys = gather_keys(db, cursor, table_name)
        sql_matches = upload_data_query(table_name, batch_data_match, keys, primary_keys)
        print(f"Created query for table: {table_name}")

        columns = list(batch_data_match[0].keys())
        batch_data_match = [tuple(d[col] for col in columns) for d in batch_data_match]
        
        # Get "teams" table query
        table_name = "teams"
        keys, primary_keys = gather_keys(db, cursor, table_name)
        sql_teams = upload_data_query(table_name, batch_data_match, keys, primary_keys)
        print(f"Created query for table: {table_name}")

        columns = list(batch_data_team[0].keys())
        batch_data_team = [tuple(d[col] for col in columns) for d in batch_data_team]

        # Get "player_statistics" table query
        table_name = "player_statistics"
        keys, primary_keys = gather_keys(db, cursor, table_name)
        sql_player_statistics = upload_data_query(table_name, batch_data_match, keys, primary_keys)
        print(f"Created query for table: {table_name}")

        columns = list(batch_data_player[0].keys())
        batch_data_player = [tuple(d[col] for col in columns) for d in batch_data_player]

        # Upload the batch data to the tables
        cursor.executemany(sql_matches, batch_data_match)
        print("Added the data to 'matches'")  
        cursor.executemany(sql_teams, batch_data_team)
        print("Added the data to 'teams'")  
        cursor.executemany(sql_player_statistics, batch_data_player)
        print("Added the data to 'player_statistics'")  

    finally:
        db.commit()
        close_database(db, cursor)


if __name__ == "__main__":
    # reset_database()
    
    db, cursor = start_database()

    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).timestamp()

    # query = """
    #     SELECT
    #         player_statistics.player_id,
    #         player_statistics.nickname,
    #         COUNT(player_statistics.match_id) AS total_matches,
    #         AVG(player_statistics.Kills) AS avg_kills,
    #         AVG(player_statistics.Assists) AS avg_assists,
    #         AVG(player_statistics.Deaths) AS avg_deaths,
    #         AVG(player_statistics.ADR) AS avg_adr,
    #         AVG(player_statistics.K_D_Ratio) AS avg_kdr
    #     FROM player_statistics
    #     JOIN matches ON matches.match_id = player_statistics.match_id
    #     WHERE matches.start_time > %s
    #     GROUP BY player_statistics.player_id
    #     ORDER BY avg_adr DESC;
    # """

    query = """
        SELECT
            p.player_id,
            (
                SELECT p2.nickname
                FROM player_statistics p2
                WHERE p2.player_id = p.player_id
                ORDER BY p2.match_id DESC
                LIMIT 1
            ) AS latest_nickname,

            COUNT(p.match_id) AS matches_played,
            AVG(p.Kills) AS avg_kills,
            AVG(p.Assists) AS avg_assists,
            AVG(p.Deaths) AS avg_deaths,
            AVG(p.ADR) AS avg_adr,
            AVG(p.K_D_Ratio) AS avg_kdr,
            SUM(p.Knife_Kills) AS knife_total,
            SUM(p.Zeus_Kills) AS zeus_total
        FROM player_statistics p
        JOIN matches ON matches.match_id = p.match_id
        WHERE matches.start_time > %s
        GROUP BY p.player_id
    """

    # Get player data into dataframe
    cursor.execute(query, (start_date,))
    res = cursor.fetchall()
    columns = ['player_id','nickname', 'matches_played', 'kills', "assists", 'deaths', 'adr','kdr', 'knife_kills', 'zeus_kills']
    df = pd.DataFrame(res, columns=columns)

    # Get total matches played
    query = """
        SELECT
            COUNT(DISTINCT(matches.match_id)) AS total_matches
        FROM matches
        WHERE matches.start_time > %s
    """
    cursor.execute(query, (start_date,))
    res = cursor.fetchall()
    total_matches = res[0][0]

    # Add match_percentage to df
    idx = 3
    # df['match_percentage'] = int( (df.matches_played / total_matches)*100 )
    df.insert(loc=idx, column='match_percentage', value=[int((matches_played / total_matches)*100) for matches_played in df.matches_played])

    print(df.sort_values(by='matches_played', ascending=False).reset_index(drop=True).to_string())
        

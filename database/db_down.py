import mysql.connector
import json
import re
import datetime
import pandas as pd
import numpy as np
from typing import *

from functions import load_api_keys
from data_processing import process_hub_data

from database.db_up import *
from database.db_manage import start_database, close_database

# Constants
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

def gather_hub_stats():
    """
    Gathers the hub stats from all player over a select time frame (for use in for example the discord bot)

    Args:
        None

    Returns:
        df
    """

    ## Start the database and cursor
    db, cursor = start_database()

    ## Set up the timeframe over which to gather the data
    start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).timestamp()

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
            SUM(p.result) AS wins,
            ROUND(AVG(p.Kills), 0) AS avg_kills,
            ROUND(AVG(p.Assists), 0) AS avg_assists,
            ROUND(AVG(p.Deaths), 0) AS avg_deaths,
            ROUND(AVG(p.ADR), 1) AS avg_adr,
            ROUND(AVG(p.K_D_Ratio), 2) AS avg_kdr,
            ROUND(AVG(p.hltv_rating), 2) AS avg_hltv,
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
    columns = ['player_id','nickname', 'matches_played', 'wins', 'kills', "assists", 'deaths', 'adr','kdr', 'hltv', 'knife_kills', 'zeus_kills']
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
    df.insert(loc=idx, column='match_percentage', value=[int((matches_played / total_matches)*100) for matches_played in df.matches_played])

    # Add the win % to df
    df['wins'] = df['wins'].div(df.matches_played, axis=0).mul(100)
    df['wins'] = df['wins'].astype(float).round(1)
    
    df = df[df['matches_played'] > 10].reset_index(drop=True)
    # print(df[df['matches_played'] > 5].sort_values(by='wins', ascending=False).reset_index(drop=True))

    return df


if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

    
    df = gather_hub_stats()

    # print(df[df['matches_played'] > 5].sort_values(by='wins', ascending=False).reset_index(drop=True))

    # print(df.nlargest(n=5, columns='matches_played'))

    matches_played = df.nlargest(n=5, columns='matches_played')[['nickname', 'matches_played']]
    print(matches_played)
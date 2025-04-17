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

from database.db_up import *
from database.db_manage import start_database, close_database

def gather_hub_stats(days=30):
    """
    Gathers the hub stats from all player over a select time frame (for use in for example the discord bot)

    Args:
        days (int): Amount of days to gather the data from (default = 30)
            - Can be 'ALL' in which case all data is being gotten

    Returns:
        df
    """
     
    ## Start the database and cursor
    db, cursor = start_database()

    ## Set up the timeframe over which to gather the data
    if days == 'ALL':
        start_date = 0
    else:
        start_date = (datetime.now() - timedelta(days=days)).timestamp()

    query = f"""
        SELECT
            p.player_id,
            (
                SELECT p2.player_name
                FROM hub_player_stats p2
                WHERE p2.player_id = p.player_id
                ORDER BY p.match_id, p.match_round DESC
                LIMIT 1
            ) AS last_name,
            
            p3.country,
            COUNT(DISTINCT CONCAT(m.match_id, '-', m.match_round)) AS maps_played,
            SUM(p.result) AS wins,
            ROUND(AVG(p.Kills), 0) AS avg_kills,
            ROUND(AVG(p.Assists), 0) AS avg_assists,
            ROUND(AVG(p.Deaths), 0) AS avg_deaths,
            ROUND(AVG(p.ADR), 1) AS avg_adr,
            ROUND(AVG(p.K_D_Ratio), 2) AS avg_kdr,
            ROUND(AVG(p.hltv_rating), 2) AS avg_hltv,
            SUM(p.Knife_Kills) AS knife_total,
            SUM(p.Zeus_Kills) AS zeus_total
            
        FROM hub_player_stats p
        JOIN hub_maps m ON (m.match_id, m.match_round) = (p.match_id, p.match_round)
        JOIN hub_matches m2 ON m2.match_id = m.match_id
        JOIN hub_players p3 ON p3.player_id = p.player_id
        WHERE m2.match_time > %s
        GROUP BY p.player_id, last_name
        ORDER BY p.player_id
    """

    # Get player data into dataframe
    cursor.execute(query, (start_date,))
    res = cursor.fetchall()
    columns = ['player_id','player_name', 'country', 'matches_played', 'wins', 'kills', "assists", 'deaths', 'adr','kdr', 'hltv', 'knife_kills', 'zeus_kills']
    df = pd.DataFrame(res, columns=columns)

    # Get total matches played
    query = f"""
        SELECT
            COUNT(DISTINCT CONCAT(m.match_id, '-', m.match_round)) AS total_matches
        FROM hub_maps m
        JOIN hub_matches m2 ON m2.match_id = m.match_id
        WHERE m2.match_time > %s
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
    
    return df

def gather_esea_stats(season: int|list) -> pd.DataFrame:
    """
    Gathers the hub stats from all player over a select time frame (for use in for example the discord bot)

    Args:
        season (int | list): The esea season(s) to gather stats from

    Returns:
        df
    """
    
    # Preparing the sql
    if isinstance(season, int):
        seasons = (season,)
    
    seasons_sql = ", ".join(map(str, seasons))
    
    ## Start the database and cursor
    db, cursor = start_database()

    query = f"""
        SELECT
            p.player_id,
            (
                SELECT p2.player_name
                FROM esea_player_stats p2
                WHERE p2.player_id = p.player_id
                ORDER BY p.match_id, p.match_round DESC
                LIMIT 1
            ) AS last_name,
            
            p3.country,
            COUNT(DISTINCT CONCAT(m.match_id, '-', m.match_round)) AS maps_played,
            SUM(p.result) AS wins,
            ROUND(AVG(p.Kills), 0) AS avg_kills,
            ROUND(AVG(p.Assists), 0) AS avg_assists,
            ROUND(AVG(p.Deaths), 0) AS avg_deaths,
            ROUND(AVG(p.ADR), 1) AS avg_adr,
            ROUND(AVG(p.K_D_Ratio), 2) AS avg_kdr,
            ROUND(AVG(p.hltv_rating), 2) AS avg_hltv,
            SUM(p.Knife_Kills) AS knife_total,
            SUM(p.Zeus_Kills) AS zeus_total
            
        FROM esea_player_stats p
        JOIN esea_maps m ON (m.match_id, m.match_round) = (p.match_id, p.match_round)
        JOIN esea_matches m2 ON m2.match_id = m.match_id
        JOIN esea_players p3 ON p3.player_id = p.player_id
        JOIN esea_seasons s ON s.season_id = m2.season_id
        WHERE p3.country IN ('nl', 'be', 'lu') AND s.season_number in ({seasons_sql})
        GROUP BY p.player_id, last_name
        ORDER BY p.player_id
    """
    
    cursor.execute(query)
    res = cursor.fetchall()
    
    columns = ['player_id','player_name', 'country', 'matches_played', 'wins', 'kills', "assists", 'deaths', 'adr','kdr', 'hltv', 'knife_kills', 'zeus_kills']
    df = pd.DataFrame(res, columns=columns)
    
    return df

def gather_players_country() -> pd.DataFrame:
    """
    Gathers the players country from the database

    Returns:
        df
    """
    
    ## Start the database and cursor
    db, cursor = start_database()

    query = f"""
        SELECT *      
        FROM players_country p
    """

    # Get player data into dataframe
    cursor.execute(query)
    res = cursor.fetchall()
    columns = ['player_id','player_name', 'country']
    df = pd.DataFrame(res, columns=columns)

    return df



if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    # db,cursor = start_database()
    
    # Gather data for hub
    df_hub = gather_hub_stats(days=30)
    df_hub = df_hub.sort_values(by=['hltv'], ascending=False).reset_index(drop=True)
    
    # # Gather data for esea
    # df_esea = gather_esea_stats(season=50)
    # df_esea = df_esea.sort_values(by=['hltv'], ascending=False).reset_index(drop=True)
    
    
    print(df_hub.to_string())
    
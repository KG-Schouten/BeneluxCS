# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import os
import random
import pandas as pd
from datetime import datetime, timedelta
import re
import time
from typing import Dict, Any, Callable, Optional

# AI imports
from model.model_utils import load_model, predict, preprocess_data

# API imports
import asyncio
from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1
from faceit_api.sliding_window import RequestDispatcher
from faceit_api.async_progress import gather_with_progress

# dp imports
from data_processing.dp_general import *

# function imports
from functions import load_api_keys

## Data paths
team_data_path = "C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"

async def process_hub_data(competition_id: str, items_to_return: int|str=100) -> None:
    """
    The main function to process the data for a hub
    
    Args:
        competition_id (str): The ID of the hub to process data for
        items_to_return (int | str): Specifies the amount of matches that will be gathered. It can be: (default=100) 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
    Returns:
        tuple:
            - df_matches: DataFrame containing match data
            - df_teams_matches: DataFrame containing team match data
            - df_teams: DataFrame containing team details
            - df_maps: DataFrame containing map data
            - df_teams_maps: DataFrame containing team map data
            - df_players_stats: DataFrame containing player statistics
            - df_hub_players: DataFrame containing player details
    """
    print(
    f"""
        
    -------------------------------------
        Processing Hub Data:
    -------------------------------------
    
    """
    )
    async with RequestDispatcher(request_limit=350, interval=10, concurrency=5) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        # Initialize the FaceitData object with the API token and dispatcher
        api_keys = load_api_keys()
        faceit_data = FaceitData(api_keys.get("FACEIT_TOKEN"), dispatcher)
        faceit_data_v1 = FaceitData_v1(dispatcher)
        
        ## Dataframe with the matches and teams that played in these matches (df_matches, df_teams_matches)
        print("Gathering hub matches...")
        df_matches, df_teams_matches = await gather_hub_matches(competition_id, faceit_data=faceit_data)
        
        if items_to_return != "ALL":
            df_matches = df_matches.head(items_to_return)
            df_teams_matches = df_teams_matches[df_teams_matches['match_id'].isin(df_matches['match_id'])]
        
        ## Dataframe with team details (df_teams)
        print("Gathering hub team details...")
        team_ids = list(df_teams_matches['team_id'].unique())
        df_teams = await process_team_details_batch(team_ids, faceit_data=faceit_data)
        
        ## Dataframes with the maps, teams in the maps and player stats (df_maps, df_teams_maps, df_players_stats)
        print("Gathering hub match and player stats...")
        match_ids = df_matches['match_id'].unique().tolist()
        df_maps, df_teams_maps, df_players_stats = await process_match_stats_batch(match_ids, faceit_data=faceit_data)
        
        ## Dataframe with the players that played in the matches (df_players)
        print("Gathering hub player details...")
        player_ids = df_players_stats['player_id'].unique().tolist()
        df_hub_players = await process_player_details_batch(player_ids, faceit_data_v1=faceit_data_v1)
        
    return df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_hub_players

async def gather_hub_matches(competition_id: str, faceit_data: FaceitData):
    """
    Gathers all the hub matches from a specific hub
    
    Args:
        competition_id (str): The ID of the hub to gather matches from
        faceit_data (FaceitData): The FaceitData object to use for API calls
    
    Returns:
        tuple:
            - df_matches: DataFrame containing match data
            - df_teams_matches: DataFrame containing team match data

    """
    tasks = [faceit_data.hub_matches(competition_id, starting_item_position=i, return_items=100) for i in range(0, 1000, 100)]
    results = await gather_with_progress(tasks, desc="Fetching matches", unit='match')
    extracted_matches = [match for result in results if isinstance(result, dict) and result['items'] for match in result['items']] \
    
    ## Getting df_hub_matches and df_hub_teams_matches
    match_list, team_match_list = [], []
    for match in extracted_matches:
        if match.get('status') != "CANCELLED":
            match_id = match.get('match_id')
            # Get winning team id
            winning_fac = match['results'].get('winner', None)
            winning_id = match['teams'][winning_fac]['faction_id']
            
            # Create a dictionary for the match
            match_dict = {
                "match_id": match_id,
                "competition_id": match.get('competition_id', None),
                "competition_type": match.get('competition_type', None),
                "competition_name": match.get('competition_name', None),
                "organizer_id": match.get('organizer_id', None),
                "match_time": match.get('configured_at', None),
                "demo_url": match.get('demo_url', None),
                "best_of": match.get('best_of', None),
                "winner": winning_id,
                "status": match.get('status', None),          
            }
            match_list.append(match_dict)
            
            for team in match['teams'].values():
                team_id = team.get('faction_id')
                
                team_match_list.append(
                    {
                        "team_id": team_id,
                        "match_id": match_id,
                    }
                )        
    df_matches = pd.DataFrame(match_list)
    df_teams_matches = pd.DataFrame(team_match_list)
    
    return df_matches, df_teams_matches

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
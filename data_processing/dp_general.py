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

# function imports
from functions import load_api_keys

## Data paths
team_data_path = "C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"

def modify_keys(d):
    """
    Modifies the keys in a dictionary to make sure a MySQL database can handle the keys as column headers

    Args:
        d (dict | list): The data to be processed. Can be:
            - A dict
            - A list of dict

    Returns:
        Dictionary with modified keys (if input is not a dict it will return a dict back)
    """
    ## Check if the input is a dict
    if isinstance(d, list):
        d_list = []
        for item in d:
            if isinstance(item, dict):
                # Create a new dict with modified keys
                d_item = {
                    re.sub(r'[^a-zA-Z0-9_]', '_', key): modify_keys(value) if isinstance(value, dict) else value 
                    for key, value in item.items()
                }
                d_list.append(d_item)
        return d_list
    elif isinstance(d, dict):
        # Create a new dict with modified keys
        return {
            re.sub(r'[^a-zA-Z0-9_]', '_', key): modify_keys(value) if isinstance(value, dict) else value 
            for key, value in d.items()
        }
    # Returns value of d if not a dict
    else: 
        print("Input was not a dict so returned it")
        return d

async def fetch_and_check_player_details(player_id: str, check: bool=True):
    """
    Helper function to gather player details and check Benelux status concurrently.
    
    Args:
        player_id (str): Faceit player ID
        check (bool): True if you want to check if the players are benelux or not
    
    Returns:
        player_item (dict): A dictionary containing player details, or None if the player is not Benelux
    """
    # Fetch player details concurrently
    player_details = await faceit_data.player_id_details(player_id)
    
    player_name = player_details.get('nickname', None)
    country = player_details.get('country', None)
    avatar = player_details.get('avatar', None)
    elo = player_details['games']['cs2'].get('faceit_elo')
    
    if check:
        # Check Benelux status concurrently
        check_bnlx = await check_benelux(player_id, player_name, country)  # Use player_id as nickname if player_name is not provided
    
        # If the player is Benelux, create the player entry
        benelux_codes = ['nl', 'be', 'lu']
        if (check_bnlx[2] and (country not in benelux_codes)): # If player is benelux and fake flagging
            new_country = check_bnlx[5] 
            print(f"{player_name}'s country flag changed from '{country}' to '{new_country}'")
            country = new_country      
        elif ((not check_bnlx[2]) and (country in benelux_codes)):  # If player is not benelux and fakeflagging benelux
            new_country = check_bnlx[5] 
            print(f"{player_name}'s country flag changed from '{country}' to '{new_country}'")
            country = new_country
    
    player_item = {
        'player_id': player_id,
        'nickname': player_details.get('nickname', ''),
        'country': country,
        'elo': elo,
        'avatar': avatar
    }

    return player_item

### -----------------------------------------------------------------
### Match processing functions
### -----------------------------------------------------------------

async def process_match_stats_batch(match_ids, faceit_data: FaceitData) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    """
    Processes match stats for a batch of match IDs.
    
    Args:
        match_ids (list): List of match IDs to process
        faceit_data (FaceitData): FaceitData object for API calls
    
    Returns:
        tuple:
            - df_maps (pd.DataFrame): DataFrame containing map details
            - df_teams_maps (pd.DataFrame): DataFrame containing team stats per map
            - df_players_stats (pd.DataFrame): DataFrame containing player stats per map
    """
    tasks = [process_match_stats(match_id, faceit_data) for match_id in match_ids]
    results = await gather_with_progress(tasks, desc="Processing match stats", unit="matches")
    
    df_maps = pd.DataFrame([item for row in results if row is not None for item in row[0]])
    df_teams_maps = pd.DataFrame([item for row in results if row is not None for item in row[1]])
    df_players_stats = pd.DataFrame([item for row in results if row is not None for item in row[2]])
    
    return df_maps, df_teams_maps, df_players_stats

async def process_match_stats(match_id, faceit_data: FaceitData) -> tuple[list[Dict], list[Dict], list[Dict]]:
    try:
        match_stats = await faceit_data.match_stats(match_id)
        
        if match_stats == 404:
            return None
        else:
            map_dict, team_map_dict, player_stats_dict = await process_match_stats_dict(match_id, match_stats)
            return map_dict, team_map_dict, player_stats_dict
    
    except Exception as e:
        print(f"Error processing match ID {match_id}: {e}")
        return None

async def process_match_stats_dict(match_id, match_stats):
    """ 
    Processing of the match stat dictionary gathered from the v4 faceit_data.match_stats() function
    
    Args:
        match_id (str): The match ID
        match_stats (dict): The match stats dictionary
    
    Returns:
        map_list (list): A list of dictionaries containing map details
        team_map_list (list): A list of dictionaries containing team stats per map
        player_stats_list (list): A list of dictionaries containing player stats per map
    """
    # Initialize the lists
    map_list, team_map_list, player_stats_list = [], [], []
    
    for map in match_stats['rounds']:
        ## Gather map details
        match_round = map.get("match_round", None)
        
        sorted_keys = sorted(map['round_stats'].keys())
        map_dict = {
            "match_id": match_id,
            "match_round": match_round,
            "best_of": map.get("best_of", None),
            **{key: map['round_stats'][key] for key in sorted_keys}
        }
        map_list.append(map_dict)
        
        for team in map['teams']:
            team_id = team.get("team_id")   
            ## Gather team stats per map
            sorted_keys = sorted(team['team_stats'].keys())
            team_map_dict = {
                "match_id": match_id,
                "match_round": match_round,
                "team_id": team_id,
                **{key: team['team_stats'][key] for key in sorted_keys}
            }
            team_map_list.append(team_map_dict)
            
            for player in team['players']:
                ## Gather player stats per map
                player_id = player.get("player_id", None)
                player_name = player.get("nickname", None)

                sorted_keys = sorted(player['player_stats'].keys())
                player_stats_dict = {
                    "player_id": player_id,
                    "player_name": player_name,
                    "team_id": team_id,
                    "match_id": match_id,
                    "match_round": match_round,
                    **{key: player['player_stats'][key] for key in sorted_keys}
                }
                player_stats_list.append(player_stats_dict)
    
    return map_list, team_map_list, player_stats_list

### -----------------------------------------------------------------
### Player details functions
### -----------------------------------------------------------------

async def process_player_details_batch(player_ids: list[str], faceit_data_v1: FaceitData_v1) -> pd.DataFrame:
    """
    Processes player details for a batch of player IDs.
    
    Args:
        player_ids (list): List of player IDs to process
        faceit_data_v1 (FaceitData_v1): FaceitData_v1 object for API calls
        
    Returns:
        df_players (pd.DataFrame): DataFrame containing player details
    """
    batch_size = 15  # Adjust the batch size as needed
    tasks = [
        process_player_details(list(player_ids[i:i + batch_size]), faceit_data_v1=faceit_data_v1)
        for i in range(0, len(player_ids), batch_size)
    ]
    results = await gather_with_progress(tasks, desc="Processing player details", unit="players")

    # Flatten the list of lists into a single list
    player_list = [player for sublist in results for player in sublist if player is not None]
    
    df_players = pd.DataFrame(player_list)
    return df_players

async def process_player_details(player_ids: list[str], faceit_data_v1: FaceitData_v1) -> dict:
    try:
        player_details = await faceit_data_v1.player_details_batch(list(player_ids))
        if player_details == 404:
            return None
        else:
            player_list = []
            for player_id, player in player_details['payload'].items():
                player_dict = {
                    'player_id': player_id,
                    'nickname': player.get('nickname', None),
                    'avatar': player.get('avatar', None),
                    'country': player.get('country', None),
                    'faceit_elo': next((g.get('elo') for g in player.get('games', []) if g.get('game') == 'cs2'), None),
                    'faceit_level': next((g.get('skill_level') for g in player.get('games', []) if g.get('game') == 'cs2'), None),
                    'steam_id_64': player.get('steam_id_64', None),
                    'memberships': player.get('memberships', None),
                }
                player_list.append(player_dict)
 
            return player_list
        
    except Exception as e:
        print(f"Error processing player IDs {player_ids}: {e}")
        return None

### -----------------------------------------------------------------
### Team details functions
### -----------------------------------------------------------------

async def process_team_details_batch(team_ids: list[str], faceit_data: FaceitData) -> pd.DataFrame:
    """
    Processes team details for a batch of team IDs.
    
    Args:
        team_ids (list): List of team IDs to process
        faceit_data (FaceitData): FaceitData object for API calls
        
    Returns:
        df_teams (pd.DataFrame): DataFrame containing team details
    """
    tasks = [process_team_details(team_id, faceit_data=faceit_data) for team_id in team_ids]
    results = await gather_with_progress(tasks, desc="Processing team details", unit="teams")

    team_list = results
    
    df_teams = pd.DataFrame(team_list)
    return df_teams

async def process_team_details(team_id, faceit_data: FaceitData) -> dict:
    try:
        team_details = await faceit_data.team_details(team_id)
        if team_details == 404:
            team_dict = {
                'team_id': team_id,
                'name': None,
                'nickname': None,
                'avatar': None,
            }
        else:
            team_dict = {
                'team_id': team_details['team_id'],
                'name': team_details.get('name', None),
                'nickname': team_details.get('nickname', None),
                'avatar': team_details.get('avatar', None),
            }
        return team_dict
    
    except Exception as e:
        print(f"Error processing team ID {team_id}: {e}")
        return None

### -----------------------------------------------------------------
### Event data functions
### -----------------------------------------------------------------

def process_event_data(event_id, event_name, event_start_time, event_end_time, event_description, event_type, event_region, stages):
    """
    Process the event data and return a dictionary with the processed data.
    
    Args:
        event_id (str): The ID of the event.
        event_name (str): The name of the event.
        event_start_time (int): The start time of the event in UNIX timestamp.
        event_end_time (int): The end time of the event in UNIX timestamp.
        event_description (str): The description of the event.
        event_type (str): The type of the event (ESEA, TOURNAMENT, HUB, LAN).
        event_region (str): The region of the event (Europe, North America, South America, Asia, Oceania).
        stages (dict): A dictionary containing stage IDs and names.
    """
    # Process the event data here
    event_dict = {
        'event_id' : event_id,
        'event_name' : event_name,
        'event_start_time' : event_start_time,
        'event_end_time' : event_end_time,
        'event_description' : event_description,
        'event_type' : event_type,
        'event_region' : event_region
    }
    
    stage_list = []
    for stage_id, stage_name in stages.items():
        stage_list.append(
            {
                'event_id': event_id,
                'stage_id': stage_id,
                'stage_name': stage_name
            }
        )
    
    return event_dict, stage_list

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
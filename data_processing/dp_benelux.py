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
import asyncio

# API imports
from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1
from functions import load_api_keys

## Data paths
team_data_path = "C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"

async def check_all_players(player_ids: list, player_names: list, PRINT: bool=False):
    """
    Small wrapper to get result of a list of player_ids
    
    Args:
        player_ids (list): A list of player IDs
        player_names (list): A list of player names
        PRINT (bool): True if you want to print confirmation of benelux (default=False)

    Returns:
        pd.Dataframe: A dataframe containing the results of the benelux check for each player being:
            - player_id (str): faceit player id
            - player_name (str): player nickname
            - bnlx_country (str): The most common benelux country from friendlist
            - all_country (str): The most common country from friendlist
            - benelux_sum (int): The number of friends from Benelux
            - friend_frac (float): The fraction of friends from Benelux
            - is_benelux_hub (bool): True if the player is in a hub with benelux country requirements
            - InHub (bool): True if the player is in the Benelux hub
    """
    tasks = [check_benelux(player_id, player_name, PRINT) for player_id, player_name in zip(player_ids, player_names)]
    results = await asyncio.gather(*tasks) # Run all tasks concurrently
    
    # Convert results to a dataframe
    columns = ['player_id', 'player_name', 'bnlx_country', 'all_country', 'benelux_sum', 'friend_frac', 'is_benelux_hub', 'InHub']
    df_results = pd.DataFrame(results, columns=columns)
    
    return df_results

async def check_benelux(player_id: str, player_name: str, PRINT: bool=False):
    """
    General code block for checking if a player is benelux
    
    Args:
        player_id (str): faceit player id
        player_name (str): player nickname
        PRINT (bool): True if you want to print confirmation of benelux (default=False)
        
    Returns:
        tuple: (
            player_id (str), 
            player_name (str),
            bnlx_country (str),
            all_country (str),
            benelux_sum (int),
            friend_frac (float),
            is_benelux_hub (bool),
            InHub (bool)
            )
    """ 
    # Faceit friendslist check
    check_friends_tuple  = await check_friends_faceit(player_id, player_name, PRINT)
    
    if check_friends_tuple is None:
        return player_id, player_name, None, None, None, None, None, None
    else:
        bnlx_country, all_country, benelux_sum, friend_frac = check_friends_tuple
    
    # Faceit hub requirements and Benelux hub check
    is_benelux_hub, InHub = await check_benelux_hub(player_id)
    
    return player_id, player_name, bnlx_country, all_country, benelux_sum, friend_frac, is_benelux_hub, InHub 

async def check_friends_faceit(player_id: str, player_name: str, PRINT: bool=False):
    """
    Function to check if a player is benelux based on their faceit friendslist
    
    Args:
        player_id (str): faceit player id
        player_name (str): player nickname
        PRINT (bool): True if you want to print confirmation of benelux (default=False)
        
    Returns:
        tuple: (
            bnlx_country (str),
            all_country (str),
            benelux_sum (int),
            friend_frac (float),
            is_benelux_hub (bool),
            InHub (bool)
            )
    """
    # Constants
    benelux_codes = ['nl', 'be', 'lu']
    # Get friend ids
    df_friends = await get_friend_list_faceit(player_id, player_name)
    
    if df_friends is not None:
        country_count = df_friends.get('country', None)
        if country_count is None:
            return None, None, None, None
        
        country_count = country_count.value_counts()
        if country_count is None:
            return None, None, None, None
        benelux_sum = sum(country_count.get(country,0) for country in benelux_codes)
        friend_frac = int(benelux_sum)/int(len(df_friends))
        
        # Determine most common benelux country from friendlist
        bnlx_count = country_count.reindex(benelux_codes).dropna()
        bnlx_country = bnlx_count.idxmax() if not bnlx_count.empty else None
        
        # Determine the most common country from friendlist
        all_country = country_count.idxmax() if not country_count.empty else None
            
        if PRINT:
            print(f"{player_name} has {benelux_sum} friends from Benelux: {friend_frac}")
            
        return bnlx_country, all_country, benelux_sum, friend_frac
    else:
        print(f"Friend list for {player_name} is private or not available.")
        return None # Returns none if the friend list is private

async def get_friend_list_faceit(player_id: str, player_name: str = None) -> pd.DataFrame:
    try:
        friend_list = []
        batch_start = 0
        batch = 100
        # Keep looping until all friends in friendlist have been appended
        while len(friend_list) == batch_start:
            data = await faceit_data_v1.player_friend_list(player_id, starting_item_position=batch_start, return_items=batch)
            data = data['payload']['results']

            # Add the new data to the friend list
            friend_list.extend(data)
            
            batch_start += batch
                
        df = pd.DataFrame.from_dict(friend_list)
        return df
    except Exception as e:
        print(f"Exception while loading faceit friend list for {player_name}: {e}")
        return None

async def check_benelux_hub(player_id) -> bool:
    """
    Function to check if a player is in a hub with benelux country requirements
    Args:
        player_id (str): faceit player id
    
    Returns:
        tuple: (is_benelux_hub (bool), InHub (bool))
        
    """
    results = []
    
    benelux_countries = {'BE', 'NL', 'LU'}
    max_whitelist_size = 5 # Maximum size of the whitelist
    
    # Gather the location requirements of the hubs that someone is a member of
    response = await faceit_data_v1.player_hubs(player_id, return_items=int(20))
    hubs = [hub['competition'] for hub in response['payload']['items']]
    
    for hub in hubs:        
        # Check if the BeneluxHub is part of the hubs
        if hub.get('guid') == '3e549ae1-d6a7-47d4-98cd-a6077a4da07c':
            InHub = True
        else:
            InHub = False
        
        # Check if the whitelist contains benelux countries
        whitelist = hub.get('whitelistGeoCountries', [])
        
        if whitelist is None:
            is_benelux_hub = None
        else:
            # Check if the whitelist contains any benelux countries
            contains_benelux = any(country in benelux_countries for country in whitelist)
        
            if contains_benelux:
                if len(whitelist) >= max_whitelist_size:
                    is_benelux_hub = None
                else:
                    is_benelux_hub = True
            else:
                is_benelux_hub = False
        
        results.append([is_benelux_hub, InHub])

    # For is_benelux_hub: If any true -> true, if none true and any false -> false, if all none -> None
    is_benelux_hub = None if all(result[0] is None for result in results) else any(result[0] for result in results)
    InHub = any(result[1] for result in results)  # Check if any of the InHub results are True
    
    # print(f"{player_id}: {is_benelux_hub, InHub} for: {results}")
    
    return is_benelux_hub, InHub

async def get_hub_players(hub_id: str = '3e549ae1-d6a7-47d4-98cd-a6077a4da07c') -> list:
    """
    Function to get all the players playing in the benelux hub
    
    Args:
        hub_id (str): The ID of the hub to get the players from (default is the benelux hub)
    Returns:
        list: A list of player IDs in the hub
    """
    hub_ids = []
    batch_start = 0
    batch_size = 50
    
    try:
        while len(hub_ids) == batch_start:
            data = await faceit_data.hub_members(hub_id, starting_item_position=batch_start, return_items=batch_size)

            hub_ids.extend([player['user_id'] for player in data['items']])

            batch_start += batch_size
    except Exception as e:
        print(f"Exception while loading hub players: {e}")
    
    return hub_ids
 
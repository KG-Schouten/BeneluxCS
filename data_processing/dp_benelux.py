# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

# AI imports
from data_processing.model.model_utils import load_model, predict, preprocess_data

# API imports
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.sliding_window import RequestDispatcher, request_limit, interval, concurrency
from data_processing.faceit_api.async_progress import gather_with_progress

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")

async def process_player_country_details(player_ids: list) -> pd.DataFrame:
    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        # Initialize the FaceitData object with the API token and dispatcher
        api_keys = load_api_keys()
        faceit_data = FaceitData(api_keys.get("FACEIT_TOKEN"), dispatcher)
        faceit_data_v1 = FaceitData_v1(dispatcher)
        
        tasks = [check_benelux(player_id, faceit_data_v1) for player_id in player_ids]
        results = await gather_with_progress(tasks, desc="checking players", unit="players")
        
        df_players_country_details = pd.DataFrame(results)
        
    return df_players_country_details

async def check_benelux(player_id: str, faceit_data_v1: FaceitData_v1) -> tuple:
    """
    General code block for checking if a player is benelux
    
    Args:
        player_id (str): faceit player id
        
    Returns:
        dict: {
            'player_id': player_id,
            'bnlx_country': bnlx_country,
            'all_country': all_country,
            'benelux_sum': benelux_sum,
            'friend_frac': friend_frac,
            'is_benelux_hub': is_benelux_hub,
            'InHub': InHub
        }
    """ 
    # Faceit friendslist check
    results_check_friends_faceit = await check_friends_faceit(player_id, faceit_data_v1=faceit_data_v1)
    
    if results_check_friends_faceit is None:
        bnlx_country, all_country, benelux_sum, friend_frac = None, None, None, None
    else:
        bnlx_country, all_country, benelux_sum, friend_frac = results_check_friends_faceit
    
    # Faceit hub requirements and Benelux hub check
    is_benelux_hub, InHub = await check_benelux_hub(player_id, faceit_data_v1=faceit_data_v1)
    
    benelux_dict = {
        'player_id': player_id,
        'bnlx_country': bnlx_country,
        'all_country': all_country,
        'benelux_sum': benelux_sum,
        'friend_frac': friend_frac,
        'is_benelux_hub': is_benelux_hub,
        'InHub': InHub
    }
    
    return benelux_dict

async def check_friends_faceit(player_id: str, faceit_data_v1: FaceitData_v1):
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
    df_friends = await get_friend_list_faceit(player_id, faceit_data_v1)
    
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
   
        return bnlx_country, all_country, benelux_sum, friend_frac
    else:
        print(f"Friend list for {player_id} is private or not available.")
        return None # Returns none if the friend list is private

async def get_friend_list_faceit(player_id: str, faceit_data_v1: FaceitData_v1) -> pd.DataFrame:
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
        print(f"Exception while loading faceit friend list for {player_id}: {e}")
        return None

async def check_benelux_hub(player_id, faceit_data_v1: FaceitData_v1) -> bool:
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

async def get_hub_players(faceit_data: FaceitData_v1) -> list:
    """
    Function to get all the players playing in the benelux hub
    
    Args:
        hub_id (str): The ID of the hub to get the players from (default is the benelux hub)
    Returns:
        list: A list of player IDs in the hub
    """
    hub_id = '3e549ae1-d6a7-47d4-98cd-a6077a4da07c' # For benelux hub
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

def update_player_country(df_players: pd.DataFrame, df_players_country: pd.DataFrame) -> pd.DataFrame:
    """
    Updates the df_players DataFrame with the player country details from a dataframe df_players_country (either from the data/players_country.json file or from the database)
    """
    
    df_merged = df_players.merge(
        df_players_country,
        on='player_id',
        how='left',
        suffixes=('', '_new')
    )

    for col in df_merged.columns:
        if col.endswith('_new'):
            # Check if the column is in the original dataframe
            original_col = col[:-4]  # Remove '_new' suffix to get the original column name
            if original_col in df_merged.columns:
                # Combine the new column with the original column, keeping non-null values from the new column
                df_merged[original_col] = df_merged[original_col].combine_first(df_merged[col])
                # Drop the new column
                df_merged.drop(columns=[col], inplace=True)
            else:
                # If the original column doesn't exist, just rename the new column to the original name
                df_merged.rename(columns={col: original_col}, inplace=True)
        else:
            # If the column doesn't end with '_new', just keep it as is
            df_merged.rename(columns={col: col}, inplace=True)

    df_players = df_merged.copy()
    
    return df_players
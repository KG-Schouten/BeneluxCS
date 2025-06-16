# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import json
import asyncio

# API imports
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.sliding_window import RequestDispatcher, request_limit, interval, concurrency
from data_processing.faceit_api.async_progress import gather_with_progress
from data_processing.faceit_api.logging_config import function_logger

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")

async def process_player_country_details(player_ids: list) -> pd.DataFrame:
    """
    Function to process player country details
    """
    
    semaphore = asyncio.Semaphore(concurrency)  # Limit the number of concurrent requests
    
    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        async with FaceitData_v1(dispatcher) as faceit_data_v1:
            
            async def bounded_check(player_id: str) -> dict:
                async with semaphore:
                    return await check_benelux(player_id, faceit_data_v1)
            
            tasks = [bounded_check(player_id) for player_id in player_ids]
            results = await gather_with_progress(tasks, desc="checking players", unit="players")
            
            df_players_country_details = pd.DataFrame(filter(None, results)) # skip empty results
            

            # Remove rows where 'player_id' is None or empty
            if df_players_country_details.empty:
                function_logger.warning("No valid player data found. Returning an empty DataFrame.")
                return pd.DataFrame()
            
            df_players_country_details = df_players_country_details[df_players_country_details['player_id'].notna() & (df_players_country_details['player_id'] != '')]
            
    return df_players_country_details

async def check_benelux(player_id: str, faceit_data_v1: FaceitData_v1) -> dict:
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
    results_check_friends_faceit, results_check_benelux_hub = await asyncio.gather(
        check_friends_faceit(player_id, faceit_data_v1=faceit_data_v1),
        check_benelux_hub(player_id, faceit_data_v1=faceit_data_v1)
    )
    
    # # Faceit friendslist check
    # results_check_friends_faceit = await check_friends_faceit(player_id, faceit_data_v1=faceit_data_v1)
    
    if results_check_friends_faceit:
        # If the results are not empty, unpack the values
        bnlx_country, all_country, benelux_sum, friend_frac = results_check_friends_faceit
    else:
        function_logger.info(f"{player_id} is missing data from the friendslist check. Skipping player and returning empty values.")
        return {}
    
    # Faceit hub requirements and Benelux hub check
    # results_check_benelux_hub = await check_benelux_hub(player_id, faceit_data_v1=faceit_data_v1)
    if results_check_benelux_hub:
        # If the results are not empty, unpack the values
        is_benelux_hub, InHub = results_check_benelux_hub
    else:
        function_logger.info(f"{player_id} is missing data from the hub check. Skipping player and returning empty values.")
        return {}
    
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

async def check_friends_faceit(player_id: str, faceit_data_v1: FaceitData_v1) -> tuple:
    """
    Function to check if a player is benelux based on their faceit friendslist
    
    Args:
        player_id (str): faceit player id
        
    Returns:
        tuple: (
            bnlx_country (str),
            all_country (str),
            benelux_sum (int),
            friend_frac (float),
            )
    """
    # Constants
    benelux_codes = ['nl', 'be', 'lu']
    # Get friend ids
    df_friends = await get_friend_list_faceit(player_id, faceit_data_v1)
    
    if df_friends.empty:
        return ()
    try:
        country_column = df_friends.get('country', None)
        if country_column is None:
            function_logger.warning(f"No country data found for player {player_id}.")
            return ()
        
        country_count = country_column.value_counts()
        if country_count is None:
            function_logger.warning(f"No country counts found for player {player_id}.")
            return ()
        
        benelux_sum = sum(country_count.get(country,0) for country in benelux_codes)
        friend_frac = int(benelux_sum)/int(len(df_friends))
        
        # Determine most common benelux country from friendlist
        bnlx_count = country_count.reindex(benelux_codes).dropna()
        bnlx_country = bnlx_count.idxmax() if not bnlx_count.empty else None
        
        # Determine the most common country from friendlist
        all_country = country_count.idxmax() if not country_count.empty else None

        return bnlx_country, all_country, benelux_sum, friend_frac
    except Exception as e:
        print(f"Exception while checking friends for {player_id}: {e}")
        return ()

async def get_friend_list_faceit(player_id: str, faceit_data_v1: FaceitData_v1) -> pd.DataFrame:
    try:
        friend_list = []
        batch_start = 0
        batch = 100
        # Keep looping until all friends in friendlist have been appended
        while True:
            data = await faceit_data_v1.player_friend_list(player_id, starting_item_position=batch_start, return_items=batch)
            
            if isinstance(data, dict) and 'payload' in data and 'results' in data['payload']:
                data = data['payload']['results']
            else:
                function_logger.warning(f"Unexpected data format for player {player_id}: {data}")
                return pd.DataFrame()  # Return empty DataFrame if data format is unexpected
            
            # Add the new data to the friend list
            if data is not None and isinstance(data, list):
                friend_list.extend(data)
            else:
                function_logger.warning(f"Unexpected data for player {player_id}: {data}")
                return pd.DataFrame()
            
            batch_start += batch

            if len(data) < batch:
                break
        if not friend_list:
            function_logger.warning(f"No friends found for player {player_id}.")
            return pd.DataFrame()       
        df = pd.DataFrame(friend_list)
        return df
    except Exception as e:
        print(f"Exception while loading faceit friend list for {player_id}: {e}")
        return pd.DataFrame()  # Return empty DataFrame if an exception occurs

async def check_benelux_hub(player_id, faceit_data_v1: FaceitData_v1) -> tuple:
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
    
    try:
        # Gather the location requirements of the hubs that someone is a member of
        response = await faceit_data_v1.player_hubs(player_id, return_items=int(20))
        
        if isinstance(response, dict) and 'payload' in response and 'items' in response['payload']:
            hubs = [hub['competition'] for hub in response['payload']['items']]
        else:
            function_logger.warning(f"Unexpected data format for player {player_id}: {response}")
            return ()
        
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
    
    except Exception as e:
        function_logger.warning(f"Exception while checking benelux hub for {player_id}: {e}")
        return ()
    
    # print(f"{player_id}: {is_benelux_hub, InHub} for: {results}")
    return is_benelux_hub, InHub


async def get_hub_players() -> pd.DataFrame:
    """ Function to get all the players playing in the benelux hub """
    hub_id = '3e549ae1-d6a7-47d4-98cd-a6077a4da07c' # For benelux hub
    hub_players = []
    batch_start = 0
    batch_size = 50
    
    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data:
            try:
                while len(hub_players) == batch_start:
                    data = await faceit_data.hub_members(hub_id, starting_item_position=batch_start, return_items=batch_size)

                    hub_players.extend([player for player in data['items']])

                    batch_start += batch_size
            except Exception as e:
                function_logger.warning(f"Exception while loading hub players: {e}")
                raise
    
    df_players = pd.DataFrame(hub_players)
    if df_players.empty:
        function_logger.warning("No hub players found or unexpected data format.")
        return pd.DataFrame()
    
    return df_players.rename(columns={'user_id': 'player_id'})[['player_id', 'nickname']].drop_duplicates(subset='player_id').reset_index(drop=True)

async def get_benelux_leaderboard_players(elo_cutoff=2000) -> pd.DataFrame:
    """ Function to get all the players on the leaderboards of the Benelux countries (Belgium, Netherlands, Luxembourg) """
    country_list = ['be', 'nl', 'lu']
    
    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data:
            
            tasks = [fetch_country_leaderboard(country, elo_cutoff, faceit_data) for country in country_list]
            results = await gather_with_progress(tasks, desc="Fetching leaderboard players", unit="countries")
            
            all_leaderboard_players = [player for country_players in results for player in country_players]
    # Flatten the list of lists into a single list
    
    if isinstance(all_leaderboard_players, list) and all_leaderboard_players:
        df_leaderboard_players = pd.DataFrame(all_leaderboard_players)
    else:
        function_logger.warning("No leaderboard players found or unexpected data format.")
        return pd.DataFrame()
    return df_leaderboard_players   

async def fetch_country_leaderboard(country: str, elo_cutoff: int, faceit_data: FaceitData):
    batch_start = 0
    stop_fetching = False
    
    leaderboard_players = []
    batch_size = 100  # Number of players to retrieve in each batch (max 100 and should be divisible by total_leaderboard_players)
    
    try:
        while not stop_fetching:
            # Fetch the leaderboard for the country
            data = await faceit_data.game_global_ranking(
                game_id='cs2', 
                region='EU', 
                country=country, 
                starting_item_position=batch_start, 
                return_items=batch_size
            )
            
            if isinstance(data, dict) and 'items' in data:
                if isinstance(data['items'], list):
                    for player in data['items']:
                        player_elo = player.get('faceit_elo', 0)
                        if player_elo >= elo_cutoff:
                            leaderboard_players.append(player)
                        else:
                            stop_fetching = True
                            break  # Stop fetching if we hit the elo cutoff
            else:
                msg = f"Unexpected data format for country {country}: batch {batch_start} to {batch_start+batch_size}"
                function_logger.warning(msg)
                raise ValueError(msg)
            
            batch_start += batch_size
    except Exception as e:
        function_logger.warning(f"Exception while loading leaderboard players for country {country}: {e}")
        raise
    
    return leaderboard_players
        
def gather_players_country_json():
    """ Reads the players_country.json file and returns a DataFrame with the player country details """
    
    BASE_DIR = os.path.dirname(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
    URL = os.path.join(BASE_DIR, 'data_processing/data', 'players_country.json')

    try:
        with open(URL, 'r', encoding='utf-8') as f:  # Open the file in read mode with UTF-8 encoding
            player_ids = json.load(f)
            
            # Convert into a dataframe
            df_players_country = pd.DataFrame(player_ids)
        if df_players_country.empty:
            function_logger.warning("The players_country.json file is empty. Returning an empty DataFrame.")
            raise ValueError("The players_country.json file is empty.")

        if not df_players_country['player_id'].is_unique:
            function_logger.warning("The player_id column in players_country.json is not unique. Duplicates will be removed.")
            df_players_country = df_players_country.drop_duplicates(subset='player_id')
            
        return df_players_country
    
    except Exception as e:
        function_logger.error(f"Error reading players_country.json: {e}")
        raise
    
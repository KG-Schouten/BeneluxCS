# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import numpy as np
import json
import asyncio
import re
import math
import pycountry
from typing import Dict

# API imports
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.steam import SteamData
from data_processing.faceit_api.sliding_window import RequestDispatcher, request_limit, interval, concurrency
from data_processing.faceit_api.async_progress import gather_with_progress
from data_processing.faceit_api.logging_config import function_logger

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")
STEAM_TOKEN = os.getenv("STEAM_TOKEN")

async def batch_process_players(players: list[dict]) -> pd.DataFrame:
    """
    players = [
        {"player_id": "abc123", "claimed_country": "de", "steam_id": "7656..."},
        ...
    ]
    """
    semaphore = asyncio.Semaphore(30)  # Limit concurrent requests

    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher:
        async with FaceitData_v1(dispatcher) as faceit_data_v1, SteamData(STEAM_TOKEN) as steam_data:

            async def wrapped_process(player):
                async with semaphore:
                    return await process_faceit_player_details(
                        player["player_id"],
                        player.get("claimed_country", ""),
                        player.get("steam_id", ""),
                        faceit_data_v1,
                        steam_data
                    )

            tasks = [wrapped_process(player) for player in players]
            results = await gather_with_progress(tasks, desc="Processing players", unit="players")

            cleaned = []
            for i, result in enumerate(results):
                if isinstance(result, dict):
                    cleaned.append(result)
                else:
                    function_logger.warning(f"Player {players[i]['player_id']} failed: {result}")
                    cleaned.append({})  # Optional: Add fallback empty row

            return pd.DataFrame(cleaned)

async def process_faceit_player_details(player_id: str, claimed_country: str, steam_id: str, faceit_data_v1, steam_data) -> dict:
    try:
        features_faceit = await process_faceit_friendlist(player_id=player_id, claimed_country=claimed_country, faceit_data_v1=faceit_data_v1)
        prefixed_faceit = {f"faceit_{k}": v for k, v in features_faceit.items()}
        
        features_faceit_hub = await extract_hub_features(player_id=player_id, claimed_country=claimed_country, faceit_data_v1=faceit_data_v1)
        
        if steam_id:
            features_steam = await process_steam_friendlist(steam_id, steam_data)
            prefixed_steam = {
            f"steam_{k}": v for k, v in features_steam.get(steam_id, {}).items()
            }
        else:
            function_logger.warning(f"No Steam ID provided for player {player_id}, skipping Steam features.")
            prefixed_steam = {}

        merged = {
            "faceit_player_id": player_id,
            "steam_id": steam_id,
            **prefixed_faceit,
            **prefixed_steam,
            **features_faceit_hub
        }

        return merged

    except Exception as e:
        function_logger.error(f"Failed to process player {player_id}: {e}", exc_info=True)
        return {}
    
    
async def process_faceit_friendlist(player_id: str, claimed_country: str, faceit_data_v1: FaceitData_v1) -> dict:
    df_friends = await get_faceit_friendlist(player_id, faceit_data_v1)
    if df_friends.empty:
        return extract_friend_features(df_friends, claimed_country)
    return extract_friend_features(df_friends, claimed_country, country_col='country')
    
async def get_faceit_friendlist(player_id: str, faceit_data_v1: FaceitData_v1) -> pd.DataFrame:
    try:
        data = await faceit_data_v1.player_friend_list(player_id, starting_item_position=0, return_items=1)
        
        if not isinstance(data, dict) or 'payload' not in data or 'total_count' not in data['payload']:
            function_logger.error(f"Invalid response for player {player_id}: {data}")
            return pd.DataFrame()
        
        total_friends = int(data['payload'].get('total_count', 0))
        friends_max = min(total_friends, 200)

        tasks = [
            faceit_data_v1.player_friend_list(player_id, starting_item_position=i, return_items=100)
            for i in range(0, friends_max, 100)
        ]
        results = await asyncio.gather(*tasks)

        friends = [
            {
                'player_id': f.get('guid'),
                'country': f.get('country', '').lower(),
                'steam_id_64': f.get('identifier', {}).get('value')
                if f.get('identifier', {}).get('platform') == 'steam' else None
            }
            for batch in results
            if isinstance(batch, dict) and 'payload' in batch 
            for f in batch.get('payload', {}).get('results', [])
        ]

        return pd.DataFrame(friends)
    except Exception as e:
        function_logger.error(f"Faceit friend list error for {player_id}: {e}", exc_info=True)
        return pd.DataFrame()


async def process_steam_friendlist(steam_ids: list[str] | str, steam_data: SteamData) -> dict:
    if isinstance(steam_ids, str):
        steam_ids = [steam_ids]
    if not steam_ids:
        return {}

    # Fetch player country info in batches
    claimed_data_batches = await asyncio.gather(*[
        steam_data.get_player_summaries(steam_ids[i:i + 100]) for i in range(0, len(steam_ids), 100)
    ])

    claimed_map = {}
    for batch in claimed_data_batches:
        if isinstance(batch, dict) and 'response' in batch and 'players' in batch['response']:
            for player in batch['response']['players']:
                claimed_map[player['steamid']] = player.get('loccountrycode', '').lower()

    # Process friend lists
    features = {}
    for steam_id in steam_ids:
        try:
            df_friends = await get_steam_friendlist(steam_id, steam_data)
            claimed_country = claimed_map.get(steam_id)
            
            if df_friends is None or df_friends.empty:
                features[steam_id] = {
                    'claimed_country': claimed_country,
                    'friend_list_visible': False,
                    'total_friends': 0,
                    'most_common_country': None,
                    'friends_most_common_country': 0,
                    'percentage_most_common_country': 0.0,
                    'friends_claimed_country': 0,
                    'percentage_claimed_country': 0.0,
                    'is_claimed_country_same': False,
                    'country_entropy': 0.0
                }
            else:
                f = extract_friend_features(df_friends, claimed_country, country_col='loccountrycode')
                f['friend_list_visible'] = True
                features[steam_id] = f

        except Exception as e:
            function_logger.error(f"Failed to process Steam friend data for {steam_id}: {e}", exc_info=True)
            features[steam_id] = {
                'claimed_country': claimed_map.get(steam_id),
                'friend_list_visible': False,
                'total_friends': 0,
                'most_common_country': None,
                'friends_most_common_country': 0,
                'percentage_most_common_country': 0.0,
                'friends_claimed_country': 0,
                'percentage_claimed_country': 0.0,
                'is_claimed_country_same': False,
                'country_entropy': 0.0
            }

    return features
         
async def get_steam_friendlist(steam_id: str, steam_data: SteamData) -> pd.DataFrame:
    try:
        friends_data = await steam_data.get_friend_list(steam_id)
        friend_list = friends_data.get('friendslist', {}).get('friends', [])

        if not friend_list:
            return pd.DataFrame()

        friend_ids = [f['steamid'] for f in friend_list][:200]

        batches = await asyncio.gather(*[
            steam_data.get_player_summaries(friend_ids[i:i+100]) for i in range(0, len(friend_ids), 100)
        ])

        friends = [
            {
                'steam_id': f['steamid'],
                'loccountrycode': f.get('loccountrycode', '').lower()
            }
            for batch in batches
            for f in batch.get('response', {}).get('players', [])
        ]
        df = pd.DataFrame(friends)
        return df[df['loccountrycode'].notna()]

    except Exception as e:
        function_logger.error(f"Steam friend list error for {steam_id}: {e}", exc_info=True)
        return pd.DataFrame()


def extract_friend_features(df_friends: pd.DataFrame, claimed_country: str, country_col: str = 'country') -> dict:
    total_friends = len(df_friends)
    country_counts = df_friends[country_col].value_counts()
    most_common_country = country_counts.idxmax() if not country_counts.empty else None
    friends_most_common_country = country_counts.max() if not country_counts.empty else 0
    percentage_most_common_country = friends_most_common_country / total_friends if total_friends > 0 else 0.0

    if claimed_country and isinstance(claimed_country, str) and re.fullmatch(r"[a-zA-Z]{2}", claimed_country):
        friends_claimed_country = (df_friends[country_col] == claimed_country.lower()).sum()
        percentage_claimed_country = friends_claimed_country / total_friends if total_friends > 0 else 0.0
        is_claimed_country_same = claimed_country.lower() == most_common_country
    else:
        friends_claimed_country = 0
        percentage_claimed_country = 0.0
        is_claimed_country_same = False

    def entropy(counts):
        total = sum(counts)
        probs = [c / total for c in counts if total > 0]
        return -sum(p * math.log2(p) for p in probs if p > 0)

    country_entropy = entropy(country_counts.values)
    
    return {
        'claimed_country': claimed_country,
        'total_friends': total_friends,
        'most_common_country': most_common_country,
        'friends_most_common_country': friends_most_common_country,
        'percentage_most_common_country': percentage_most_common_country,
        'friends_claimed_country': friends_claimed_country,
        'percentage_claimed_country': percentage_claimed_country,
        'is_claimed_country_same': is_claimed_country_same,
        'country_entropy': country_entropy
    }


#  Helper to get a map of lowercase country names to alpha_2 codes
COUNTRY_NAME_MAP = {
    country.name.lower(): country.alpha_2 # type: ignore[attr-defined]
    for country in pycountry.countries
}

COUNTRY_NAME_MAP.update({
    "benelux": ["BE", "NL", "LU"],
    "europe": [],
    "cis": ["RU", "UA", "BY", "KZ", "UZ"],
    "baltic": ["LT", "LV", "EE"]
})

def match_country_names(text: str) -> list[str]:
    text = text.lower()
    matches = []
    for name, code in COUNTRY_NAME_MAP.items():
        if name in text:
            if isinstance(code, list):
                matches.extend(code)
            else:
                matches.append(code)
    return matches

async def extract_hub_features(player_id: str, claimed_country: str, faceit_data_v1: FaceitData_v1) -> dict:
    try:
        response = await faceit_data_v1.player_hubs(player_id, return_items=20)
        
        if not isinstance(response, dict) or 'payload' not in response:
            function_logger.error(f"Invalid response for player {player_id}: {response}")
            raise TypeError(f"Invalid response format for player {player_id}")
        
        hubs = [hub['competition'] for hub in response.get('payload', {}).get('items', [])]

        if not hubs:
            return {
                'is_in_hub': False,
                'hub_geo_whitelist': [],
                'has_geo_whitelist': False,
                'hub_geo_name_hint': [],
                'is_national_hub': False,
                'nationality_match': False,
                'num_hubs': 0
            }

        geo_whitelist_set = set()
        name_geo_hint_set = set()
        for hub in hubs:
            whitelist = hub.get('whitelistGeoCountries') or []
            geo_whitelist_set.update(whitelist)

            # Match names like "Netherlands", "Benelux", etc.
            name = hub.get('name', '').lower()
            matched = match_country_names(name)
            name_geo_hint_set.update(matched)

        whitelist_is_specific = len(geo_whitelist_set) > 0 and len(geo_whitelist_set) < 10
        has_geo_whitelist = bool(geo_whitelist_set)
        inferred_country_codes = geo_whitelist_set.union(name_geo_hint_set)

        # Check if claimed_country matches whitelist or hub name
        nationality_match = claimed_country.upper() in inferred_country_codes

        return {
            'is_in_hub': True,
            'hub_geo_whitelist': list(geo_whitelist_set),
            'has_geo_whitelist': has_geo_whitelist,
            'hub_geo_name_hint': list(name_geo_hint_set),
            'is_national_hub': bool(inferred_country_codes) and whitelist_is_specific,
            'nationality_match': nationality_match,
            'num_hubs': len(hubs)
        }

    except Exception as e:
        function_logger.warning(f"Hub extraction failed for {player_id}: {e}")
        return {
            'is_in_hub': False,
            'hub_geo_whitelist': [],
            'has_geo_whitelist': None,
            'hub_geo_name_hint': [],
            'is_national_hub': None,
            'nationality_match': None,
            'num_hubs': None
        }




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
    
    return df_players.rename(columns={'user_id': 'player_id'})[['player_id', 'player_name']].drop_duplicates(subset='player_id').reset_index(drop=True)

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
        df_leaderboard_players = df_leaderboard_players.rename(columns={'nickname': 'player_name'})
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
    
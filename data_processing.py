from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1

from functions import load_api_keys

import json
import os
import random
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import requests
import re
import time
from typing import Dict, Any, Callable, Optional

api_keys = load_api_keys()

faceit_data = FaceitData(api_keys.get("FACEIT_TOKEN"))
faceit_data_v1 = FaceitData_v1()

# Create a semaphore to limit the number of concurrent requests
SEMAPHORE = asyncio.Semaphore(5)  # Adjust the limit based on API restrictions

## Data paths
team_data_path = "C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"

# Rate limit constants
request_times = []
RATE_LIMIT = 350
TIME_WINDOW = 10

### -----------------------------------------------------------------
### Utility Functions
### -----------------------------------------------------------------
async def enforce_rate_limit():
    """
    Ensure API requests do not exceed the allowed rate limit.
    """
    global request_times
    current_time = time.time()
    request_times = [t for t in request_times if current_time - t < TIME_WINDOW] # Remove old timestamps
    
    if len(request_times) >= RATE_LIMIT:
        wait_time = TIME_WINDOW - (current_time - request_times[0])
        # print(f"Rate limit reached! Pausing for {wait_time:.2f} seconds...")
        await asyncio.sleep(wait_time)
        
    request_times.append(time.time()) # Log request timestamp
  
async def safe_api_call(api_function: Callable[..., Any], *args: Any, retries: int = 5, base_delay: float = 2.0, **kwargs: Any) -> Optional[Any]:
    """
    Safely calls an API function with automatic rate limit handling and retries.
    
    Uses an exponential backoff strategy to wait before retrying when rate-limited.

    Args:
        api_function (Callable[..., Any]): The API function to call.
        *args (Any): Positional arguments to pass to the API function.
        retries (int, optional): Maximum number of retry attempts. Defaults to 5.
        base_delay (float, optional): Base delay in seconds for exponential backoff. Defaults to 2.0.
        **kwargs (Any): Keyword arguments to pass to the API function.

    Returns:
        Optional[Any]: The API response if successful, or None if all retries fail.
    """
    for attempt in range(retries):
        await enforce_rate_limit()
        try:
            response = await asyncio.to_thread(api_function, *args, **kwargs)
            
            # If the response is an integer error code
            if isinstance(response, int):
                if response == 429: # Rate limit error
                    wait_time = (base_delay ** attempt) + random.uniform(0, 1)  # Exponential backoff
                    print(f"Rate limited! Retrying in {wait_time:.2f} seconds (Attempt {attempt + 1}/{retries})...")
                    await asyncio.sleep(wait_time)
                    continue # retry the call
                elif response == 404:
                    break  # Stop retrying for 404 error
                else:
                    print(f"API error: {response} - Unexpected error code.")
                    break  # Stop retrying for other error codes
            
            # If the response is not an error, return it
            return response  # Return the successful response
        
        except Exception as e:
            print(f"Unexpected error occurred while calling API: {e}")
            break
    
    return None

### -----------------------------------------------------------------
### General Data Processing
### -----------------------------------------------------------------
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

async def process_match_stats(match_id: str, team_id: list|str) -> dict: # Add match_entry and player_entry potentially
    """
    Async function to gathers the match stats from the Faceit API

    Args:
        match_id (str): The ID of the match
        team_id (list|str): The team_id of the team you want to get the data from

    Returns:
        A tuple of lists of dicts containing the following data:
            - map_entry
            - team_map_entry
            - player_stats_entry
    """
    map_entry, team_map_entry, player_stats_entry, player_entry = [], [], [], []
    
    # Gather the match stats from the API
    match_stats = await safe_api_call(faceit_data.match_stats, match_id)
    
    if not isinstance(match_stats, dict): # Check if the match is a FFW
        # Run this when the match is a FFW
        return None
    else:
        for map in match_stats['rounds']:
            ## Gather map details
            match_round = map.get("match_round", None)
            
            sorted_keys = sorted(map['round_stats'].keys())
            map_item = {
                "match_id": match_id,
                "match_round": match_round,
                "best_of": map.get("best_of", None),
                **{key: map['round_stats'][key] for key in sorted_keys}
            }
            map_entry.append(map_item)
            
            for team in map['teams']:
                if isinstance(team_id, str):
                    team_ids = [team_id]
                else:
                    team_ids = team_id
                for team_id in team_ids:     
                    if team_id == team.get("team_id", None):
                        ## Gather team stats per map
                        sorted_keys = sorted(team['team_stats'].keys())
                        team_item = {
                            "match_id": match_id,
                            "match_round": match_round,
                            "team_id": team_id,
                            **{key: team['team_stats'][key] for key in sorted_keys}
                        }
                        team_map_entry.append(team_item)
                        
                        for player in team['players']:
                            ## Gather player stats per map
                            player_id = player.get("player_id", None)
                            player_name = player.get("nickname", None)

                            sorted_keys = sorted(player['player_stats'].keys())
                            player_stats_item = {
                                "player_id": player_id,
                                "player_name": player_name,
                                "team_id": team_id,
                                "match_id": match_id,
                                "match_round": match_round,
                                **{key: player['player_stats'][key] for key in sorted_keys}
                            }
                            player_stats_entry.append(player_stats_item)

    return map_entry, team_map_entry, player_stats_entry

async def process_player_details(data_list: list, check: bool=False):
    """
    Gathers the player details for each unique player_id in a list of dictionaries
    
    Args:
        data_list (list): A list of dictionaries where each dict should have a 'player_id' key
        check (bool)
    
    Returns:
        player_entry (list): A list of dictionaries containing player details
    """
    unique_ids = list({d['player_id'] for d in data_list})

    player_entry = []
    
    # Run faceit_data.player_id_details and check_benelux concurrently for each player
    for player_id in unique_ids:
        player_item = await fetch_and_check_player_details(player_id, check=check)
        player_entry.append(player_item)
    
    return player_entry

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
    player_details = await safe_api_call(faceit_data.player_id_details, player_id)
    
    player_name = player_details.get('nickname', None)
    country = player_details.get('country', None)
    avatar = player_details.get('avatar', None)
    elo = player_details['games']['cs2'].get('faceit_elo')
    
    if check:
        # Check Benelux status concurrently
        check_bnlx = await check_benelux(player_id, player_name)  # Use player_id as nickname if player_name is not provided
    
        # If the player is Benelux, create the player entry
        benelux_codes = ['nl', 'be', 'lu']
        if (check_bnlx[0] and (country not in benelux_codes)): # If player is benelux and fake flagging
            new_country = check_bnlx[2] 
            print(f"{player_name}'s country flag changed from '{country}' to '{new_country}'")
            country = new_country      
        elif ((not check_bnlx[0]) and (country in benelux_codes)):  # If player is not benelux and fakeflagging benelux
            new_country = check_bnlx[2] 
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
### Benelux Check
### -----------------------------------------------------------------
async def check_all_players(player_ids, player_names, PRINT=False):
    """ Small wrapper to get result of a list of player_ids"""
    tasks = [check_benelux(player_id, player_name, PRINT) for player_id, player_name in zip(player_ids, player_names)]
    results = await asyncio.gather(*tasks) # Run all tasks concurrently
    return results
        
async def check_benelux(player_id: str, player_name: str, PRINT: bool=False):
    """
    General code block for checking if a player is benelux
    
    Args:
        player_id (str): faceit player id
        player_name (str): player nickname
        PRINT (bool): True if you want to print confirmation of benelux (default=False)
        
    Returns:
        bool: True if benelux, false if not
        friend_frac (float): The fraction of friends from benelux
        country (str): The country code of the most frequent country in friendslist
    """
    benelux_codes = ['nl', 'be', 'lu']
    
    result, friend_frac, country = await check_friends_faceit(player_id, player_name, PRINT)
    if result:
        if PRINT:
            print(f"He is Benelux ^")
        return True, friend_frac, country
    else:
        if PRINT:
            print("He is not Benelux ^")
        return False, friend_frac, country
  
async def check_friends_faceit(player_id: str, player_name: str, PRINT: bool=False):
    # Constants
    benelux_codes = ['nl', 'be', 'lu']
    Threshold = 0.1
    # Get friend ids
    df_friends = await get_friend_list_faceit(player_id, player_name)
    
    if df_friends is not None:
        country_count = df_friends['country'].value_counts()
        benelux_sum = sum(country_count.get(country,0) for country in benelux_codes)
        friend_frac = int(benelux_sum)/int(len(df_friends))
        country = country_count.idxmax()
        
        if PRINT:
            print(f"{player_name} has {benelux_sum} friends from Benelux: {friend_frac}")
        
        if friend_frac > Threshold:
            return True, friend_frac, country # Return True if there are many benelux players in the friend list
        else:
            return False, friend_frac, country # Return false if there are too little benelux players in friend list
    else:
        return None # Returns none if the friend list is private

async def get_friend_list_faceit(player_id: str, player_name: str = None) -> pd.DataFrame:
    try:
        friend_list = []
        batch_start = 0
        batch = 50
        # Keep looping until all friends in friendlist have been appended
        while len(friend_list) == batch_start:
            data = await safe_api_call(faceit_data_v1.player_friend_list, player_id, starting_item_position=batch_start, return_items=batch)
            data = data['payload']['results']

            # Add the new data to the friend list
            friend_list.extend(data)
            
            batch_start += batch
                
        df = pd.DataFrame.from_dict(friend_list)
        return df
    except Exception as e:
        print(f"Exception while loading faceit friend list for {player_name}: {e}")
        return None

### -----------------------------------------------------------------
### ESEA League Data Processing
### -----------------------------------------------------------------

def process_esea_data(team_ids: list, teams_to_return = "ALL") -> tuple[list[Dict], list[Dict], list[Dict], list[Dict], list[Dict]]:
    """
    Processes ESEA match data asynchronously for multiple teams.

    Args:
        team_ids (dict): A dictionary of team_ids per season to process (use gather_team_ids_json())
        teams_to_return (str): Specifies the amount of teams that will be gathered. It can be: (default="ALL")
            - The string "SINGLE", which will return the latests team in the list from the latest season (that has teams in it)
            - The string "ALL", which will return all teams in the list

    Returns:
        Tuple of lists of dicts
            - all_teams
            - all_esea_szn
            - all_esea_stg
            - all_players
            - all_player_team_szn
            - all_matches
            - all_maps
            - all_team_maps
            - all_player_stats
    """
    print(
        """
        
        ---------------------------------------
                Processing ESEA Data:
        ---------------------------------------
        
        """
    )
    try:
        loop = asyncio.get_running_loop()  # This works when there's already a running loop
    except RuntimeError:  # If no loop is running, then we create a new loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    if loop.is_running():
        task = loop.create_task(process_esea_data_async(team_ids, teams_to_return))
        return task.result()  # Wait for the result synchronously
    else:
        return asyncio.run(process_esea_data_async(team_ids, teams_to_return))
    
async def process_esea_data_async(team_ids: list, teams_to_return = "ALL") -> tuple[list[Dict], list[Dict], list[Dict], list[Dict], list[Dict]]:
    """
    Processes ESEA match data asynchronously for multiple teams.

    Args:
        team_ids (dict): A dictionary of team_ids per season to process (use gather_team_ids_json())
        teams_to_return (str): Specifies the amount of teams that will be gathered. It can be: (default="ALL")
            - The string "SINGLE", which will return the latests team in the list from the latest season (that has teams in it)
            - The string "ALL", which will return all teams in the list

    Returns:
        Tuple of lists of dicts
            - all_teams
            - all_esea_szn
            - all_esea_stg
            - all_players
            - all_player_team_szn
            - all_matches
            - all_maps
            - all_team_maps
            - all_player_stats
    """
    # Shared lists to collect data from all teams
    all_teams, all_esea_szn, all_esea_stg, all_players, all_player_team_szn, all_matches, all_maps, all_team_maps, all_player_stats = [], [], [], [], [], [], [], [], []
    
    # Shared sets to track unique ids
    processed_szn_ids, processed_stg_ids, processed_player_ids, processed_team_ids, processed_match_ids, processed_map_ids = set(), set(), set(), set(), set(), set()
    
    # Lock for synchronizing access to shared state across workers and create queue
    lock = asyncio.Lock()
    queue = asyncio.Queue()

    async def worker():
        while True:
            item = await queue.get()
            
            if item is None:
                break
            
            if not isinstance(item, tuple) or len(item) != 4:
                print(f"Unexpected item structure: {item}")
                queue.task_done()
                continue
            
            # Extract team_id, season_id and season_number from tuple item
            team_id, team_name, season_id, season_number = item
            
            try:
                datalists = await process_team_id_data(team_id, season_id)

                # Lock to ensure that only one worker is modifying shared data at a time
                async with lock:
                    # Add team data if not processed before
                    for team in datalists[0]:
                        team_id = team.get('team_id')
                        if team_id and team_id not in processed_team_ids:
                            all_teams.append(team)
                            processed_team_ids.add(team_id)

                    # Add season data if not processed before
                    for szn in datalists[1]:
                        season_id = szn.get('season_id')
                        if season_id and season_id not in processed_szn_ids:
                            all_esea_szn.append(szn)
                            processed_szn_ids.add(season_id)

                    # Add stage data if not processed before
                    for stg in datalists[2]:
                        stage_id = stg.get('stage_id')
                        if stage_id and stage_id not in processed_stg_ids:
                            all_esea_stg.append(stg)
                            processed_stg_ids.add(stage_id)

                    # Add player data if not processed before
                    for player in datalists[3]:
                        player_id = player.get('player_id')
                        if player_id and player_id not in processed_player_ids:
                            all_players.append(player)
                            processed_player_ids.add(player_id)

                    # Add player-team-season data if not processed before
                    for player_team_szn in datalists[4]:
                        all_player_team_szn.append(player_team_szn)
                    
                    # Add match data
                    for match in datalists[5]:
                        match_id = match.get('match_id')
                        if match_id and match_id not in processed_match_ids:
                            all_matches.append(match)
                            processed_match_ids.add(match_id)

                    # Add map data
                    for map in datalists[6]:
                        match_id = map.get('match_id')
                        round_id = map.get('match_round')
                        if match_id and round_id:
                            map_ids = (match_id, round_id)
                            
                            if map_ids not in processed_map_ids:  
                                all_maps.append(map)
                                processed_map_ids.add(map_ids)
                 
                # Add team map data
                for team_map in datalists[7]:
                    all_team_maps.append(team_map)
                
                # Add player stats data
                for player_stats in datalists[8]:
                    all_player_stats.append(player_stats)
                    
                print(f"Added data from team: {team_name} in season: {season_number}")
            except Exception as e:
                print(f"Error fetching data for team {team_name} in season {season_number}: {e}")
            
            queue.task_done()
    
    ## Create tasks for all workers
    num_workers = 20 # Adjust the number of concurrent workers
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    
    # Populate queue with team_ids based on input args
    if teams_to_return == "ALL":
        for season_id, season_data in team_ids.get('seasons', {}).items():
            season_number = season_data.get("season_number", None)
            for team_name, team_id in season_data.get("teams", {}).items():
                await queue.put((team_id, team_name, season_id, season_number))
    
    elif teams_to_return == "SINGLE":
        for season_id, season_data in sorted(team_ids.get('seasons', {}).items(), key=lambda x: x[1].get('season_number', 0), reverse=True):
            if season_data.get("teams"):
                season_number = season_data.get("season_number", None)
                team_name, team_id = next(iter(season_data.get("teams", {}).items()), (None, None))
                if team_id:
                    await queue.put((team_id, team_name, season_id, season_number))
                    break
                # Else skip to the next season with teams
    else:
        raise ValueError(f"Invalid teams_to_return value: {teams_to_return}")
    
    await queue.join() # Wait for queue to be fully processed
    
    # Stop workers by adding `None` signals
    for _ in range(num_workers):
        await queue.put(None)
        
    # Ensure all workers have finished
    await asyncio.gather(*workers)
    
    # Return the datasets and before that modify all keys so they are ok for the mySQL database
    return tuple(modify_keys(data) for data in [all_teams, all_esea_szn, all_esea_stg, all_players, all_player_team_szn, all_matches, all_maps, all_team_maps, all_player_stats])

async def process_team_id_data(team_id, season_id):
    """
    async function to process the data of a single team into multiple pandas dataframes
    
    Args:
        team_id (str): the ID of the team
        season_id (str): the ID of the season
        
    Returns:
        tuple(list[Dict], list[Dict], list[Dict], list[Dict], list[Dict], list[Dict], list[Dict], list[Dict], list[Dict]): 
            - team_entry
            - esea_szn_entry
            - esea_stg_entry
            - player_entry
            - player_team_szn_entry
            - esea_match_entry
            - esea_map_entry
            - esea_team_map_entry
            - esea_player_stats_entry
    """
    ## Constants and initialization of variables
    team_entry, esea_szn_entry, esea_stg_entry, player_entry, player_team_szn_entry, esea_match_entry, esea_map_entry, esea_team_map_entry, esea_player_stats_entry = [], [], [], [], [], [], [], [], []

    # Sets to track unique items
    processed_szn_ids, processed_stg_ids, processed_player_ids, processed_team_ids, processed_match_ids, processed_map_ids = set(), set(), set(), set(), set(), set()

    # API calls (ensure async handling if possible)
    team_details = await safe_api_call(faceit_data.team_details, team_id)
    team_details_league = await safe_api_call(faceit_data_v1.league_team_details, team_id)
    team_details_league = team_details_league['payload'][0]

    # Only keep specific keys of td
    team_keys = ['team_id', 'name', 'description', 'avatar', 'website', 'twitter', 'youtube']
    team_item = {key: team_details.get(key,None) for key in team_keys}
    team_item['league_team_id'] = team_details_league['league_seasons_info'][0]['league_team_id']

    # Append only if team_id is new
    if team_id not in processed_team_ids:
        team_entry.append(team_item)
        processed_team_ids.add(team_id)

    ## GET ESEA SEASON DETAILS
    # Gather the season data based on the season_id
    szn = next(
        (season for season in team_details_league.get("league_seasons_info", []) if season.get("season_id") == season_id),
        None,
    )
    
    if season_id not in processed_szn_ids:
        esea_szn_keys = ['season_id', 'season_number']
        esea_szn_item = {key: szn.get(key,None) for key in esea_szn_keys}
        esea_szn_entry.append(esea_szn_item)
        processed_szn_ids.add(season_id)

    for stg in szn.get("season_standings", []):
        if stg:
            stage_id = stg.get("stage_id", None) 
            ## Gather stage data
            if stage_id not in processed_stg_ids:
                esea_stg_keys = ['stage_id', 'stage_name', 'conference_id', 'conference_name', 'championship_id', 'division_id', 'division_name', 'region_id', 'region_name']
                esea_stg_item = {key: stg.get(key,None) for key in esea_stg_keys}
                esea_stg_item['season_id'] = szn['season_id']
                esea_stg_entry.append(esea_stg_item)
                processed_stg_ids.add(stage_id)

            champ_id = stg.get("championship_id", None)
            matches = await safe_api_call(faceit_data_v1.league_team_matches, team_id, champ_id)
            matches = matches['payload']['items']   
            for match in matches:
                if match.get("factions", {})[1]['id'] != "bye":
                    ## Gather match details
                    match_id = match.get("origin", {}).get("id")
                    match_state = match.get("origin", {}).get("state", None)
                    
                    # Gather match details
                    match_item = {
                        "match_id": match_id,
                        "team_id": team_id,
                        "season_id": season_id,
                        "stage_id": stage_id,
                        "opponent_id": next(f['id'] for f in match.get("factions", {}) if f['id'] != team_id),
                        "winner": match.get("winner", None),
                        "match_state": match_state,
                        "match_time": match.get("origin", {}).get("schedule", None)
                    }

                    esea_match_entry.append(match_item)

                    ## Gather match stats
                    if match_state == "FINISHED":
                        match_stat_entries = await process_match_stats(match_id, team_id)
                        # Append the data if the return isnt None (in which case the match hasnt been played for some reason)
                        if match_stat_entries is not None:
                            esea_map_entry.extend(match_stat_entries[0])
                            esea_team_map_entry.extend(match_stat_entries[1]) 
                            esea_player_stats_entry.extend(match_stat_entries[2])
    
    # Gather player_details
    player_entry = await process_player_details(esea_player_stats_entry, check=True)
    
    # Gather player_team_szn
    for player in player_entry:
        player_id = player['player_id']
        player_team_szn_item = {
            'player_id': player_id,
            'team_id': team_id,
            'season_id' : season_id,
        }
        player_team_szn_entry.append(player_team_szn_item)
    
    return team_entry, esea_szn_entry, esea_stg_entry, player_entry, player_team_szn_entry, esea_match_entry, esea_map_entry, esea_team_map_entry, esea_player_stats_entry

def gather_team_ids_json(URL: str = "data\\league_teams.json") -> dict:
    """
    Reads a JSON file containing team names and their corresponding team IDs.

    Args:
        URL (str, optional): The file path to the JSON file containing team IDs.
                             Defaults to "data/league_teams.json".

    Returns:
        dict: A dictionary containing the following structure:
            {
                "seasons": {
                    "season_id": {
                        "season_number": int,
                        "teams": {
                            "team_name": "team_id",
                            ...
                        }
                    },
                    ...
                }
            }
    """
    try:
        with open(URL, 'r', encoding='utf-8') as f:  # Open the file in read mode with UTF-8 encoding
            team_ids = json.load(f)  # Load the JSON content into a Python dictionary
        return team_ids
    except FileNotFoundError:
        print(f"Error: The file '{URL}' was not found.")
        return {}  # Return an empty dictionary if the file is missing
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from '{URL}'. Ensure the file contains valid JSON data.")
        return {}  # Return an empty dictionary if JSON parsing fails

def check_team_ids_json(URL: str = "data\\league_teams.json") -> bool:
    """
    Checks the contents of the JSON files containing the team names and their corresponding team IDs for each esea season.
    Following checks are performed:
    - Check the structure of the JSON file.
    - Check if all values are dictionaries.
    - Check if all keys are present.
    - Check if the season_number is an integer.
    - Check if the teams are a dictionary.
    - Check if all team_ids are unique.
    - Check if all team_ids are valid.
    
    Args:
        URL (str, optional): The file path to the JSON file containing team IDs.
                             Defaults to "data/league_teams.json".
    
    Returns:
        bool: True if all checks pass, False otherwise.
    """
    
    team_ids = gather_team_ids_json(URL)
    
    # Check if structure of the JSON file is correct:
        # "seasons" : {
        #     "season_id" : {
        #       "season_number" : int,
        #       "teams" : {
        #           "team_name" : "team_id",
        #           ...
        #       }
        #     },
        #     ...
        # }
    excpected_keys = ["seasons"]
    if all(key in team_ids for key in excpected_keys):
        # Check if the seasons are a dictionary
        if not isinstance(team_ids['seasons'], dict):
            print("check_team_ids_json() Error: seasons should be a dictionary.")
        for season_id in team_ids['seasons']:
            # Gather season data
            season = team_ids['seasons'][season_id]
            
            # Check if the season is a dictionary
            if not isinstance(season, dict):
                print("check_team_ids_json() Error: All values in the JSON file should be dictionaries.")
                return False
            
            # Check if all keys are in the season
            expected_keys = ['season_number', 'teams']
            
            if not all(key in season for key in expected_keys):
                print(f"check_team_ids_json() Error: Missing keys in season {season_id}")
                return False

            # Check if the season_number is an integer
            if not isinstance(season['season_number'], int):
                print(f"check_team_ids_json() Error: season_number in season {season_id} should be an integer.")
                return False
            
            # Check if the teams are a dictionary
            if not isinstance(season['teams'], dict):
                print(f"check_team_ids_json() Error: teams in season {season_id} should be a dictionary.")
                return False
            
            # Check if all team_ids are unique
            team_ids_list = list(season['teams'].values())
            if len(team_ids_list) != len(set(team_ids_list)):
                print(f"check_team_ids_json() Error: Duplicate team_ids in season {season_id}")
                return False
            
            # Check if all team_ids are valid
            for team_id in team_ids_list:
                team_details = safe_api_call(faceit_data.team_details, team_id)
                if not isinstance(team_details, dict):
                    print(f"check_team_ids_json() Error: Invalid team_id {team_id}")
                    return False
            
        # If all passes, return True
        print("check_team_ids_json() Success: All checks passed.")
        return True
    
    else:
        print("check_team_ids_json() Error: Missing keys in the JSON file.")
        return False

### -----------------------------------------------------------------
### Benelux Hub Data Processing
### -----------------------------------------------------------------

def process_hub_data(starting_item_position: int=0, return_items: int=100) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Processes Hub match data asynchronously for matches in Benelux Hub

    Args:
        starting_item_position (int): Amount of matches to skip before gathering the data: (default=0)
            - If return_items > 100, this arg will be ignored.
            - Else return_items has to be a MULTIPLE of this!!!
        return_items (int | str): Specifies the amount of matches that will be gathered. It can be: (default=100) 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
        
    Returns:
        tuple(list[Dict], list[Dict], list[Dict], list[Dict]): 
            - match_entry
            - map_entry
            - team_map_entry
            - player_entry
            - player_stats_entry
    """
    print(
        """
        
        ---------------------------------------
                Processing BNLX Hub Data:
        ---------------------------------------
        
        """
    )
    # Gather the hub matches:
    hub_matches = asyncio.run(gather_hub_matches(starting_item_position, return_items))
    
    try:
        loop = asyncio.get_running_loop()  # This works when there's already a running loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    if loop.is_running():
        task = loop.create_task(process_hub_data_async(hub_matches))
        return task.result()  # Wait for the result synchronously
    else:
        return asyncio.run(process_hub_data_async(hub_matches))

async def process_hub_data_async(hub_matches: list) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Processes Hub match data asynchronously for matches in Benelux Hub

    Args:
        starting_item_position (int): Amount of matches to skip before gathering the data: (default=0)
            - If return_items > 100, this arg will be ignored.
            - Else return_items has to be a MULTIPLE of this!!!
        return_items (int | str): Specifies the amount of matches that will be gathered. It can be: (default=100) 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
        
    Returns:
        tuple(list[Dict], list[Dict], list[Dict], list[Dict]): 
            - match_entry
            - map_entry
            - team_map_entry
            - player_entry
            - player_stats_entry
    """
    ## Constants and initialization of variables
    match_entry, map_entry, team_map_entry, player_stats_entry, player_entry = [], [], [], [], []

    # Sets to track unique items
    processed_match_ids, processed_player_ids = set(), set()
    
    # Lock for synchronizing access to shared state across workers and create queue
    lock = asyncio.Lock()
    queue = asyncio.Queue()

    async def worker():
        while True:
            match = await queue.get()
            
            if match is None:
                break

            try:
                hub_match_data = await process_hub_match(match)
                
                # Lock to ensure that only one worker is modifying shared data at a time
                async with lock:
                    # Add match data if not processed before
                    for match in hub_match_data[0]:
                        match_id = match.get('match_id')
                        if match_id and match_id not in processed_match_ids:
                            match_entry.append(match)
                            processed_match_ids.add(match_id)
                    
                    # Add map data
                    for map in hub_match_data[1]:
                        map_entry.append(map)
                    
                    # Add team_map entry
                    for team_map in hub_match_data[2]:
                        team_map_entry.append(team_map)
                        
                    # Add player details if not processed before
                    for player in hub_match_data[3]:
                        player_id = player.get('player_id')
                        if player_id and player_id not in processed_player_ids:
                            player_entry.append(player)
                            processed_player_ids.add(player_id)
                    # Add player stats
                    for player_stats in hub_match_data[4]:
                        player_stats_entry.append(player_stats)
                
                print(f"Added match data for {match_id}")
            except Exception as e:
                print(f"Error fetching data for a match: {e}")
            queue.task_done()
    
    ## Create tasks for all workers
    num_workers = 5 # Adjust the number of concurrent workers
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    
    # Populate queue with matches
    for match in hub_matches:
        await queue.put(match)
        await asyncio.sleep(0.5)
    
    await queue.join() # Wait for queue to be fully processed
    
    # Stop workers by adding `None` signals
    for _ in range(num_workers):
        await queue.put(None)
        
    # Ensure all workers have finished
    await asyncio.gather(*workers)
    
    return tuple(modify_keys(data) for data in [match_entry, map_entry, team_map_entry, player_entry, player_stats_entry])

async def process_hub_match(match):
    """
    Async function to process the data of a single HUB match to the data dictionaries
    
    Args:
        match (dict): The hub match dictionary with data about the match
    
    Returns:
        tuple(list[Dict], list[Dict], list[Dict], list[Dict]): 
            - match_entry
            - map_entry
            - team_map_entry
            - player_entry
            - player_stats_entry
        
    """
    match_entry, map_entry, team_map_entry, player_stats_entry  = [], [], [], []
    
    match_id = match.get('match_id')
    team_id_1 = match['teams']['faction1'].get('faction_id', None)
    team_id_2 = match['teams']['faction2'].get('faction_id', None)
    ## Process match data
    match_item = {
        'match_id': match_id,
        'competition_id': match.get('competition_id', None),
        'competition_name': match.get('competition_name', None),
        'team_id_1': team_id_1,
        'team_id_2': team_id_2,
        'winner': match['teams'][match['results']['winner']].get('faction_id', None),
        'match_state': match.get('status', None),
        'match_time': match.get('started_at', None)
    }
    match_entry.append(match_item)

    match_stat_entries = await process_match_stats(match_id, [team_id_1, team_id_2])

    if match_stat_entries is not None:
                            map_entry.extend(match_stat_entries[0])
                            team_map_entry.extend(match_stat_entries[1]) 
                            player_stats_entry.extend(match_stat_entries[2])
    
    # Gather player_details
    player_entry = await process_player_details(player_stats_entry)
    
    return match_entry, map_entry, team_map_entry, player_entry, player_stats_entry

async def gather_hub_matches(starting_item_position: int=0, return_items: int=100) -> list[dict, dict, dict]:
    """
    Gather Benelux hub matches (and some of the data) based on the amount of matches to return
    
    Args:
        starting_item_position (int): Amount of matches to skip before gathering the data: (default=0)
            - If return_items > 100, this arg will be ignored.
            - Else return_items has to be a MULTIPLE of this!!!
        return_items (int | str): Specifies the amount of matches that will be gathered. It can be: (default=100) 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
    """
    
    hub_id = "801f7e0c-1064-4dd1-a960-b2f54f8b5193"
    
    async def fetch_matches(start: int, count: int) -> Dict[str, Any]:
        """Helper function to fetch match data asynchronously."""
        return await safe_api_call(faceit_data.hub_matches, hub_id, starting_item_position=int(start), return_items=int(count))
    
    data: Dict[str, Any] = {"items": []}
    
    ## Gather the matches data depended on the input argument given
    if return_items == "ALL": # Gather all matches from the hub
        i = 0
        while True:
            data_batch = await fetch_matches(i, 100)
            if isinstance(data_batch, dict) and "items" in data_batch:
                data['items'].extend(data_batch['items'])
                if len(data_batch['items']) < 100: # No more matches available
                    break
            else:
                print(f"Error occurred, received error code: {data_batch}")
                break
            
            i += 100
    
    elif isinstance(return_items, int) and isinstance(starting_item_position, int):
        if return_items > 100:
            requested_count = return_items
            amount_called = 0
            while requested_count > 0:
                batch = min(requested_count, 100)
                data_batch = await fetch_matches(amount_called, batch)
                
                if isinstance(data_batch, dict) and "items" in data_batch:
                    data["items"].extend(data_batch["items"])
                else:
                    print(f"Error occurred, received error code: {data_batch}")
                    break
                
                requested_count -= batch
                amount_called += batch

        else:
            data = await fetch_matches(starting_item_position, return_items)
            if not isinstance(data, dict) or "items" not in data:
                print(f"Error occurred, received error code: {data}")
                return []
            
    else:
        raise TypeError(f"return_items: ({return_items}) must be an int or 'ALL'")
    
        
    #Remove both the "items" indent and the match if the status is not "FINISHED"
    return [match for match in data["items"] if (match["status"] == "FINISHED" )]

## Run this code only when program is run directly
if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

    team_ids = gather_team_ids_json()
    
    # datasets = process_esea_data(team_ids)
    
    hub_data = process_esea_data(team_ids, teams_to_return='ALL')
    
    print(hub_data)
    
    
    
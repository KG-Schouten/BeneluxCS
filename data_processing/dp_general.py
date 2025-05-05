# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
import re

# API imports
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.async_progress import gather_with_progress

def modify_keys(d):
    """
    Modifies the keys of a dataframe or a dictionary by replacing non-alphanumeric characters with underscores.
    """
    
    def clean_key(key):
        # Replace percent signs with 'percent'
        key = key.replace('%', 'percent')
        
        # replace non-alphanumeric characters with underscores
        key = re.sub(r'[^a-zA-Z0-9_]', '_', key)
        
        # if starts with a digit
        if re.match(r'^\d', key):
            key = '_' + key
            
        return key
    ## Check if the input is a dataframe
    if isinstance(d, pd.DataFrame):
        d = d.rename(columns=lambda x: clean_key(x))
        return d
    
    elif isinstance(d, pd.Series):
        d = d.rename(lambda x: clean_key(x))
        return d
    
    elif isinstance(d, dict):
        # Create a new dict with modified keys
        return {clean_key(key): modify_keys(value) if isinstance(value, (dict, list)) else value for key, value in d.items()}

    elif isinstance(d, list):
        # Create a new list with modified dicts
        return [modify_keys(item) if isinstance(item, dict) else item for item in d]
    
    else: 
        print("Input was not of a valid type so returned it")
        return d

### -----------------------------------------------------------------
### Match processing functions
### -----------------------------------------------------------------

async def process_matches(match_ids,  event_ids, faceit_data: FaceitData, faceit_data_v1: FaceitData_v1) -> None:
    """
    Main function to process matches and gather all relevant data.
    
    Args:
        match_ids (list): List of match IDs to process
        event_ids (list): List of event IDs corresponding to the match IDs
        faceit_data (FaceitData): FaceitData object for API calls
        faceit_data_v1 (FaceitData_v1): FaceitData_v1 object for API calls
    
    Returns:
        tuple:
            - df_matches (pd.DataFrame): DataFrame containing match details
            - df_teams_matches (pd.DataFrame): DataFrame containing team details per match
            - df_teams (pd.DataFrame): DataFrame containing team details
            - df_maps (pd.DataFrame): DataFrame containing map details
            - df_teams_maps (pd.DataFrame): DataFrame containing team stats per map
            - df_players_stats (pd.DataFrame): DataFrame containing player stats per map
            - df_players (pd.DataFrame): DataFrame containing player details
    """
    
    ## Match details, team/match details
    df_matches, df_teams_matches = await process_match_details_batch(match_ids, faceit_data=faceit_data, event_ids=event_ids)
    
    ## Team details
    team_ids = [team_id for team_id in df_teams_matches['team_id'].unique() if pd.notna(team_id) and team_id != '']
    team_ids = list(set(team_ids))  # Remove duplicates
    if team_ids:
        df_teams = await process_team_details_batch(team_ids, faceit_data=faceit_data)
    else:
        print("No teams found.")
        df_teams = pd.DataFrame()
    
    ## Map details, team/map details, player stats, player details
    match_ids = [match_id for match_id in df_matches['match_id'].loc[df_matches['status'] == 'FINISHED'].unique() if pd.notna(match_id) and match_id != '']
    match_ids = list(set(match_ids))  # Remove duplicates
    if match_ids:
        df_maps, df_teams_maps, df_players_stats = await process_match_stats_batch(match_ids, faceit_data=faceit_data)
        
        # Player details
        player_ids = [player_id for player_id in df_players_stats['player_id'].unique() if pd.notna(player_id) and player_id != '']
        player_ids = list(set(player_ids))  # Remove duplicates
        if player_ids:
            df_players = await process_player_details_batch(player_ids, faceit_data_v1=faceit_data_v1)
        else:
            print("No players found.")
            df_players = pd.DataFrame()
    else:
        print("No finished matches found.")
        df_maps, df_teams_maps, df_players_stats, df_players = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Remove any empty rows from the DataFrames
    df_matches = df_matches.dropna(how='all')
    df_teams_matches = df_teams_matches.dropna(how='all')
    df_teams = df_teams.dropna(how='all')
    df_maps = df_maps.dropna(how='all')
    df_teams_maps = df_teams_maps.dropna(how='all')
    df_players_stats = df_players_stats.dropna(how='all')
    df_players = df_players.dropna(how='all')
    
    return df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players

async def process_match_details_batch(match_ids: list[str], faceit_data: FaceitData, **kwargs) -> pd.DataFrame:
    """
    Processes match details for a batch of match IDs.

    Args:
        match_ids (list): List of match IDs to process
        faceit_data (FaceitData): FaceitData object for API calls
        **kwargs: Additional arguments for processing
            - event_ids (list): The ID of the event to filter matches by (so event_id/championship_id for ESEA)
    
    Returns:
        df_matches (pd.DataFrame): DataFrame containing match details
    """
    event_ids = kwargs.get('event_ids', [])
    tasks = [process_match_details(match_id=match_id, event_id=event_id, faceit_data=faceit_data) for match_id, event_id in zip(match_ids, event_ids)]
    results = await gather_with_progress(tasks, desc="Processing match details", unit="matches")

    df_matches = pd.DataFrame([
        row[0]
        for row in results or []
        if row and isinstance(row, (list, tuple)) and len(row) > 1 and row[1]
    ])
    df_teams_matches = pd.DataFrame([
        item
        for row in results or [] 
        if row and isinstance(row, (list, tuple)) and len(row) > 1 and row[1]
        for item in row[1] if item is not None
    ])
    
    # Modify the keys to work with the database
    df_matches = modify_keys(df_matches)
    df_teams_matches = modify_keys(df_teams_matches)
    
    return df_matches, df_teams_matches

async def process_match_details(match_id: str, event_id, faceit_data: FaceitData) -> dict:
    try:
            match_details = await faceit_data.match_details(match_id)
            
            if match_details.get('status', None) == 'SCHEDULED':
                match_time = match_details.get('scheduled_at', None)
                winning_id = None
            else:
                match_time = match_details.get('configured_at', None)
                
                if match_details.get('results', None):
                    winning_fac = match_details['results'].get('winner', None)
                    winning_id = match_details['teams'][winning_fac]['faction_id']
                else:
                    winning_id = None
            
            match_dict = {
                "match_id": match_id,
                "event_id": event_id,
                "competition_id": match_details.get("competition_id", None),
                "competition_type": match_details.get("competition_type", None),
                "competition_name": match_details.get("competition_name", None),
                "organizer_id": match_details.get("organizer_id", None),
                "match_time": match_time,
                "best_of": match_details.get("best_of", None),
                "winner_id": winning_id,
                "status": match_details.get("status", None),
                "round": match_details.get("round", None),
                "group_id": match_details.get("group", None),
                "demo_url": match_details.get("demo_url", None),
            }
            match_team_list = []
            for team in match_details['teams'].values():
                match_team_dict = {
                    "match_id": match_id,
                    "team_id": team['faction_id'],
                    "team_name": team.get("name", None),
                    "avatar": team.get("avatar", None),
                }
                match_team_list.append(match_team_dict)

            return match_dict, match_team_list
    except Exception as e:
        print(f"Error processing match ID {match_id}: {e}")
        return None

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
    
    # Modify the keys to work with the database
    df_maps = modify_keys(df_maps)
    df_teams_maps = modify_keys(df_teams_maps)
    df_players_stats = modify_keys(df_players_stats)

    # Add HLTV rating to player stats
    if not df_players_stats.empty and not df_maps.empty:
        df_players_stats = calculate_hltv(df_players_stats, df_maps)
    
    return df_maps, df_teams_maps, df_players_stats

async def process_match_stats(match_id, faceit_data: FaceitData) -> tuple[list[dict], list[dict], list[dict]]:
    try:
        match_stats = await faceit_data.match_stats(match_id)
        
        if match_stats == 404:
            return None
        else:
            ## Check for replayed matches
            seen = {}
            for round_data in match_stats.get('rounds', [{}]):
                match_round = round_data.get('match_round', None)
                seen[match_round] = round_data
            match_stats['rounds'] = sorted(seen.values(), key=lambda x: int(x.get('match_round', 0)))
            
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
        match_round = string_to_number(map.get("match_round", None))
        
        sorted_keys = sorted(map['round_stats'].keys())
        map_dict = {
            "match_id": match_id,
            "match_round": match_round,
            "best_of": map.get("best_of", None),
            **{key.lower(): string_to_number(map['round_stats'][key]) for key in sorted_keys}
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
                **{key.lower(): string_to_number(team['team_stats'][key]) for key in sorted_keys}
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
                    **{key.lower(): string_to_number(player['player_stats'][key]) for key in sorted_keys}
                }
                player_stats_list.append(player_stats_dict)
    
    return map_list, team_map_list, player_stats_list

def calculate_hltv(df_players_stats: pd.DataFrame, df_maps: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate HLTV rating for players based on their stats.
    
    Args:
        df_players_stats (pd.DataFrame): DataFrame containing player stats
        df_maps (pd.DataFrame): DataFrame containing map details
    
    Returns:
        df_players_stats (pd.DataFrame): DataFrame with added HLTV rating
    """
    try:
        # Merge the player stats with map details to get the round count
        df_merged = pd.merge(df_players_stats, df_maps[['match_id', 'match_round', 'rounds']], on=['match_id', 'match_round'], how='left')
        
        ## Calculate HLTV 1.0 rating
        avg_kpr = 0.679 # avg kill per round
        avg_spr = 0.317 # avg survived rounds per round
        avg_rmk = 1.277 # avg value calculated from rounds with multi-kills
        
        for idx, row in df_merged.iterrows():  
            try:
                kills = row['kills']
                deaths = row['deaths']
                rounds = int(row['rounds'])
                double = row['double_kills']
                triple = row['triple_kills']
                quadro = row['quadro_kills']
                penta = row['penta_kills']
                single = kills - (2*double + 3*triple + 4*quadro + 5*penta)

                kill_rating = kills / rounds / avg_kpr
                survival_rating = (rounds - deaths) / rounds / avg_spr
                rounds_with_multi_kill_rating = (single + 4*double + 9*triple + 16*quadro + 25*penta) / rounds / avg_rmk

                hltv_rating = (kill_rating + 0.7*survival_rating + rounds_with_multi_kill_rating) / 2.7
            except ZeroDivisionError:
                hltv_rating = 0.0
            except Exception as e:
                print(f"Error calculating HLTV rating for player {row['player_id']} in match {row['match_id']}: {e}")
                hltv_rating = 0.0
            
            # Add the calculated rating to the DataFrame
            df_players_stats.at[idx, 'hltv'] = round(hltv_rating, 2)
            
        return df_players_stats
    except Exception as e:
        print(f"Error calculating HLTV rating: {e}")
        return df_players_stats

def string_to_number(value):
    """ Tries to convert a string to an integer or float. If conversion fails, returns the original value."""
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value

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
    
    ## Modify the keys to work with the database
    df_players = modify_keys(df_players)
    
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
    
    ## Modify the dataframe to have the correct column names and df name
    df_teams = modify_keys(df_teams)
    
    return df_teams

async def process_team_details(team_id, faceit_data: FaceitData) -> dict:
    try:
        team_details = await faceit_data.team_details(team_id)
        if team_details == 404:
            team_dict = {
                'team_id': team_id,
                'team_name': None,
                'nickname': None,
                'avatar': None,
            }
        else:
            team_dict = {
                'team_id': team_details['team_id'],
                'team_name': team_details.get('name', None),
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
"""
The following structure is required for the event data (a dictionary with the following keys):

    - event_id (str): The ID of the event
    - stage_id (str): The ID of the stage (None if only one stage in event)
    - event_name (str): The name of the event
    - event_type (str): The type of the event (e.g., 'championship', 'championship_hub', 'championship_lan', 'hub', 'esea')
    - event_format (str): The format of the event (e.g., 'matchmaking', 'bracket (single elim)', 'doubleElimination', 'swiss', 'roundRobin', 'league')
    - event_description (str): The description of the event
    - event_avatar (str): The URL of the event avatar
    - event_banner (str): The URL of the event banner
    - event_start (str): The start time of the event
    - organizer_id (str): The ID of the event organizer
    - organizer_name (str): The name of the event organizer
    - maps (list): A list of maps associated with the event
"""
async def gather_event_details(event_id: str, event_type: str, faceit_data_v1: FaceitData_v1) -> pd.DataFrame:
    """
    Processing event data for a batch of event IDs.
    
    Args:
        event_id (str): The ID of the event to process
        event_type (str): The type of the event (e.g., 'championship', 'hub', 'esea')
        faceit_data_v1 (FaceitData_v1): FaceitData_v1 object for API calls
        
    Returns:
        df_events (pd.DataFrame): DataFrame containing event details:
        - event_id (str): The ID of the event
        - stage_id (str): None
        - event_name (str): The name of the event
        - event_type (str): The type of the event (e.g., 'championship', 'championship_hub', 'championship_lan', 'hub', 'esea')
        - event_format (str): The format of the event (e.g., 'matchmaking', 'singleElimination', 'doubleElimination', 'swiss', 'roundRobin')
        - event_description (str): The description of the event
        - event_avatar (str): The URL of the event avatar
        - event_banner (str): The URL of the event banner
        - event_start (str): The start time of the event
        - organizer_id (str): The ID of the event organizer
        - organizer_name (str): The name of the event organizer
        - maps (list): A list of maps associated with the event
    """
    if event_type == 'championship':
        try:
            details = await faceit_data_v1.championship_details(championship_id=event_id)  
            if isinstance(details, dict):
                details = details.get('payload', None)
                event_dict = {
                    "event_id": event_id,
                    "stage_id": 1,
                    "event_name": details.get('name', None),
                    "event_type": event_type,
                    "event_format": details.get('type', None),
                    "event_description": details.get('description', None),
                    "event_avatar": details.get('avatar', None),
                    "event_banner": details.get('coverImage', None),
                    "event_start": details.get('championshipStart', None),
                    "organizer_id": details.get('organizerId', None),
                    "organizer_name": details.get('organizer', {}).get('name', None),
                    "maps": [map['game_map_id'] for map in details.get('matchConfiguration', {}).get('tree', {}).get('map', {}).get('values', {}).get('value', {})]
                }
            else:
                print(f"Error code while fetching championship details: {details}")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching championship details: {e}")
            return pd.DataFrame()
            
        df_events = pd.DataFrame([event_dict])
        return df_events
    elif event_type == 'championship_hub' or event_type == 'championship_lan': # For goofy tournament organizers that have used club queues as tournament or for LAN tournaments. This requires an entry in the events.json file
        df_events = gather_event_details_json(event_id=event_id)
        return df_events
    elif event_type == 'hub':
        try:
            details = await faceit_data_v1.hub_details(hub_id=event_id)
            if isinstance(details, dict):
                details = details.get('payload', None)
                event_dict = {
                    "event_id": event_id,
                    "stage_id": 1,
                    "event_name": details.get('name', None),
                    "event_type": event_type,
                    "event_format": 'matchmaking',
                    "event_description": details.get('description', None),
                    "event_avatar": details.get('avatar', None),
                    "event_banner": details.get('coverImage', None),
                    "event_start": details.get('hubStart', None),
                    "organizer_id": details.get('organizerGuid', None),
                    "organizer_name": details.get('organizerName', None),
                    "maps": None
                }
            else:
                print(f"Error code while fetching hub details: {details}")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching hub details: {e}")
            return pd.DataFrame()
        
        df_events = pd.DataFrame([event_dict])
        
        ## Modify the keys to work with the database
        df_events = modify_keys(df_events)
        
        ## Give the dataframe a unique name
        df_events.name = "events"
        
        return df_events
    else:
        print(f"Unknown event type: {event_type}")
        return None

def gather_event_details_json(event_id: str) -> pd.DataFrame:
    """
    Gathers the event details from the manually created events.json file in the data folder
    
    Args:
        event_id (str): The ID of the event to gather details for
        
    Returns:
        - df_events (pd.DataFrame): DataFrame containing event details
        # - df_event_teams (pd.DataFrame): DataFrame containing event teams details
        # - df_event_stages (pd.DataFrame): DataFrame containing event stages details
        # - df_event_stage_matches (pd.DataFrame): DataFrame containing event stage matches details
    """
    # Load the event data from the JSON file
    events = load_event_data_json()

    event_list = []
    
    try:
        for event in events:
            if event['event_id'] == event_id:
                for stage in event.get('event_stages', {}):                    
                    event_list.append(
                        {
                            "event_id": event_id,
                            "stage_id": stage.get('stage_id', None),
                            "event_name": event.get('event_name', None) + " | " + stage.get('stage_name', None),
                            "event_type": event.get('event_type', None),
                            "event_format": stage.get('format', None),
                            "event_description": event.get('event_description', None),
                            "event_avatar": event.get('event_avatar', None),
                            "event_banner": event.get('event_banner', None),
                            "event_start": event.get('event_start', None),
                            "organizer_id": event.get('organizer_id', None),
                            "organizer_name": event.get('organizer_name', None),
                            "maps": event.get('maps', None)
                        }
                    )
                
                # # Not used yet, but can be used for future reference
                # event_stage_list, event_teams_list, event_stage_matches_list = [], [], []
                
                # for team in event.get('teams', []):
                #     event_teams_list.append(
                #         {
                #             "event_id": event['event_id'],
                #             "team_id": team.get('team_id'),
                #             "team_name": team.get('team_name', None),
                #             "avatar": team.get('avatar', None),
                #             "team_id_linked": team.get('team_id_linked', None),
                #         }
                #     )                    
                
                # for event_stage_name, event_stage in event.get('event_stages', {}).items():
                #     for stage in event_stage:
                #         event_stage_list.append(
                #             {
                #                 "event_id": event['event_id'],
                #                 "event_stage_id": stage.get('event_stage_id'),
                #                 "event_stage_name": event_stage_name,
                #                 "event_stage_type": stage.get('event_stage_type', None),
                #                 "event_stage_group_type": stage.get('event_stage_group_type', None),
                #                 "event_stage_team_ids": stage.get('teams', []),
                #             }
                #         )
                        
                #         for match in stage.get('matches', []):
                #             event_stage_matches_list.append(
                #                 {
                #                     "event_id": event['event_id'],
                #                     "event_stage_id": stage.get('event_stage_id'),
                #                     "match_id": match.get('match_id', None),
                #                     "round": match.get('round', None),
                #                     "winner_to": match.get('winner_to', {}),
                #                     "loser_to": match.get('loser_to', {}),
                #                 }
                #             )
        # Process into DataFrames  
        df_events = pd.DataFrame(event_list)
        # df_event_teams = pd.DataFrame(event_teams_list)
        # df_event_stages = pd.DataFrame(event_stage_list)
        # df_event_stage_matches = pd.DataFrame(event_stage_matches_list)
        
        ## Modify the keys to work with the database
        df_events = modify_keys(df_events)
        
        ## Give the dataframe a unique name
        df_events.name = "events"
        
        return df_events #, df_event_teams, df_event_stages, df_event_stage_matches
    except Exception as e:
        print(f"An unexpected error occurred while creating df_events for '{event_id}': {e}")
        return pd.DataFrame() #, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()   # Return an empty dictionary if any other error occurs
    
def load_event_data_json() -> dict:
    """
    Loads the event data from the events.json file in the data folder
    
    Returns:
        - events (dict): A dictionary containing the event data
    """
    
    BASE_DIR = os.path.dirname(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
    URL = os.path.join(BASE_DIR, 'data', 'events.json')

    try:
        with open(URL, 'r', encoding='utf-8') as f:
            events = json.load(f)
            return events
    except FileNotFoundError:
        print(f"File not found: {URL}")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from '{URL}'. Ensure the file contains valid JSON data.")
        return {}
    except Exception as e:
        print(f"An unexpected error occurred while loading team ids from json: {e}")
        return {}

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    df_events = gather_event_details_json(event_id="12096eed-0a0e-4abc-8ff1-69a121e4fc45")

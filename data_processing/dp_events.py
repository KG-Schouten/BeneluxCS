# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
from dateutil import parser

# API imports
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.sliding_window import RequestDispatcher, request_limit, interval, concurrency
from data_processing.faceit_api.async_progress import gather_with_progress

# dp imports
from data_processing.dp_general import *

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")

### -----------------------------------------------------------------
### ESEA Data Processing
### -----------------------------------------------------------------

async def process_esea_teams_data(**kwargs) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """The main function to process all data of the Benelux teams in ESEA.
    Args:
        **kwargs: Additional optional keyword arguments:
            - season_number ("ALL" | int | list[int])
            - season_id ("ALL" | int | list[int])
            - team_id (str | list[str])
            - match_amount ("ALL" | int)
            - match_amount_type ("ANY" | "TEAM" | "SEASON"):
            - from_timestamp (int | str): The start timestamp (UNIX) for the matches to be gathered. (default is 0)
    Returns:
        tuple:
            - df_seasons (pd.DataFrame): DataFrame containing season data
            - df_events (pd.DataFrame): DataFrame containing event data
            - df_teams_benelux (pd.DataFrame): DataFrame containing team data
            - df_matches (pd.DataFrame): DataFrame containing match data
            - df_teams_matches (pd.DataFrame): DataFrame containing team match data
            - df_teams (pd.DataFrame): DataFrame containing team details
            - df_maps (pd.DataFrame): DataFrame containing map data
            - df_teams_maps (pd.DataFrame): DataFrame containing team map data
            - df_players_stats (pd.DataFrame): DataFrame containing player stats data
            - df_players (pd.DataFrame): DataFrame containing player details
    """
    print(
    f"""
    ---------------------------------
        Processing ESEA team data
    ---------------------------------
    """
    )

    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        # Initialize the FaceitData object with the API token and dispatcher
        faceit_data = FaceitData(FACEIT_TOKEN, dispatcher)
        faceit_data_v1 = FaceitData_v1(dispatcher)
        
        ## Dataframe with the season data (df_seasons)
        df_seasons, df_events = await process_esea_season_data(faceit_data_v1=faceit_data_v1)
        df_seasons = df_seasons.loc[df_seasons['region_name'].isin(['Europe', 'North America'])] # Filter out to keep only EU and NA regions
        
        ## Dataframe with the benelux teams for each season and division (df_teams_benelux)
        df_teams_benelux = gather_team_ids_json(**kwargs) # Get the team ids from the json file
        if df_teams_benelux.empty or df_teams_benelux is None:
            print("No teams found in the json file for the given kwargs.")
            return None
        
        df_teams_benelux = df_teams_benelux.merge(
            df_seasons
            .groupby(['season_id', 'region_id', 'division_name'])['event_id']
            .agg(lambda x: list(x.unique()))
            .reset_index(),
            on=['season_id', 'region_id', 'division_name'],
            how='left'
        )
        
        ## Gathering the match ids
        df_esea_matches = await gather_esea_matches(df_teams_benelux, faceit_data_v1=faceit_data_v1) # Get the match ids from the teams
        
        match_amount = kwargs.get("match_amount", "ALL") # Get the match amount from the kwargs (default is "ALL")
        match_amount_type = kwargs.get("match_amount_type", "ANY") # Get the match amount type from the kwargs (default is "ANY")
        from_timestamp = kwargs.get("from_timestamp", 0) # Get the from timestamp from the kwargs (default is 0)
        
        if match_amount: # Only keep the matches that are specified in the kwargs
            df_esea_matches = pd.merge(df_esea_matches, df_seasons, on='event_id', how='left')
            if match_amount == "ALL":
                pass
            
            elif isinstance(match_amount, int):
                if match_amount_type == "ANY":
                    df_esea_matches = df_esea_matches.sort_values(by='match_time', ascending=False).head(match_amount).reset_index(drop=True)
                elif match_amount_type == "TEAM":
                    df_esea_matches = df_esea_matches.sort_values(by='match_time', ascending=False).groupby('team_id').head(match_amount).reset_index(drop=True)
                elif match_amount_type == "SEASON":
                    df_esea_matches = df_esea_matches.sort_values(by='match_time', ascending=False).groupby('season_id').head(match_amount).reset_index(drop=True)
                else:
                    print(f"Invalid match_amount_type value: {match_amount_type}. Must be 'ANY', 'TEAM' or 'SEASON'.")
                    return None
            else:
                print(f"Invalid match_amount type: {match_amount}. Must be an integer or 'ALL'.")
                return None
        
        ## Filter the matches based on the from_timestamp
        df_esea_matches = df_esea_matches.loc[df_esea_matches['match_time'] >= int(from_timestamp), :].reset_index(drop=True)
        
        # Create dataframe with match_id and event_id for unique match_ids
        df_matches_events = df_esea_matches[['match_id', 'event_id']].drop_duplicates()
        
        match_ids = df_matches_events['match_id'].to_list()
        event_ids = df_matches_events['event_id'].to_list()
        
        ## Processing matches in esea
        df_events = df_events.loc[df_events['event_id'].isin(event_ids), :].reset_index(drop=True)
        df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
            match_ids=match_ids, 
            event_ids=event_ids, 
            faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
        )
        
        ## Add team_id_linked_column to df_teams (and make it a copy of team_id)
        df_teams['team_id_linked'] = df_teams['team_id']
        
        ## Add internal_event_id to df_events adn df_matches (a combination of event_id and stage_id)
        df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)
        df_matches = df_matches.merge(
            df_events[['event_id', 'internal_event_id']],
            on='event_id',
            how='left'
        )
        
        # Explode df_teams_benelux on event_id
        df_teams_benelux= df_teams_benelux.explode('event_id')
        
        ## Modify the keys in all dataframes using modify_keys function
        df_seasons = modify_keys(df_seasons)
        df_events = modify_keys(df_events)
        df_teams_benelux = modify_keys(df_teams_benelux)
        
        ## Set the names of the dataframes
        df_seasons.name = "seasons"
        df_teams_benelux.name = "teams_benelux"
        df_events.name = "events"
        df_matches.name = "matches"
        df_teams_matches.name = "teams_matches"
        df_teams.name = "teams"
        df_maps.name = "maps"
        df_teams_maps.name = "teams_maps"
        df_players_stats.name = "players_stats"
        df_players.name = "players"  

    return df_seasons, df_events, df_teams_benelux, df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players

async def process_esea_season_data(faceit_data_v1: FaceitData_v1) -> pd.DataFrame:
    """ Gathers all of the esea seasons and """
    try:
        # Gather the data from the API
        league_seasons_data = await faceit_data_v1.league_seasons()
        league_data = await faceit_data_v1.league_details()

        season_ids = [season['id'] for season in league_seasons_data['payload']]
        season_numbers = [season['season_number'] for season in league_seasons_data['payload']]
        seasons = {szn_id : szn_number for szn_id, szn_number in zip(season_ids, season_numbers)}
        
        ## Processing df_seasons and df_events
        tasks = [faceit_data_v1.league_season_stages(season_id) for season_id in seasons.keys()]
        results = await gather_with_progress(tasks, desc="Processing ESEA seasons")
        
        rows, event_list = [], []
        for season_data, league_season_data in zip(results, league_seasons_data['payload']): 
            # Get the league season data from the dictionary inside payload where 'id' == season_id
            season_id = league_season_data['id']
            season_number = league_season_data['season_number']
        
            for region in season_data['payload']['regions']:
                region_id = region['id']
                region_name = region['name']
                
                for division in region['divisions']:
                    division_id = division['id']
                    division_name = division['name']
                    
                    for stage in division['stages']:
                        stage_id = stage['id']
                        stage_name = stage['name']

                        for conference in stage['conferences']:
                            conference_id = conference['id']
                            conference_name = conference['name']
                            championship_id = conference['championship_id']

                            rows.append(
                                {
                                    "season_id": season_id,
                                    "season_number": season_number,
                                    "region_id": region_id,
                                    "region_name": region_name,
                                    "division_id": division_id,
                                    "division_name": division_name,
                                    "stage_id": stage_id,
                                    "stage_name": stage_name,
                                    "conference_id": conference_id,
                                    "conference_name": conference_name,
                                    "event_id": championship_id,
                                }
                            )
                            event_list.append(
                                {
                                    "event_id": championship_id,
                                    "stage_id": 1,
                                    "event_name": f"ESEA League {season_number} | {division_name} | {region_name} {conference_name}",
                                    "event_type": "esea",
                                    "event_format": "league",
                                    "event_description": league_data['payload'].get('description', None),
                                    "event_avatar": league_data['payload'].get('organizer_details', {}).get('avatar_url', None),
                                    "event_banner": league_data['payload'].get('organizer_details', {}).get('cover_url', None),
                                    "event_start": parser.isoparse(league_season_data.get('time_start', None)).timestamp(),
                                    "organizer_id": league_data['payload'].get('organizer_details', {}).get('id', None),
                                    "organizer_name": league_data['payload'].get('organizer_details', {}).get('name', None),
                                    "maps": [map.get('name', None) for map in league_season_data.get('map_pool', [{}])[0].get('maps', [{}]) if map.get('name', None) is not None]
                                }
                            )
        
        # Create the dataframes
        df_seasons = pd.DataFrame(rows)    
        df_events = pd.DataFrame(event_list)
        
        return df_seasons, df_events
    
    except Exception as e:
        print(f"Error processing ESEA season data: {e}")
        return None

async def gather_esea_matches(df_teams_benelux: pd.DataFrame, faceit_data_v1: FaceitData_v1) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ 
    Gather the matches of all benelux teams in a season
    
    Returns:
        pd.DataFrame: A pandas dataframe containing the match data
        with the following columns:
        'match_id', 'event_id', 'match_time'
    """
    try:
        tasks = []
        
        tasks = [(team_id, faceit_data_v1.league_team_matches(team_id, event_id_list)) 
                 for team_id, event_id_list in zip(df_teams_benelux['team_id'], df_teams_benelux['event_id'])]
        team_ids, coroutines = zip(*tasks) # Unzip the tasks into team_ids and coroutines
        results = await gather_with_progress(coroutines, desc='Gathering league team match ids', unit='teams') # Run all tasks concurrently
        
        match_list = [
            {
                'match_id': match['origin']['id'],
                'team_id': team_id,
                'event_id': match.get('championshipId', None),
                'match_time': int(match['origin']['schedule'] / 1000) if match['origin'].get('schedule') is not None else None
            }
            for team_id, result in zip(team_ids, results)
            if isinstance(result, dict)
            if result['payload']['items'] is not None
            for match in result['payload']['items']
            if match['origin']['id'] is not None
            if not any(faction['id'] == 'bye' for faction in match['factions'])
        ]
        
        df_esea_matches = pd.DataFrame(match_list)
        return df_esea_matches
    except Exception as e:
        print(f"Error gathering ESEA match ids for teams in league: {e}")
        return None

def gather_team_ids_json(**kwargs) -> pd.DataFrame:
    """
    Reads a JSON file containing team names and their corresponding team IDs.

    Args:
        **kwargs: Additional optional keyword arguments:
            - season_number (str | int | list):
                - "ALL" to process all seasons
                - A specific season number (str | int)
                - A list of season numbers (str | int)
            - season_id (str | int | list): (Default is None)
                - "ALL" to process all seasons
                - A single season ID (as string)
                - A list of season IDs
            - team_id (str or list[str]): (Default is None)
                - A single team ID (as string)
                - A list of team IDs 
    Returns:
        pd.Dataframe: A pandas DataFrame containing the following columns:
            - season_id: The ID of the season
            - season_number: The number of the season
            - division_name: The name of the division
            - team_name: The name of the team
            - team_id: The ID of the team
    """
    # Data path
    # Path to the API keys file
    BASE_DIR = os.path.dirname(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
    URL = os.path.join(BASE_DIR, 'data_processing/data', 'league_teams.json')

    try:
        with open(URL, 'r', encoding='utf-8') as f:  # Open the file in read mode with UTF-8 encoding
            team_ids = json.load(f)  # Load the JSON content into a Python dictionary
            
            # Convert into a dataframe
            rows = []
            
            for season in team_ids.get('seasons', []):
                if isinstance(season, dict):
                    season_id = season.get('season_id', None)
                    season_number = season.get('season_number', None)
                    
                    for region in season.get('regions', []):
                        if isinstance(region, dict):
                            region_id = region.get('region_id', None)
                            region_name = region.get('region_name', None)
                            
                            for division in region.get('divisions', []):
                                if isinstance(division, dict):
                                    division_name = division.get('division_name', None)
                                    
                                    for team_name, teamID in division.get('teams', {}).items():
                                        rows.append({
                                            'season_id': season_id,
                                            'season_number': season_number,
                                            'region_id': region_id,
                                            'region_name': region_name,
                                            'division_name': division_name,
                                            'team_name': team_name,
                                            'team_id': teamID
                                        })   
            df_teams = pd.DataFrame(rows)
            
            # Load the kwargs 
            season_number = kwargs.get("season_number", None) # Get the season number from the kwargs (default is "ALL")
            season_id = kwargs.get("season_id", None) # Get the season id from the kwargs (default is None)
            team_id = kwargs.get("team_id", None) # Get the team id from the kwargs (default is None)
            # Filter the dataframe based on the kwargs
            if season_number is not None: # Filtering based on season_number
                
                if season_number == "ALL":
                    pass
                
                elif isinstance(season_number, str | int):
                    try:
                        season_number = int(season_number) # try to convert the season number to an integer

                        if season_number in df_teams['season_number'].values:
                            df_teams = df_teams[df_teams['season_number'] == season_number]
                        else:
                            print(f"Invalid season number: {season_number}. Not in the dataframe.")
                            return None
                    except ValueError:
                        print(f"Invalid season number: {season_number}. Must be an integer (or able to be converted to).")
                        return None
                
                elif isinstance(season_number, list): # Check if the season number is a list of integers
                    try:
                        if all(isinstance(int(season), int) for season in season_number):
                            season_numbers = [int(season) for season in season_number]
                            if all(season in df_teams['season_number'].values for season in season_numbers):
                                df_teams = df_teams[df_teams['season_number'].isin(season_numbers)]
                            else:
                                print(f"Invalid season number in list: {season_number}. Not in the dataframe.")
                                return None
                        else:
                            print(f"Invalid season number in list: {season_number}. Must be an integer (or able to be converted to).")
                            return None
                    except Exception as e:
                        print(f"Error while filtering season numbers from season_number list: {e}")
                        return None
                
                else:
                    print(f"Invalid season number type: {season_number}.")
                    return None
            elif season_id is not None:
                
                if season_id == "ALL":
                    pass
                
                elif isinstance(season_id, str):
                    if season_id in df_teams['season_id'].values:
                        df_teams = df_teams[df_teams['season_id'] == season_id]
                    else:
                        print(f"Invalid season id: {season_id}. Not in the dataframe.")
                        return None
                
                elif isinstance(season_id, list):
                    try:
                        if all(isinstance(szn_id, str) for szn_id in season_id):
                            if all(szn_id in df_teams['season_id'].values for szn_id in season_id):
                                df_teams = df_teams[df_teams['season_id'].isin(season_id)]
                            else:
                                print(f"Invalid season id in list: {season_id}. Not in the dataframe.")
                                return None
                        else:
                            print(f"Invalid season id in list: {season_id}. Must be a string.")
                            return None
                    except Exception as e:
                        print(f"Error while filtering season ids from season_id list: {e}")
                        return None
            
            if team_id is not None:
                if isinstance(team_id, str):
                    if team_id in df_teams['team_id'].values:
                        df_teams = df_teams[df_teams['team_id'] == team_id]
                    else:
                        print(f"Invalid team id: {team_id}. Not in the dataframe.")
                        return None
                
                elif isinstance(team_id, list):
                    try:
                        if all(isinstance(tid, str) for tid in team_id):
                            if all(tid in df_teams['team_id'].values for tid in team_id):
                                df_teams = df_teams[df_teams['team_id'].isin(team_id)]
                            else:
                                print(f"Invalid team id in list: {team_id}. Not in the dataframe.")
                                return None
                        else:
                            print(f"Invalid team id in list: {team_id}. Must be a string.")
                            return None
                    except Exception as e:
                        print(f"Error while filtering team ids from team_id list: {e}")
                        return None

            return df_teams
    except FileNotFoundError:
        print(f"Error: The file '{URL}' was not found.")
        return {}  # Return an empty dictionary if the file is missing
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from '{URL}'. Ensure the file contains valid JSON data.")
        return {}  # Return an empty dictionary if JSON parsing fails
    except Exception as e:
        print(f"An unexpected error occurred while loading team ids from json: {e}")
        return {}

### -----------------------------------------------------------------
### Hub Data Processing
### -----------------------------------------------------------------

async def process_hub_data(hub_id: str, items_to_return: int|str=100, **kwargs) -> pd.DataFrame:
    """
    The main function to process the data for a hub
    
    Args:
        hub_id (str): The ID of the hub to process data for
        items_to_return (int | str): Specifies the amount of matches that will be gathered. It can be: (default=100) 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
        **kwargs: Additional optional keyword arguments:
            - from_timestamp (int | str): The start timestamp (UNIX) for the matches to be gathered. (default is 0)
    Returns:
        tuple:
            - df_events: DataFrame containing event details
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
    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        # Initialize the FaceitData object with the API token and dispatcher
        faceit_data = FaceitData(FACEIT_TOKEN, dispatcher)
        faceit_data_v1 = FaceitData_v1(dispatcher)
        
        ## Gathering event details
        df_events = await gather_event_details(event_id=hub_id, event_type="hub", faceit_data_v1=faceit_data_v1)
        
        ## Gathering matches in hub
        df_hub_matches = await gather_hub_matches(hub_id, faceit_data=faceit_data)
        if items_to_return != "ALL":
            df_hub_matches = df_hub_matches.sort_values(by='match_time', ascending=False).head(items_to_return)
            
        ## Filter the matches based on the from_timestamp
        df_hub_matches = df_hub_matches.loc[df_hub_matches['match_time'] >= int(kwargs.get("from_timestamp", 0)), :].reset_index(drop=True)
        
        match_ids = df_hub_matches['match_id'].unique().tolist()
        
        # Processing matches in hub
        event_ids = [hub_id]*len(match_ids)
        df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
            match_ids=match_ids, 
            event_ids=event_ids, 
            faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
            )
        
        ## Add team_id_linked_column to df_teams (and make it a copy of team_id)
        df_teams['team_id_linked'] = df_teams['team_id']
        
        ## Add internal_event_id to df_events (a combination of event_id and stage_id)
        df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)
        df_matches = df_matches.merge(
            df_events[['event_id', 'internal_event_id']],
            on='event_id',
            how='left'
        )
        
        ## Modify the keys in all dataframes using modify_keys function
        df_events = modify_keys(df_events)
        
        # Set the names of the dataframes
        df_events.name = "events"
        df_matches.name = "matches"
        df_teams_matches.name = "teams_matches"
        df_teams.name = "teams"
        df_maps.name = "maps"
        df_teams_maps.name = "teams_maps"
        df_players_stats.name = "players_stats"
        df_players.name = "players"  
        
        
    return df_events, df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players

async def gather_hub_matches(hub_id: str, faceit_data: FaceitData):
    """
    Gathers all the hub matches from a specific hub
    
    Args:
        hub_id (str): The ID of the hub to gather matches from
        faceit_data (FaceitData): The FaceitData object to use for API calls
    
    Returns:
        df_hub_matches: DataFrame containing match data with columns:
            - match_id: The ID of the match
            - match_time: The time the match was configured
    """
    tasks = [faceit_data.hub_matches(hub_id=hub_id, starting_item_position=i, return_items=100) for i in range(0, 1000, 100)]
    results = await gather_with_progress(tasks, desc="Fetching hub matches", unit='matches')
    extracted_matches = [match for result in results if isinstance(result, dict) and result['items'] for match in result['items']]
    
    ## Getting df_hub_matches and df_hub_teams_matches
    match_list = []
    for match in extracted_matches:
        if match.get('status') != "CANCELLED":
            match_id = match.get('match_id')
            # Create a dictionary for the match
            match_dict = {
                "match_id": match_id,
                "match_time": match.get('configured_at', None),        
            }
            match_list.append(match_dict)
                  
    df_hub_matches = pd.DataFrame(match_list)
    return df_hub_matches

### --------------------------------------------------------------------------------------------------------
### Championship Data Processing (also includes championships that are hosted in hub queues and LANs)
### --------------------------------------------------------------------------------------------------------

async def process_championship_data(championship_id: str, event_type: str, items_to_return: int|str="ALL", **kwargs) -> pd.DataFrame:
    """
    The main function to process the data for a championship
    
    Args:
        championship_id (str): The ID of the championship to process data for
        event_type (str): The type of the event (e.g., championship, championship_hub, championship_lan)
        items_to_return (int | str): Specifies the amount of matches that will be gathered. It can be: (default="ALL") 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
        **kwargs: Additional optional keyword arguments:
            - from_timestamp (int | str): The start timestamp (UNIX) for the matches to be gathered. (default is 0)
    Returns:
        tuple:
            - df_events: DataFrame containing event details
            - df_matches: DataFrame containing match data
            - df_teams_matches: DataFrame containing team match data
            - df_teams: DataFrame containing team details
            - df_maps: DataFrame containing map data
            - df_teams_maps: DataFrame containing team map data
            - df_players_stats: DataFrame containing player statistics
            - df_championship_players: DataFrame containing player details
    """
    print(
    f""" 
    -------------------------------------
        Processing Championship Data:
    -------------------------------------
    """
    )
    
    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        # Initialize the FaceitData object with the API token and dispatcher
        faceit_data = FaceitData(FACEIT_TOKEN, dispatcher)
        faceit_data_v1 = FaceitData_v1(dispatcher)
        
        ## Gathering event details
        df_events = await gather_event_details(event_id=championship_id, event_type=event_type, faceit_data_v1=faceit_data_v1)
        
        ## Gathering match ids
        if event_type == "championship":
            ## Gathering matches in championship
            df_championship_matches = await gather_championship_matches(championship_id, faceit_data=faceit_data)
            if items_to_return != "ALL":
                df_championship_matches = df_championship_matches.sort_values(by='match_time', ascending=False).head(items_to_return)
                
            ## Filter the matches based on the from_timestamp
            df_championship_matches = df_championship_matches.loc[df_championship_matches['match_time'] >= int(kwargs.get("from_timestamp", 0)), :].reset_index(drop=True)
            match_ids = df_championship_matches['match_id'].unique().tolist()
            
        elif event_type == "championship_hub":
            ## Gathering matches in championship hub
            match_ids = await gather_championship_hub_matches(championship_id)

        elif event_type == "championship_lan":
            # ADD LATER
            print("championship_lan event type is not yet supported.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
        else:
            print(f"Invalid event type: {event_type}. Must be 'championship', 'championship_hub' or 'championship_lan'.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        ## Processing matches in championship
        event_ids = [championship_id]*len(match_ids)
        df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
            match_ids=match_ids, 
            event_ids=event_ids,
            faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
        )
        
        # Add team_id_linked_column to df_teams (and make it a copy of team_id)
        df_teams['team_id_linked'] = df_teams['team_id']
        
        ## Add additional data from the events.json file to the dataframes if championship_hub event type
        if event_type == "championship_hub":
            events = load_event_data_json()
            for event in events:
                if event['event_id'] == championship_id:
                    # add round/group ids to df_matches
                    for stage in event.get('event_stages', [{}]):
                        for group in stage.get('groups', [{}]):
                            group_id = group.get('group_id', None)
                            for match in group.get('matches', [{}]):
                                match_id = match.get('match_id', None)
                                round = match.get('round', None)
                                
                                if match_id and match_id in df_matches['match_id'].values:
                                    df_matches.loc[df_matches['match_id'] == match_id, 'group_id'] = group_id
                                    df_matches.loc[df_matches['match_id'] == match_id, 'round'] = round
                    
                    ## add correct team details to df_teams
                    for team in event.get('teams', [{}]):
                        team_id = team.get('team_id', None)
                        team_name = team.get('team_name', None)
                        avatar = team.get('avatar', None)
                        team_id_linked = team.get('team_id_linked', None)
                        
                        if team_id and team_id in df_teams['team_id'].values:
                            if team_name:
                                df_teams.loc[df_teams['team_id'] == team_id, 'team_name'] = team_name
                            if avatar:
                                df_teams.loc[df_teams['team_id'] == team_id, 'avatar'] = avatar
                            if team_id_linked:
                                df_teams.loc[df_teams['team_id'] == team_id, 'team_id_linked'] = team_id_linked
                    break
            else:
                print(f"Event ID {championship_id} not found in the events data.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        ## Add internal_event_id to df_events (a combination of event_id and stage_id)
        df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)
        
        ## Add internal_event_id to df_matches (a combination of the event_id in df_matches and the corresponding stage_id in df_events *it will be unique*)
        if event_type == "championship_hub":
            events = load_event_data_json()
            for event in events:
                if event['event_id'] == championship_id:
                    match_event_stage_list = []
                    for stage in event.get('event_stages', [{}]):
                        stage_id = stage.get('stage_id', None)
                        for group in stage.get('groups', [{}]):
                            for match in group.get('matches', [{}]):
                                match_id = match.get('match_id', None)
                                match_event_stage_list.append(
                                    {
                                        "match_id": match_id,
                                        "event_id": championship_id,
                                        "stage_id": stage_id,
                                    }
                                )
                    ## Create df with match_id, event_id and stage_id
                    df_match_event_stage = pd.DataFrame(match_event_stage_list)
                    
                    df_matches['internal_event_id'] = df_matches['event_id'].astype(str) + "_" + df_match_event_stage.loc[df_match_event_stage['match_id'].isin(df_matches['match_id']), 'stage_id'].astype(str).values
        elif event_type == "championship":
            df_matches['internal_event_id'] = df_matches['event_id'].astype(str) + "_" + df_events.loc[df_events['event_id'].isin(df_matches['event_id']), 'stage_id'].astype(str).values
   
        # Modify the keys in all dataframes using modify_keys function
        df_events = modify_keys(df_events)
        
        # Rename the dataframes
        df_events.name = "events"
        df_matches.name = "matches"
        df_teams_matches.name = "teams_matches"
        df_teams.name = "teams"
        df_maps.name = "maps"
        df_teams_maps.name = "teams_maps"
        df_players_stats.name = "players_stats"
        df_players.name = "players"  
        
    return df_events, df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players

async def gather_championship_matches(championship_id: str, faceit_data: FaceitData):
    """
    Gathers all the championship matches from a specific championship
    
    Args:
        championship_id (str): The ID of the championship to gather matches from
        faceit_data (FaceitData): The FaceitData object to use for API calls
    
    Returns:
        df_championship_matches: DataFrame containing match data with columns:
            - match_id: The ID of the match
            - match_time: The time the match was configured
    """
    tasks = [faceit_data.championship_matches(championship_id=championship_id, starting_item_position=i, return_items=100) for i in range(0, 1000, 100)]
    results = await gather_with_progress(tasks, desc="Fetching championship matches", unit='matches')
    extracted_matches = [match for result in results if isinstance(result, dict) and result['items'] for match in result['items']]
    
    ## Getting df_championship_matches and df_championship_teams_matches
    match_list = []
    for match in extracted_matches:
        if match.get('status') != "CANCELLED":
            match_id = match.get('match_id')
            # Create a dictionary for the match
            match_dict = {
                "match_id": match_id,
                "match_time": match.get('configured_at', None),        
            }
            match_list.append(match_dict)
                  
    df_championship_matches = pd.DataFrame(match_list)
    return df_championship_matches

async def gather_championship_hub_matches(championship_id: str):
    try:
        events = load_event_data_json()
        
        match_ids = []
        for event in events:
            if event['event_id'] == championship_id:
                for stage in event.get('event_stages', []):
                    for group in stage.get('groups', []):
                        match_ids.extend([match.get('match_id', None) for match in group.get('matches', [{}]) if match.get('match_id', None) is not None and match.get('match_id', None) != ""])

                return match_ids
            else:
                print(f"Event ID {championship_id} not found in the events data.")
                return None
    except Exception as e:
        print(f"Error gathering championship hub matches: {e}")
        return None

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
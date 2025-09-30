# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
from dateutil import parser
from typing import List, Union

# API imports
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.sliding_window import RequestDispatcher, request_limit, interval, concurrency
from data_processing.faceit_api.async_progress import gather_with_progress
from data_processing.faceit_api.logging_config import function_logger

# dp imports
from data_processing.dp_general import process_matches, modify_keys, gather_event_details

# db imports
from database.db_down_update import gather_event_players

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")

### -----------------------------------------------------------------
### ESEA Data Processing
### -----------------------------------------------------------------

async def process_esea_teams_data(**kwargs) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """The main function to process all data of the Benelux teams in ESEA.
    Args:
        **kwargs: Additional optional keyword arguments:
            - season_number ("ALL" | int | list[int])
            - season_id ("ALL" | int | list[int])
            - team_id (str | list[str])
            - match_amount ("ALL" | int)
            - match_amount_type ("ANY" | "TEAM" | "SEASON"):
            - from_timestamp (int | str): The start timestamp (UNIX) for the matches to be gathered. (default is 0)
            - event_status (list | str): "ALL" | "ONGOING" | "PAST" | "UPCOMING"
    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
    """
    print("--- Processing ESEA Teams Data ---")

    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        # Initialize the FaceitData object with the API token and dispatcher
        async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1: # Use the FaceitData context manager to ensure the session is closed properly
            df_seasons, df_events = await process_esea_season_data(faceit_data_v1=faceit_data_v1)
            
            ## Add internal_event_id to df_events and df_matches (a combination of event_id and stage_id)
            df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)
            
            df_teams_benelux = await process_teams_benelux_esea(
                faceit_data_v1=faceit_data_v1, 
                season_number=kwargs.get('season_number', 'ALL'),
                team_id=kwargs.get('team_id', 'ALL')
            )
            
            ## Gathering the match ids
            team_ids = df_teams_benelux['team_id'].to_list()
            event_ids = df_teams_benelux['event_id'].tolist()
            df_esea_matches = await gather_esea_matches(
                team_ids, 
                event_ids, 
                faceit_data_v1=faceit_data_v1, 
                match_amount=kwargs.get('match_amount', 'ALL'),
                match_amount_type=kwargs.get('match_amount_type', 'ANY'),
                from_timestamp=kwargs.get('from_timestamp', 0),
                event_status=kwargs.get('event_status', 'ALL'),
                df_events=df_events
            ) # Get the match ids from the teams
            
            if df_esea_matches.empty:
                msg = "No matches found for the given criteria"
                function_logger.warning(msg)
                return df_seasons, df_events, df_teams_benelux, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            # confirm column
            if 'match_id' not in df_esea_matches.columns or 'event_id' not in df_esea_matches.columns:
                msg = "The DataFrame df_esea_matches does not contain the required columns 'match_id' and 'event_id'."
                function_logger.critical(msg)
                raise ValueError(msg)
            
            # Filter df_esea_matches to remove duplicate (match_id, event_id) pairs and rows where either or is None        
            df_matches_events = df_esea_matches[['match_id', 'event_id']].drop_duplicates()
            df_matches_events = df_matches_events.dropna(subset=['match_id', 'event_id'])
            
            match_ids = df_matches_events['match_id'].to_list()
            event_ids = df_matches_events['event_id'].to_list()
            
            ## Processing matches in esea
            df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
                match_ids=match_ids, 
                event_ids=event_ids, 
                faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
            )
            
            if df_matches.empty:
                msg = "No matches found for the given criteria"
                function_logger.warning(msg)
                return df_seasons, df_events, df_teams_benelux, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            if 'event_id' not in df_matches.columns:
                msg = "The DataFrame df_matches does not contain the required columns 'event_id'"
                function_logger.critical(msg)
                raise ValueError(msg)
            
            df_matches = df_matches.merge(
                df_events[['event_id', 'internal_event_id']],
                on='event_id',
                how='left'
            )
                
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

async def process_esea_season_data(faceit_data_v1: FaceitData_v1) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ Gathers all of the esea seasons and """
    try:
        # Gather the data from the API
        league_seasons_data = await faceit_data_v1.league_seasons()
        league_data = await faceit_data_v1.league_details()
        
        if not isinstance(league_data, dict):
            function_logger.critical(f"league_data is not a dict: {league_data}")
            raise TypeError(f"league_data is not a dictionary: {league_data}")
        else:
            if not league_data.get('payload'):
                function_logger.critical(f"No data found in league_data dict: {league_data}")
                raise ValueError(f"No data found in league_data dict: {league_data}")
        
        if not isinstance(league_seasons_data, dict):
            function_logger.critical(f"league_seasons_data is not a dict: {league_seasons_data}")
            raise TypeError(f"league_seasons_data is not a dictionary: {league_seasons_data}")
        else:
            if not league_seasons_data.get('payload'):
                function_logger.critical(f"No data found in league_seasons_data dict: {league_seasons_data}")
                raise ValueError(f"No data found in league_seasons_data dict: {league_seasons_data}")       
        
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
                                    "event_start": round(parser.isoparse(league_season_data.get('time_start', None)).timestamp()),
                                    "event_end": round(parser.isoparse(league_season_data.get('time_end', None)).timestamp()),
                                    "organizer_id": league_data['payload'].get('organizer_details', {}).get('id', None),
                                    "organizer_name": league_data['payload'].get('organizer_details', {}).get('name', None),
                                    "maps": [map.get('name', None) for map in league_season_data.get('map_pool', [{}])[0].get('maps', [{}]) if map.get('name', None) is not None]
                                }
                            )
        
        # Create the dataframes
        df_seasons = pd.DataFrame(rows)    
        df_events = pd.DataFrame(event_list)
        
        ## Modify the keys in all dataframes using modify_keys function
        df_seasons = modify_keys(df_seasons)
        df_events = modify_keys(df_events)
        
        if not isinstance(df_seasons, pd.DataFrame):
            function_logger.critical(f"df_seasons is not a DataFrame: {df_seasons}")
            raise TypeError(f"df_seasons is not a DataFrame: {df_seasons}")
        if not isinstance(df_events, pd.DataFrame):
            function_logger.critical(f"df_events is not a DataFrame: {df_events}")
            raise TypeError(f"df_events is not a DataFrame: {df_events}")
        
        return df_seasons, df_events
    
    except Exception as e:
        function_logger.critical(f"Error processing ESEA seasons data: {e}")
        raise

async def process_teams_benelux_esea(
    faceit_data_v1: FaceitData_v1,
    season_number: Union[str, int, List[Union[str, int]]] = "ALL",
    team_id: Union[str, List[str]] = "ALL",
    ) -> pd.DataFrame:
    """ Gathers the df_seasons, df_events and df_teams_benelux dataframes"""
    df_teams_benelux = read_league_teams_json(
        season_number=season_number,
        team_id=team_id
    ) # Get the team ids from the json file
    
    if not isinstance(df_teams_benelux, pd.DataFrame) or df_teams_benelux is None:
        raise TypeError(f"df_teams_benelux is not a DataFrame: {df_teams_benelux}")
    else:
        if df_teams_benelux.empty:
            raise ValueError(f"No teams found in the Benelux json for the specified season(s). {df_teams_benelux}")
    
    # Create the season standings dataframe
    team_ids = df_teams_benelux['team_id'].unique().tolist() # Get the unique team ids from the dataframe
    tasks = [process_league_team_season_standings(team_id, faceit_data_v1) for team_id in team_ids]
    league_team_season_standings = await gather_with_progress(tasks, desc="Processing league team details", unit="teams")
    
    if not isinstance(league_team_season_standings, list):
        raise TypeError(f"league_team_season_standings is not a list: {league_team_season_standings}")
    else:
        if not league_team_season_standings:
            raise ValueError("No league team season standings found. Please check the input parameters.")
        else:
            # Check if all items in the list are empty lists
            if all(isinstance(item, list) and not item for item in league_team_season_standings):
                raise ValueError("All league team season standings are empty lists. Please check the input parameters.")
            # Check if all items in the list are None
            if all(item is None for item in league_team_season_standings):
                raise ValueError("All league team season standings are None. Please check the input parameters.")
        
    df_league_team_season_standings = pd.DataFrame(
        [
            item 
            for sublist in league_team_season_standings
            if isinstance(sublist, list) and sublist
            for item in sublist
            if isinstance(item, dict) and item
        ]
    )
    
    if not isinstance(df_league_team_season_standings, pd.DataFrame) or df_league_team_season_standings is None:
        raise TypeError(f"df_league_team_season_standings is not a dataframe: {df_league_team_season_standings}")
    else:
        if df_league_team_season_standings.empty:
            raise ValueError("No league team season standings found. Please check the input parameters.")
    
    df_teams_benelux = df_teams_benelux.merge(
        df_league_team_season_standings,
        on=['team_id', 'season_number'],
        how='inner'
    )
    
    # Gather the players that have played matches for each team
    try:
        event_ids = df_teams_benelux['event_id'].tolist()
        team_ids = df_teams_benelux['team_id'].tolist()
        df_event_players = gather_event_players(event_ids=event_ids, team_ids=team_ids)
        
        # If the players_main list of a team in df_teams_benelux is smaller than the players in df_event_players, replace it and the sub lists
        if not df_event_players.empty:
            # Merge the dataframes on team_id and event_id
            merged = df_teams_benelux.merge(df_event_players, on=["team_id", "event_id"], suffixes=("_df1", "_df2"), how="left")
            
            def choose_players(row):
                try:
                    list_main_1 = row["players_main_df1"]
                    list_main_2 = row["players_main_df2"]
                    list_sub_1 = row["players_sub_df1"]
                    list_sub_2 = row["players_sub_df2"]
                    if isinstance(list_main_1, list) and isinstance(list_main_2, list) and list_main_2:
                        if not list_main_1:
                            return (list_main_2, list_sub_2)
                        if len(list_main_1) < len(list_main_2):    
                            return (list_main_2, list_sub_2)
                        else:
                            return (list_main_1, list_sub_1)
                    return (list_main_1, list_sub_1) if pd.notna(list_main_1) else (list_main_2, list_sub_2)
                except Exception as e:
                    function_logger.warning(f"Issue choosing players for team {row.get('team_name', 'Unknown')}: {e}")
                    return (row.get("players_main_df1", []), row.get("players_sub_df1", []))

            # Replace players_main where needed
            merged[["players_main", "players_sub"]] = merged.apply(
                lambda row: pd.Series(choose_players(row)), axis=1
            )
            
            if not merged.empty:
                df_teams_benelux = merged.drop(columns=[col for col in merged.columns if col.endswith('_df1') or col.endswith('_df2')])
            
    except Exception as e:
        function_logger.warning(f"Issue gathering alternative event players: {e}")
        pass
    
    df_teams_benelux = df_teams_benelux.drop('season_number', axis=1)
    
    df_teams_benelux = modify_keys(df_teams_benelux)
    
    if not isinstance(df_teams_benelux, pd.DataFrame):
        function_logger.critical(f"df_teams_benelux is not a DataFrame: {df_teams_benelux}")
        raise TypeError(f"df_teams_benelux is not a DataFrame: {df_teams_benelux}")

    return df_teams_benelux

def read_league_teams_json(
    season_number: Union[str, int, List[Union[str, int]]] = "ALL",
    team_id: Union[str, List[str]] = "ALL" 
    ) -> pd.DataFrame:
    """
    Loads team data from a JSON file and filters it based on optional parameters.

    Args:
        **kwargs:
            - season_number (str | int | list): "ALL", a season number, or list of them
            - team_id (str | list): A team ID or list of them

    Returns:
        pd.DataFrame: DataFrame with team details.
    """
    # Data path
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    DATA_PATH = os.path.join(BASE_DIR, 'data', 'league_teams.json')

    try:
        with open(DATA_PATH, 'r', encoding='utf-8') as f:  # Open the file in read mode with UTF-8 encoding
            data = json.load(f)  # Load the JSON content into a Python dictionary
            
        # Convert into a dataframe
        rows = [
            {
                'season_number': season.get('season_number'),
                'team_id': team_id,
                'team_name': team_dict.get('team_name'),
                'avatar': team_dict.get('avatar')
            }
            for season in data.get('seasons', []) if isinstance(season, dict)
            for team_id, team_dict in season.get('teams', {}).items()
        ] 
        
        df = pd.DataFrame(rows)
            
        # Apply filters if provided
        def validate_and_filter(df, column, value, convert=None):
            df_used = df.copy()  # Make a copy to avoid modifying the original DataFrame
            if value is None or value == "ALL":
                return df_used
            if isinstance(value, (str, int)):
                value = [convert(value) if convert else value]
            elif isinstance(value, list):
                if convert:
                    try:
                        value = [convert(v) for v in value]
                    except Exception:
                        raise TypeError(f"Invalid value in {column}: {value}. Expected a string, integer, or list of strings/integers.")
            else:
                raise TypeError(f"Invalid type for {column}: {type(value)}. Expected a string, integer, or list of strings/integers.")
            
            invalid = [v for v in value if v not in df_used[column].values]
            if invalid:
                raise ValueError(f"Invalid {column} values: {invalid}. Must be in the existing data.")
            
            df_used.loc[:, column] = df_used[column]  # Ensure it's not a view
            df_used = df_used[df_used[column].isin(value)]
            return df_used
        
        # Apply filters one by one
        df_filtered = validate_and_filter(df, 'season_number', season_number, convert=str)
        if not df_filtered.empty:
            df = df_filtered

        df_filtered = validate_and_filter(df, 'team_id', team_id)
        if not df_filtered.empty:
            df = df_filtered
        
        return df    
    
    except Exception as e:
        function_logger.critical(f"Error gathering team IDs from JSON: {e}")
        raise

async def process_league_team_season_standings(
    team_id, 
    faceit_data_v1: FaceitData_v1) -> list:
    try:
        data = await faceit_data_v1.league_team_details(team_id)

        if not isinstance(data, dict):
            msg = f"Expected a dictionary for team {team_id}, got: {type(data)} - {data}"
            raise TypeError(msg)

        if not data.get('payload'):
            msg = f"No payload found for team {team_id}: {data}"
            raise ValueError(msg)

        data_list = []

        for season in data['payload'][0].get('league_seasons_info', []):
            season_number = season.get('season_number')
            players_main, players_sub, players_coach = [], [], []

            for player in season.get('team_members', []):
                team_role = player.get('game_role')
                player_info = {
                    'player_id': player.get('user_id'),
                    'player_name': player.get('user_name')
                }

                try:
                    if team_role == "player":
                        players_main.append(player_info)
                    elif team_role == "substitute":
                        players_sub.append(player_info)
                    elif team_role == "coach":
                        players_coach.append(player_info)
                    else:
                        raise ValueError(f"Unknown team role '{team_role}' for player {player_info}")
                except Exception as e:
                    function_logger.warning(f"Error processing player {player_info}: {e}")

            for stage in season.get('season_standings', []):
                try:
                    data_list.append({
                        'team_id': team_id,
                        'event_id': stage.get('championship_id'),
                        'season_number': season_number,
                        'placement': stage.get('placement', {}),
                        'wins': stage.get('wins', 0),
                        'losses': stage.get('losses', 0),
                        'ties': stage.get('ties', 0),
                        'players_main': players_main,
                        'players_sub': players_sub,
                        'players_coach': players_coach
                    })
                except Exception as e:
                    function_logger.error(f"Error adding season stage data for team {team_id}, season {season_number}: {e}")

        return data_list

    except Exception as e:
        function_logger.error(f"Fatal error processing team {team_id}: {e}", exc_info=True)
        return []

async def gather_esea_matches(
    team_ids: list,
    event_ids: list,
    faceit_data_v1: FaceitData_v1,
    match_amount: Union[int, str] = "ALL",
    match_amount_type: str = "ANY",
    from_timestamp: int = 0,
    event_status: str | List[str] = "ALL",
    df_events: pd.DataFrame = pd.DataFrame()
) -> pd.DataFrame:
    """
    Gather ESEA match data and apply optional filtering.

    Args:
        team_ids (List[str]): List of Faceit team IDs.
        event_ids (List[str]): Corresponding list of ESEA event IDs.
        faceit_data_v1 (FaceitData_v1): Data source for fetching matches.
        match_amount (int | str): Number of matches to keep. 'ALL' for no filtering.
        match_amount_type (str): 'ANY' | 'TEAM'
        from_timestamp (int): Unix timestamp; only keep matches after this time.
        event_status (str | List[str]): Filter by event status. Options: 'ALL', 'ONGOING', 'PAST', 'UPCOMING'.

    Returns:
        pd.DataFrame: Filtered match data.
    """
    try:
        if not (isinstance(team_ids, list) and all(isinstance(tid, str) for tid in team_ids)):
            msg = f"team_ids must be a list of strings, got {type(team_ids)}: {team_ids}"
            function_logger.error(msg)
            raise TypeError(msg)

        if not isinstance(event_ids, list):
            msg = f"event_ids must be a list, got {type(event_ids)}: {event_ids}"
            function_logger.error(msg)
            raise TypeError(msg)

        for eid in event_ids:
            if isinstance(eid, str):
                continue
            if isinstance(eid, list):
                if not all(isinstance(eid_item, str) for eid_item in eid):
                    msg = f"All items in nested event_ids lists must be strings, got {event_ids}"
                    function_logger.error(msg)
                    raise TypeError(msg)
            else:
                msg = f"event_ids must be a list of strings or lists of strings, got {type(eid)} in {event_ids}"
                function_logger.error(msg)
                raise TypeError(msg)
                
        if len(team_ids) != len(event_ids):
            msg = f"team_ids and event_ids must be the same length. Got team_ids: {len(team_ids)} - event_ids: {len(event_ids)}."
            function_logger.error(msg)
            raise ValueError(msg)

        tasks = [
            gather_esea_matches_team(team_id, event_id, faceit_data_v1)
            for team_id, event_id in zip(team_ids, event_ids)
        ]
        results = await gather_with_progress(tasks, desc="Gathering ESEA matches", unit="teams")

        # Flatten and create DataFrame
        flat_list = [d for sublist in results for d in sublist]
        df = pd.DataFrame(flat_list)
        
        if df.empty:
            msg = f"No matches found for the provided team_ids: {team_ids} and event_ids: {event_ids}."
            function_logger.info(msg)
            return pd.DataFrame()
        else:
            df = filter_esea_matches(
                df=df,
                df_events=df_events,
                match_amount=match_amount,
                match_amount_type=match_amount_type,
                from_timestamp=from_timestamp,
                event_status=event_status
            )
            
            return df

    except Exception as e:
        function_logger.error(f"Error gathering ESEA matches: {e}", exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame on error

async def gather_esea_matches_team(
    team_id: str, 
    event_id: str | list[str], 
    faceit_data_v1: FaceitData_v1) -> list:
    """ Gathers the ESEA matches for a specific team and event(s)"""
    try:
        # team_id checks
        if not isinstance(team_id, str):
            msg = f"team_id must be a string, got {type(team_id)}"
            function_logger.critical(msg)
            raise TypeError(msg)
        
        # event_id checks
        if not isinstance(event_id, str):
            if not isinstance(event_id, list):
                msg =  f"event_id must be a string or a list of strings, got {type(event_id)}"
                function_logger.critical(msg)
                raise TypeError(msg)
            else:
                if not all(isinstance(eid, str) for eid in event_id) or not event_id:
                    msg = f"All items in event_id list must be strings, got {event_id}"
                    function_logger.critical(msg)
                    raise TypeError(msg)
        else:
            event_id = [event_id]      
        
        data = await faceit_data_v1.league_team_matches(team_id, event_id)

        if not isinstance(data, dict):
            msg = f"Expected a dictionary for team {team_id}, got: {type(data)} - {data}"
            function_logger.critical(msg)
            raise TypeError(msg)
        if not data.get('payload'):
            msg = f"No payload found for team {team_id}: {data}"
            function_logger.warning(msg)
            raise ValueError(msg)
        
        if not data['payload'].get('items'):
            msg = f"No matches found for team {team_id} in event(s) {event_id}: {data}"
            function_logger.info(msg)
            return []
        else:
            match_list = []
            for match in data['payload']['items']:
                    origin = match.get("origin", {})
                    match_id = origin.get("id")
                    
                    if not match_id:
                        function_logger.warning(f"Missing match ID for team {team_id}. Skipping.")
                        continue
                    if any(faction.get("id") == "bye" for faction in match.get("factions", [])):
                        function_logger.info(f"Bye match {match_id} for team {team_id}. Skipping.")
                        continue

                    schedule = origin.get("schedule")
                    match_time = int(schedule / 1000) if isinstance(schedule, (int, float)) else None

                    match_list.append({
                        "match_id": match_id,
                        "team_id": team_id,
                        "event_id": match.get("championshipId"),
                        "match_time": match_time,
                    })

            return match_list
        
    except Exception as e:
        function_logger.error(f"Fatal error processing team {team_id}: {e}", exc_info=True)
        return []

def filter_esea_matches(
    df: pd.DataFrame, 
    df_events: pd.DataFrame,
    match_amount: Union[int, str] = "ALL",
    match_amount_type: str = "ANY",
    from_timestamp: int = 0,
    event_status: str | List[str] = "ALL") -> pd.DataFrame:
    """ Filters the matches gathered from gather_esea_matches function based on the provided parameters."""
    
    try:
        # Merge with df_events if provided
        df_copy = df.copy()
        if not df_events.empty:
            try:          
                # df_events checks
                if not isinstance(df_events, pd.DataFrame):
                    msg = f"df_events must be a pandas DataFrame, got {type(df_events)}"
                    function_logger.critical(msg)
                    raise TypeError(msg)
                required_columns = ['event_id', 'event_start', 'event_end']
                if not all(col in df_events.columns for col in required_columns):
                    msg = f"df_events must contain the columns: {required_columns}. Found: {df_events.columns.tolist()}"
                    function_logger.critical(msg)
                    raise ValueError(msg)
                
                # df checks
                if 'event_id' not in df_copy.columns:
                    msg = "df must contain 'event_id' column to merge with df_events."
                    function_logger.critical(msg)
                    raise ValueError(msg)
                
                df_copy = df_copy.merge(df_events[['event_id', 'event_start', 'event_end']], on='event_id', how='left')
                
                # Filter by event status
                if isinstance(event_status, str):
                    event_status = [event_status.upper()]
                elif isinstance(event_status, list):
                    event_status = [status.upper() for status in event_status]
                else:
                    msg = f"Invalid event_status type: {type(event_status)}. Must be a string or a list of strings."
                    function_logger.critical(msg)
                    raise TypeError(msg)
                
                for status in event_status:
                    if status not in ["ALL", "ONGOING", "PAST", "UPCOMING"]:
                        msg = f"Invalid event_status: {status}. Must be 'ALL', 'ONGOING', 'PAST', or 'UPCOMING'. Skipping filter"
                        function_logger.error(msg)
                        continue
                    
                    if status == "ONGOING":
                        current_time = pd.Timestamp.now().timestamp()
                        df_copy = df_copy[(df_copy['event_start'] <= current_time) & (df_copy['event_end'] >= current_time)]
                    elif status == "PAST":
                        current_time = pd.Timestamp.now().timestamp()
                        df_copy = df_copy[df_copy['event_end'] < current_time]
                    elif status == "UPCOMING":
                        current_time = pd.Timestamp.now().timestamp()
                        df_copy = df_copy[df_copy['event_start'] > current_time]
                    
            except Exception as e:
                function_logger.critical(f"Error merging df_events to matches df: {e}", exc_info=True)
                df_copy = df.copy()  # Fallback to original df if merge fails
        else:
            msg = "No df_events provided, skipping merge and event_status filtering."
            function_logger.info(msg)
        
        # Filter by from_timestamp
        df_copy = df_copy[df_copy["match_time"] >= int(from_timestamp)].reset_index(drop=True)

        # Apply match amount filter
        if match_amount != "ALL":
            if not isinstance(match_amount, int):
                msg = f"Invalid match_amount: {match_amount}. Must be 'ALL' or int."
                function_logger.critical(msg)
                raise TypeError(msg)

            if match_amount_type not in {"ANY", "TEAM", "SEASON"}:
                msg = f"Invalid match_amount_type: {match_amount_type}. Must be 'ANY', 'TEAM', or 'SEASON'."
                function_logger.critical(msg)
                raise ValueError(msg)

            df_copy = df_copy.sort_values(by="match_time", ascending=False)

            if match_amount_type == "ANY":
                df_copy = df_copy.head(match_amount)
            elif match_amount_type == "TEAM":
                df_copy = df_copy.groupby("team_id", group_keys=False).head(match_amount)

            df_copy = df_copy.reset_index(drop=True)

        return df_copy
    
    except Exception as e:
        function_logger.critical(f"Error filtering ESEA matches: {e}", exc_info=True)
        return df

### -----------------------------------------------------------------
### Hub Data Processing
### -----------------------------------------------------------------

async def process_hub_data(hub_id: str, items_to_return: int|str=100, **kwargs) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
            - `df_teams`_matches: DataFrame containing team match data
            - df_teams: DataFrame containing team details
            - df_maps: DataFrame containing map data
            - df_teams_maps: DataFrame containing team map data
            - df_players_stats: DataFrame containing player statistics
            - df_hub_players: DataFrame containing player details
    """
    print(
    """ 
    -------------------------------------
        Processing Hub Data:
    -------------------------------------
    """
    )
    async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1: # Use the FaceitData context manager to ensure the session is closed properly
        
            ## Gathering event details
            df_events = await gather_event_details(event_id=hub_id, event_type="hub", faceit_data_v1=faceit_data_v1)
            
            ## Gathering matches in hub
            df_hub_matches = await gather_hub_matches(hub_id, faceit_data=faceit_data)
            if isinstance(items_to_return, str):
                if items_to_return.upper() == "ALL":
                    pass
                else:
                    function_logger.critical(f"Invalid items_to_return string: {items_to_return}. Must be an integer or 'ALL'.")
                    raise ValueError(f"Invalid items_to_return string: {items_to_return}. Must be an integer or 'ALL'.")
            elif isinstance(items_to_return, int):
                if items_to_return <= 0:
                    function_logger.critical(f"Invalid items_to_return value: {items_to_return}. Must be a positive integer or 'ALL'.")
                    raise ValueError(f"Invalid items_to_return value: {items_to_return}. Must be a positive integer or 'ALL'.")
                else:
                    df_hub_matches = df_hub_matches.sort_values(by='match_time', ascending=False).head(items_to_return)
            else:
                function_logger.critical(f"Invalid items_to_return type: {type(items_to_return)}. Must be an integer or 'ALL'.")
                raise TypeError(f"Invalid items_to_return type: {type(items_to_return)}. Must be an integer or 'ALL'.")
            
            ## Filter the matches based on the from_timestamp
            df_hub_matches = df_hub_matches.loc[df_hub_matches['match_time'] >= int(kwargs.get("from_timestamp", 0)), :].reset_index(drop=True)
            
            match_ids = df_hub_matches['match_id'].unique().tolist()
            
            if not match_ids:
                function_logger.warning(f"No matches found for hub_id {hub_id}. Returning empty dataframes.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            if not isinstance(match_ids, list):
                function_logger.critical(f"match_ids is not a list: {match_ids}")
                raise TypeError(f"match_ids is not a list: {match_ids}")
            
            # Processing matches in hub
            event_ids = [hub_id]*len(match_ids)
            df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
                match_ids=match_ids, 
                event_ids=event_ids, 
                faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
            )
            
            ## Add internal_event_id to df_events (a combination of event_id and stage_id)
            df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)
            df_matches = df_matches.merge(
                df_events[['event_id', 'internal_event_id']],
                on='event_id',
                how='left'
            )
            
            ## Modify the keys in all dataframes using modify_keys function
            df_events = modify_keys(df_events)
            
            if not isinstance(df_events, pd.DataFrame):
                df_events = pd.DataFrame(df_events)
            
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

# ### --------------------------------------------------------------------------------------------------------
# ### Championship Data Processing (also includes championships that are hosted in hub queues and LANs)
# ### --------------------------------------------------------------------------------------------------------

# async def process_championship_data(championship_id: str, event_type: str, items_to_return: int|str="ALL", **kwargs) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
#     """
#     The main function to process the data for a championship
    
#     Args:
#         championship_id (str): The ID of the championship to process data for
#         event_type (str): The type of the event (e.g., championship, championship_hub, championship_lan)
#         items_to_return (int | str): Specifies the amount of matches that will be gathered. It can be: (default="ALL") 
#             - An integer, which will return the latests n matches where n is the integer
#             - The string "ALL", which will return all matches played in the hub since the start
#         **kwargs: Additional optional keyword arguments:
#             - from_timestamp (int | str): The start timestamp (UNIX) for the matches to be gathered. (default is 0)
#     Returns:
#         tuple:
#             - df_events: DataFrame containing event details
#             - df_matches: DataFrame containing match data
#             - df_teams_matches: DataFrame containing team match data
#             - df_teams: DataFrame containing team details
#             - df_maps: DataFrame containing map data
#             - df_teams_maps: DataFrame containing team map data
#             - df_players_stats: DataFrame containing player statistics
#             - df_championship_players: DataFrame containing player details
#     """
#     print("--- Processing Championship Data ---")
    
#     async with RequestDispatcher(request_limit=request_limit, interval=interval, concurrency=concurrency) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
#         async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1: # Use the FaceitData context manager to ensure the session is closed properly
        
#             ## Gathering event details
#             df_events = await gather_event_details(event_id=championship_id, event_type=event_type, faceit_data_v1=faceit_data_v1)
            
#             ## Gathering match ids
#             if event_type == "championship":
#                 ## Gathering matches in championship
#                 df_championship_matches = await gather_championship_matches(championship_id, faceit_data=faceit_data)
#                 if isinstance(items_to_return, str):
#                     if items_to_return.upper() == "ALL":
#                         pass
#                     else:
#                         function_logger.critical(f"Invalid items_to_return string: {items_to_return}. Must be an integer or 'ALL'.")
#                         raise ValueError(f"Invalid items_to_return string: {items_to_return}. Must be an integer or 'ALL'.")
#                 elif isinstance(items_to_return, int):
#                     if items_to_return <= 0:
#                         function_logger.critical(f"Invalid items_to_return value: {items_to_return}. Must be a positive integer or 'ALL'.")
#                         raise ValueError(f"Invalid items_to_return value: {items_to_return}. Must be a positive integer or 'ALL'.")
#                     else:
#                         df_championship_matches = df_championship_matches.sort_values(by='match_time', ascending=False).head(items_to_return)
#                 else:
#                     function_logger.critical(f"Invalid items_to_return type: {type(items_to_return)}. Must be an integer or 'ALL'.")
#                     raise TypeError(f"Invalid items_to_return type: {type(items_to_return)}. Must be an integer or 'ALL'.")
                    
#                 ## Filter the matches based on the from_timestamp
#                 df_championship_matches = df_championship_matches.loc[df_championship_matches['match_time'] >= int(kwargs.get("from_timestamp", 0)), :].reset_index(drop=True)
#                 match_ids = df_championship_matches['match_id'].unique().tolist()
                
#             elif event_type == "championship_hub":
#                 ## Gathering matches in championship hub
#                 match_ids = await gather_championship_hub_matches(championship_id)

#             elif event_type == "championship_lan":
#                 # ADD LATER
#                 print("championship_lan event type is not yet supported.")
#                 return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                
#             else:
#                 function_logger.critical(f"Invalid event_type: {event_type}. Must be 'championship', 'championship_hub' or 'championship_lan'.")
#                 raise ValueError(f"Invalid event_type: {event_type}. Must be 'championship', 'championship_hub' or 'championship_lan'.")
            
#             ## Processing matches in championship
#             event_ids = [championship_id]*len(match_ids)
#             df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
#                 match_ids=match_ids, 
#                 event_ids=event_ids,
#                 faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
#             )
            
#             # Add team_id_linked_column to df_teams (and make it a copy of team_id)
#             df_teams['team_id_linked'] = df_teams['team_id']
            
#             ## Add additional data from the events.json file to the dataframes if championship_hub event type
#             if event_type == "championship_hub":
#                 events = load_event_data_json()
#                 for event in events:
#                     if event['event_id'] == championship_id:
#                         # add round/group ids to df_matches
#                         for stage in event.get('event_stages', [{}]):
#                             for group in stage.get('groups', [{}]):
#                                 group_id = group.get('group_id', None)
#                                 for match in group.get('matches', [{}]):
#                                     match_id = match.get('match_id', None)
#                                     round = match.get('round', None)
                                    
#                                     if match_id and match_id in df_matches['match_id'].values:
#                                         df_matches.loc[df_matches['match_id'] == match_id, 'group_id'] = group_id
#                                         df_matches.loc[df_matches['match_id'] == match_id, 'round'] = round
                        
#                         ## add correct team details to df_teams
#                         for team in event.get('teams', [{}]):
#                             team_id = team.get('team_id', None)
#                             team_name = team.get('team_name', None)
#                             avatar = team.get('avatar', None)
#                             team_id_linked = team.get('team_id_linked', None)
                            
#                             if team_id and team_id in df_teams['team_id'].values:
#                                 if team_name:
#                                     df_teams.loc[df_teams['team_id'] == team_id, 'team_name'] = team_name
#                                 if avatar:
#                                     df_teams.loc[df_teams['team_id'] == team_id, 'avatar'] = avatar
#                                 if team_id_linked:
#                                     df_teams.loc[df_teams['team_id'] == team_id, 'team_id_linked'] = team_id_linked
#                         break
#                 else:
#                     function_logger.warning(f"Championship hub with ID {championship_id} not found in the events data.")
#                     raise ValueError(f"Championship hub with ID {championship_id} not found in the events data.")

#             ## Add internal_event_id to df_events (a combination of event_id and stage_id)
#             df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)
            
#             ## Add internal_event_id to df_matches (a combination of the event_id in df_matches and the corresponding stage_id in df_events *it will be unique*)
#             if event_type == "championship_hub":
#                 events = load_event_data_json()
#                 for event in events:
#                     if event['event_id'] == championship_id:
#                         match_event_stage_list = []
#                         for stage in event.get('event_stages', [{}]):
#                             stage_id = stage.get('stage_id', None)
#                             for group in stage.get('groups', [{}]):
#                                 for match in group.get('matches', [{}]):
#                                     match_id = match.get('match_id', None)
#                                     match_event_stage_list.append(
#                                         {
#                                             "match_id": match_id,
#                                             "event_id": championship_id,
#                                             "stage_id": stage_id,
#                                         }
#                                     )
#                         ## Create df with match_id, event_id and stage_id
#                         df_match_event_stage = pd.DataFrame(match_event_stage_list)
                        
#                         df_matches['internal_event_id'] = df_matches['event_id'].astype(str) + "_" + df_match_event_stage.loc[df_match_event_stage['match_id'].isin(df_matches['match_id']), 'stage_id'].astype(str).values
#             elif event_type == "championship":
#                 df_matches['internal_event_id'] = df_matches['event_id'].astype(str) + "_" + df_events.loc[df_events['event_id'].isin(df_matches['event_id']), 'stage_id'].astype(str).values
    
#             # Modify the keys in all dataframes using modify_keys function
#             df_events = modify_keys(df_events)
            
#             if not isinstance(df_events, pd.DataFrame):
#                 df_events = pd.DataFrame(df_events)
            
#             # Rename the dataframes
#             df_events.name = "events"
#             df_matches.name = "matches"
#             df_teams_matches.name = "teams_matches"
#             df_teams.name = "teams"
#             df_maps.name = "maps"
#             df_teams_maps.name = "teams_maps"
#             df_players_stats.name = "players_stats"
#             df_players.name = "players"  
            
#         return df_events, df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players

# async def gather_championship_matches(championship_id: str, faceit_data: FaceitData):
#     """
#     Gathers all the championship matches from a specific championship
    
#     Args:
#         championship_id (str): The ID of the championship to gather matches from
#         faceit_data (FaceitData): The FaceitData object to use for API calls
    
#     Returns:
#         df_championship_matches: DataFrame containing match data with columns:
#             - match_id: The ID of the match
#             - match_time: The time the match was configured
#     """
#     tasks = [faceit_data.championship_matches(championship_id=championship_id, starting_item_position=i, return_items=100) for i in range(0, 1000, 100)]
#     results = await gather_with_progress(tasks, desc="Fetching championship matches", unit='matches')
#     extracted_matches = [match for result in results if isinstance(result, dict) and result['items'] for match in result['items']]
    
#     ## Getting df_championship_matches and df_championship_teams_matches
#     match_list = []
#     for match in extracted_matches:
#         if match.get('status') != "CANCELLED":
#             match_id = match.get('match_id')
#             # Create a dictionary for the match
#             match_dict = {
#                 "match_id": match_id,
#                 "match_time": match.get('configured_at', None),        
#             }
#             match_list.append(match_dict)
                  
#     df_championship_matches = pd.DataFrame(match_list)
#     return df_championship_matches

# async def gather_championship_hub_matches(championship_id: str) -> list:
#     try:
#         events = load_event_data_json()
        
#         match_ids = []
#         found_event = False
#         for event in events:
#             if event['event_id'] == championship_id:
#                 found_event = True
#                 for stage in event.get('event_stages', []):
#                     for group in stage.get('groups', []):
#                         match_ids.extend(
#                             [
#                                 match.get('match_id', None) 
#                                 for match in group.get('matches', [{}]) 
#                                 if match.get('match_id', None) is not None and match.get('match_id', None) != ""
#                             ]
#                         )
#                 return match_ids
#         if not found_event:
#             print(f"Event ID {championship_id} not found in the events data.")
#         return []
#     except Exception as e:
#         print(f"Error gathering championship hub matches: {e}")
#         return []

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    pass
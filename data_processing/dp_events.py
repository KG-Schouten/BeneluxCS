# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from dateutil import parser
from typing import List, Union

# API imports
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.async_progress import gather_with_progress

from logs.update_logger import get_logger

# dp imports
from data_processing.dp_general import modify_keys

# db imports
from database.db_down_update import gather_event_players
from database.db_down import gather_league_teams, gather_season_numbers_from_event_ids

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")

function_logger = get_logger("functions")

### -----------------------------------------------------------------
### ESEA Data Processing
### -----------------------------------------------------------------
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
        
        def safe_parse_ts(value):
            return round(parser.isoparse(value).timestamp()) if value else None
        
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
                                    "event_banner": league_season_data.get('header_image_url', None),
                                    "event_start": safe_parse_ts(league_season_data.get('time_start')),
                                    "event_end": safe_parse_ts(league_season_data.get('time_end')),
                                    "registration_start": safe_parse_ts(league_season_data.get('registration_start')),
                                    "registration_end": safe_parse_ts(league_season_data.get('registration_end')),
                                    "roster_lock": safe_parse_ts(league_season_data.get('roster_lock_at')),
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
    team_ids: list = [],
    event_ids: list = [],
    season_numbers: list = [],
    ) -> pd.DataFrame:

    """ Gathers the df_seasons, df_events and df_teams_benelux dataframes"""
    # In case of event_ids instead of season_numbers, gather the season_numbers from the event_ids
    season_numbers = gather_season_numbers_from_event_ids(
        event_ids=event_ids,
    ) if event_ids else season_numbers
    
    df_teams_benelux = gather_league_teams(
        team_ids=team_ids,
        season_numbers=season_numbers,
    ) # Get the team ids from the json file
    
    if df_teams_benelux.empty:
        raise ValueError(f"No teams found in the Benelux json for the specified season(s). {team_ids}/{event_ids}/{season_numbers}")
    
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
    
    # Convert season_number to string for both dataframes before merging
    df_teams_benelux['season_number'] = df_teams_benelux['season_number'].astype(str)
    df_league_team_season_standings['season_number'] = df_league_team_season_standings['season_number'].astype(str)
    # Convert season_number to string for both dataframes before merging
    df_teams_benelux['season_number'] = df_teams_benelux['season_number'].astype(str)
    df_league_team_season_standings['season_number'] = df_league_team_season_standings['season_number'].astype(str)
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

                    # Case 1: list_main_2 missing or NaN
                    if list_main_2 is None or (isinstance(list_main_2, float) and pd.isna(list_main_2)):
                        return (list_main_1, list_sub_1)

                    # Case 2: both are lists
                    if isinstance(list_main_1, list) and isinstance(list_main_2, list):
                        if not list_main_1:
                            return (list_main_2, list_sub_2)
                        if len(list_main_1) < len(list_main_2):    
                            return (list_main_2, list_sub_2)
                        else:
                            return (list_main_1, list_sub_1)

                    # Fallback: if list_main_1 is usable, take it; otherwise fallback to list_main_2
                    if list_main_1 is not None and not (isinstance(list_main_1, float) and pd.isna(list_main_1)):
                        return (list_main_1, list_sub_1)
                    else:
                        return (list_main_2, list_sub_2)

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

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    pass
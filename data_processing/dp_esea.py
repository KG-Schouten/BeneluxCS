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

# dp imports
from data_processing.dp_general import *

# function imports
from functions import load_api_keys

## Data paths
team_data_path = "C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"

### -----------------------------------------------------------------
### ESEA League Season Data Processing
### -----------------------------------------------------------------

async def process_esea_season_data(faceit_data_v1: FaceitData_v1) -> pd.DataFrame:
    """ Gathers all of the esea seasons and """
    try:
        # Gather the data from the API
        data = await faceit_data_v1.all_leagues()
    
        season_ids = [season['id'] for season in data['payload']]
        season_number = [season['season_number'] for season in data['payload']]
        seasons = {season_id : season_number for season_id, season_number in zip(season_ids, season_number)}
        
        # Gather the data for all seasons
        tasks = [faceit_data_v1.league_season_stages(season_id) for season_id in seasons.keys()]
        results = await gather_with_progress(tasks, desc="Processing ESEA seasons")
        
        df_szn_list = []
        for season_data, (season_id, season_number) in zip(results, seasons.items()):
            df_szn_list.append(process_single_season(season_data, season_id, season_number))
        
        # Create the dataframe with all the seasons
        df_seasons = pd.concat(df_szn_list, ignore_index=True)
        
        return df_seasons
    
    except Exception as e:
        print(f"Error processing ESEA season data: {e}")
        return None
     
def process_single_season(season_data: dict, season_id: str, season_number: str) -> pd.DataFrame:
    """ Gathers esea season data for all esea seasons 
    
    Returns:
        pd.DataFrame: A pandas dataframe containing the season data
        with the following columns: 
        'season_id', 'season_number', 'region_id', 'region_name', 'division_id', 'division_name', 'stage_id', 'stage_name', 'conference_id', 'conference_name', 'championship_id'
    """
    
    # Create the dataframe
    rows = []
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
                            "championship_id": championship_id,
                        }
                    )

    return pd.DataFrame(rows)

### -----------------------------------------------------------------
### ESEA League Data Processing
### -----------------------------------------------------------------

async def process_esea_teams_data(**kwargs) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """The main function to process all data of the Benelux teams in ESEA.
    Args:
        **kwargs: Additional optional keyword arguments:
            - season_number (str | int | list):
                - "ALL" to process all seasons
                - A specific season number (str | int)
                - A list of season numbers (str | int)
            - season_id (str | int | list): The ID(s) of the season(s) to process. Can be:
                - "ALL" to process all seasons
                - A single season ID (as string)
                - A list of season IDs
            - team_id (str or list[str]): The ID(s) of the team(s) to process. Can be:
                - A single team ID (as string)
                - A list of team IDs
            - match_amount (int | str): The number of matches to process. Default is 'ALL'. (will take the most recent matches)
                - 'ALL' to process all matches
                - A specific number of matches (int)
            - match_amount_type (str): How the match_amount is determined
                - 'ANY' to process the latest n matches of all available matches
                - 'TEAM' to process the latest n matches of each team
                - 'SEASON' to process the latest n matches of each season
    Returns:
        tuple:
            - df_seasons (pd.DataFrame): DataFrame containing season data
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
    # Load the kwargs 
    match_amount = kwargs.get("match_amount", "ALL") # Get the match amount from the kwargs (default is "ALL")
    match_amount_type = kwargs.get("match_amount_type", "ANY") # Get the match amount type from the kwargs (default is "ANY")

    async with RequestDispatcher(request_limit=350, interval=10, concurrency=5) as dispatcher: # Create a dispatcher with the specified rate limit and concurrency
        # Initialize the FaceitData object with the API token and dispatcher
        api_keys = load_api_keys()
        faceit_data = FaceitData(api_keys.get("FACEIT_TOKEN"), dispatcher)
        faceit_data_v1 = FaceitData_v1(dispatcher)
        
        ## Dataframe with the season data (df_seasons)
        print('Gathering ESEA season data...')
        df_seasons = await process_esea_season_data(faceit_data_v1=faceit_data_v1)
        ## Dataframe with the benelux teams for each season and division (df_teams_benelux)
        print('Gathering ESEA benelux team data...')
        df_teams_benelux = gather_team_ids_json(kwargs=kwargs) # Get the team ids from the json file
        if df_teams_benelux.empty or df_teams_benelux is None:
            print("No teams found in the json file for the given kwargs.")
            return None

        # Gather the match_ids 
        season_numbers = df_teams_benelux['season_number'].unique().tolist() # Get the season numbers from the teams dataframe     
        tasks = [gather_esea_matches_season(df_seasons, df_teams_benelux, season_number, faceit_data_v1=faceit_data_v1) for season_number in season_numbers]
        results = await gather_with_progress(tasks, desc="Processing ESEA match ids", unit='seasons')
        df_esea_matches = pd.concat(results, ignore_index=True) # Concatenate the results into a single dataframe
        
        if match_amount: # Only keep the matches that are specified in the kwargs
            if match_amount == "ALL":
                pass
            
            elif isinstance(match_amount, int):
                if match_amount_type == "ANY":
                    df_esea_matches = df_esea_matches.sort_values(by='match_time', ascending=False).head(match_amount).reset_index(drop=True)
                elif match_amount_type == "TEAM":
                    df_esea_matches = df_esea_matches.sort_values(by='match_time', ascending=False).groupby('team_id').head(match_amount).reset_index(drop=True)
                elif match_amount_type == "SEASON":
                    df_esea_matches = pd.merge(df_esea_matches, df_seasons, on='championship_id', how='left')
                    df_esea_matches = df_esea_matches.sort_values(by='match_time', ascending=False).groupby('season_id').head(match_amount).reset_index(drop=True)

        match_ids = df_esea_matches['match_id'].unique().tolist() # Get the match ids from the matches
        championship_ids = df_esea_matches['championship_id'].unique().tolist() # Get the championship ids from the matches
        df_matches, df_teams_matches = await process_match_details_batch(match_ids, faceit_data=FaceitData, event_ids=championship_ids) # Get the match details for the matches in the matches
        
        ## Dataframe with the team details (df_teams)
        print('Gathering ESEA team details...')
        team_ids = list(df_teams_matches['team_id'].unique()) # Get the team ids from the matches
        df_teams = await process_team_details_batch(team_ids, faceit_data=faceit_data) # Get the team details for the teams in the matches
        
        ## Dataframes with the maps, teams in the maps and player stats (df_maps, df_teams_maps, df_players_stats)
        print('Gathering ESEA match stats...')
        match_ids = list(df_matches['match_id'][df_matches['status'] == 'finished'].unique()) # Get the match ids from the matches that are finished and are unique
        df_maps, df_teams_maps, df_players_stats = await process_match_stats_batch(match_ids, faceit_data=faceit_data) # Get the match stats for the matches in the matches
        
        ## Dataframe with the player details (df_players)
        print('Gathering ESEA player details...')
        # Get the players ids that have played for a team in that season and division from the benelux sheet teams
        df_merged = ( 
            df_players_stats
            .merge(df_matches, on='match_id', how='left')
            .merge(df_seasons, on='championship_id', how='left')
        )
        valid_combos = set()
        for index, row in df_teams_benelux.iterrows():
            season_number = row['season_number']
            division_name = row['division_name']
            team_id = row['team_id']
            valid_combos.add((season_number, division_name, team_id))
        # Filter the DataFrame based on the valid combinations
        df_merged['key'] = list(zip(df_merged['season_number'], df_merged['division_name'], df_merged['team_id']))
        valid_players = df_merged[df_merged['key'].isin(valid_combos)]

        player_ids = valid_players['player_id'].unique()
        df_players = await process_player_details_batch(player_ids, faceit_data_v1=faceit_data_v1) # Get the player details for the players in the matches
    
    return df_seasons, df_teams_benelux, df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players
    
async def gather_esea_matches_season(df_seasons: pd.DataFrame, df_teams_benelux: pd.DataFrame, season_number: int, faceit_data_v1: FaceitData_v1) -> tuple[pd.DataFrame, pd.DataFrame]:
    """ 
    Gather the matches of all benelux teams in a season
    
    Returns:
        pd.DataFrame: A pandas dataframe containing the match data
        with the following columns:
        'match_id', 'championship_id', 'match_time'
    """
    try:
        # season_id = df_seasons.loc[df_seasons['season_number'] == int(season_number), 'season_id'].values[0]

        tasks = []
        
        df_season_teams = df_teams_benelux.loc[df_teams_benelux['season_number'] == int(season_number)]
        
        for division_name, group in df_season_teams.groupby('division_name'):
            team_ids = group['team_id'].to_list()
            
            championship_ids = df_seasons[
                (df_seasons['season_number'] == int(season_number)) &
                (df_seasons['division_name'] == division_name) &
                (df_seasons['region_name'].isin(['Europe', 'North America']))
            ]['championship_id'].to_list()
            
            for team_id in team_ids:
                tasks.append(faceit_data_v1.league_team_matches(team_id, championship_ids))
        
        results = await asyncio.gather(*tasks)  # Run all tasks concurrently
        
        match_list = [
            {
                'match_id': match['origin']['id'],
                'championship_id': match.get('championshipId', None),
                'match_time': match['origin'].get('schedule'),
            }
            for result in results
            if isinstance(result, dict)
            if result['payload']['items'] is not None
            for match in result['payload']['items']
            if match['origin']['id'] is not None
            if not any(faction['id'] == 'bye' for faction in match['factions'])
        ]
        
        df_esea_matches = pd.DataFrame(match_list)
        return df_esea_matches
    except Exception as e:
        print(f"Error gathering ESEA matches for teams in season {season_number}: {e}")
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
    BASE_DIR = os.path.dirname(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    URL = os.path.join(BASE_DIR, 'data', 'league_teams.json')

    # Load the kwargs 
    season_number = kwargs.get("season_number", None) # Get the season number from the kwargs (default is "ALL")
    season_id = kwargs.get("season_id", None) # Get the season id from the kwargs (default is None)
    team_id = kwargs.get("team_id", None) # Get the team id from the kwargs (default is None)

    try:
        with open(URL, 'r', encoding='utf-8') as f:  # Open the file in read mode with UTF-8 encoding
            team_ids = json.load(f)  # Load the JSON content into a Python dictionary
            
            # Convert into a dataframe
            rows = []
            for season_id, season_data in team_ids['seasons'].items():
                season_number = season_data['season_number']
                for division_name, teams in season_data['teams'].items():
                    for team_name, team_id in teams.items():
                        rows.append({
                            'season_id': season_id,
                            'season_number': season_number,
                            'division_name': division_name,
                            'team_name': team_name,
                            'team_id': team_id
                        })
            df_teams = pd.DataFrame(rows)
            
            # Filter the dataframe based on the kwargs
            if season_number is not None: # Filtering based on season_number
                
                if season_number == "ALL":
                    pass
                
                elif isinstance(season_number, str or int):
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

### -----------------------------------------------------------------
### ESEA League Team/Player discovery (UNDER CONSTRUCTION)
### -----------------------------------------------------------------

async def gather_benelux_esea_all(season_number: int):
    """ Gather all the teams and players of all stages in a league season"""
    # Gather the league season details and stages
    
    # Initialize the empty dataframes
    df_players = pd.DataFrame()
    df_teams = pd.DataFrame()
    
    season_data = await gather_esea_stages(season_number)
    
    for division in season_data['divisions']:
        division_name = division['division_name']
        
        for stage in division['stages']:
            stage_name = stage['stage_name']
            if stage_name == "Regular Season": # Only check the regular season
                
                print(f"'\nGathering data for {division_name} - {stage_name}")
                
                conference_id = stage['conference_id']
                
                df_players_div, df_teams_div = await check_benelux_esea(conference_id) # Check the stage for benelux players and teams
                df_teams_div['division_name'] = division_name
        
                df_players = pd.concat([df_players, df_players_div], ignore_index=True)
                df_teams = pd.concat([df_teams, df_teams_div], ignore_index=True)
    
    return df_players, df_teams

async def gather_esea_stages(season_number: str) -> list:
    """
    Function to gather all the stages of a league season
    
    Args:
        season_number (str): The ID of the season to get the stages from
        
    Returns:
        dict: A dictionary containing the following structure:
            {
                'season_id': str,
                'season_number': str,
                'divisions': [
                    {
                        'division_id': str,
                        'division_name': str,
                        'stages': [
                            {
                                'stage_id': str,
                                'stage_name': str,
                                'conference_id': str
                            },
                            ...
                        ]
                    },
                    ...
                ]
            }
    """
    # Gather the league seasons
    leagues = await faceit_data_v1.all_leagues()
    leagues = leagues['payload']
    
    # Gather the league season id based on the season_number
    season_id = next(
        (season['id'] for season in leagues if int(season['season_number']) == int(season_number)),
        None
    )
    
    # Gather the league stages based on the season_id
    season_stages = await faceit_data_v1.league_season_stages(season_id)
    season_stages = season_stages['payload']['regions']

    season_divs = [division for region in season_stages if region['name'] == 'Europe' for division in region['divisions']]
    
    # Creathe the dictionary to return
    season_data = {
        'season_id': season_id,
        'season_number': season_number,
        'divisions': [
            {
                'division_id': division['id'],
                'division_name': division['name'],
                'stages': [
                    {
                        'stage_id': stage['id'],
                        'stage_name': stage['name'],
                        'conference_id': stage['conferences'][0]['id']
                    }
                    for stage in division['stages']
                ]
            }
            for division in season_divs
        ]
    }
    
    return season_data

async def check_benelux_esea(conference_id: str) -> bool:
    """ 
    Function to check which teams and players in an esea stage are from the benelux 
    
    Args:
        conference_id (str): The conference (stage) ID of the stage to check
    
    Returns:
        tuple(pd.DataFrame, pd.DataFrame): A tuple containing two pandas dataframes:
            - df_players: DataFrame containing player details
            - df_teams: DataFrame containing team details
    """
    
    # Gather the teams and players of the stage
    df_players, df_teams = await gather_esea_stage_teams(conference_id)
    
    # Check if the players are benelux
    player_ids = df_players['player_id'].tolist()
    player_names = df_players['player_name'].tolist()
    
    # Check if the players are benelux
    df_results = await check_all_players(player_ids, player_names, PRINT=False)
    
    # Run the ai model on the players
    df_prepped = preprocess_data(df_results.drop(columns=['bnlx_country']))
    df_pred = df_prepped.drop(columns=['player_id', 'player_name'])
    pred = predict(df_pred)
    
    df_players['prediction'] = pred

    # Keep only the benelux players and teams with benelux players
    benelux_players = df_players[df_players['prediction'] == 1]
    benelux_teams = df_teams[df_teams['team_id'].isin(benelux_players['team_id'])]
    
    return benelux_players, benelux_teams
    
async def gather_esea_stage_teams(conference_id: str) -> list:
    """
    Function to gather all the teams of a league stage
    
    Args:
        conference_id (str): The conference ID of the stage to get the teams from
        
    Returns:
        tuple(pd.DataFrame, pd.DataFrame): A tuple containing two pandas dataframes:
            - df_players: DataFrame containing player details
            - df_teams: DataFrame containing team details
                
    """
    
    team_details = []
    team_list = []
    batch_start = 0
    batch = 50
    # Keep looping until all friends in friendlist have been appended
    while len(team_list) == batch_start:
        data = await faceit_data_v1.league_season_stage_teams(conference_id, starting_item_position=batch_start, return_items=batch)
        
        data = [
            {
                'team_id': team['premade_team_id'],
                'status': team['status']
            }
            for team in data['payload']
        ]
        
        # Gather team details
        team_list_item = [team['team_id'] for team in data if team['status'] != 'DISQUALIFIED']
        team_details_data = await faceit_data_v1.league_team_details_batch(team_list_item)
        team_details.extend(team_details_data['payload'])
        
        team_list.extend(data)
        
        batch_start += batch

        # Create pandas dataframe with team details
        teams = [
            {
                'team_id': team['id'],
                'team_name': team['name'],
                'team_avatar': team['avatar'],
                'description': team['description'],
                'website': team['website'],
                'twitter': team['twitter'],
                'facebook': team['facebook'],
                'youtube': team['youtube']
            }
            for team in team_details
        ]

        players = [
            {
                'player_id': player['id'],
                'player_name': player['nickname'],
                'team_id': team['id'],
                'country': player['country'],
                'avatar': player['avatar']
            }
            for team in team_details
            for player in team['members']
        ]

        df_players = pd.DataFrame(players)
        df_teams = pd.DataFrame(teams)
        
    return df_players, df_teams


if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

    df = gather_team_ids_json(season_number='ALL')

    print(df)
    
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

async def process_esea_teams_data(season_number: str | int):
    """ The main function to process all data of the benelux teams in ESEA 
    
    Args:
        season_number (str): The number of the season to process, can be:
            - "ALL" for all seasons
            - int for a specific season number
    
    Returns:
        tuple:
            - df_seasons: DataFrame with the season data
            - df_teams_benelux: DataFrame with the benelux teams for each season and division
            - df_matches: DataFrame with the matches data
            - df_teams_matches: DataFrame with the teams in the matches
            - df_teams: DataFrame with the team details
            - df_maps: DataFrame with the maps data
            - df_teams_maps: DataFrame with the teams in the maps
            - df_players_stats: DataFrame with the player stats data
            - df_players: DataFrame with the player details
    """
    print(
    f"""
        
    ------------------------------------------------------------
        Processing ESEA team data for season: {season_number}
    ------------------------------------------------------------
    
    """
    )

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
        df_teams_benelux = gather_team_ids_json()
        
        ## Dataframes with the matches and teams in these matches (df_matches, df_teams_matches)
        print('Gathering ESEA match data...')
        if season_number == "ALL":
            # Get all season numbers
            season_numbers = df_teams_benelux['season_number'].unique()
            # Gather the matches for all seasons
            tasks = [gather_esea_matches_season(df_seasons, df_teams_benelux, season_number, faceit_data_v1=faceit_data_v1) for season_number in season_numbers]
            results = await gather_with_progress(tasks, desc="Processing ESEA matches", unit='seasons')
            
            df_matches = pd.concat([result[0] for result in results], ignore_index=True)
            df_teams_matches = pd.concat([result[1] for result in results], ignore_index=True)
        elif isinstance(season_number, int): # Get the data from the specific season number
            df_matches, df_teams_matches = await gather_esea_matches_season(df_seasons, df_teams_benelux, season_number, faceit_data_v1=faceit_data_v1)
        
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
    """ Gather the matches of all benelux teams in a season"""
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
        
        extracted_matches = [
            match
            for result in results
            if isinstance(result, dict)
            if result['payload']['items'] is not None
            for match in result['payload']['items']
        ]
        
        ## Create the match list and team match dataframes
        match_list = []
        team_match_list = []
        team_match_set = set()

        for match in extracted_matches:
            if not any(faction['id'] == 'bye' for faction in match['factions']):
                match_id = match['origin']['id']
                match_list.append(
                    {
                        'match_id': match_id,
                        'championship_id': match['championshipId'],
                        'group': match['group'],
                        'round': match['round'],
                        'status': match['status'],
                        'winner': match.get('winner', None),
                        'match_time' : match['origin'].get('schedule', None)
                    }
                )
                
                for team in match['factions']:
                    team_id = team['id']
                    if (team_id, match_id) not in team_match_set and team_id != 'bye':
                        team_match_set.add((team_id, match_id))
                        
                        team_match_list.append(
                            {
                                'team_id': team_id,
                                'match_id': match_id
                            }
                        )

        df_matches = pd.DataFrame(match_list)
        df_teams_matches = pd.DataFrame(team_match_list)
        
        return df_matches, df_teams_matches
    except Exception as e:
        print(f"Error gathering ESEA matches for teams in season {season_number}: {e}")
        return None

def gather_team_ids_json(URL: str = "data\\league_teams.json") -> pd.DataFrame:
    """
    Reads a JSON file containing team names and their corresponding team IDs.

    Args:
        URL (str, optional): The file path to the JSON file containing team IDs.
                             Defaults to "data/league_teams.json".

    Returns:
        pd.Dataframe: A pandas DataFrame containing the following columns:
            - season_id: The ID of the season
            - season_number: The number of the season
            - division_name: The name of the division
            - team_name: The name of the team
            - team_id: The ID of the team
    """
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
    
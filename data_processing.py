from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1

from functions import load_api_keys

import json
import os

import asyncio
import aiohttp

import requests

import pandas as pd

api_keys = load_api_keys()

faceit_data = FaceitData(api_keys.get("FACEIT_TOKEN"))
faceit_data_v1 = FaceitData_v1()

## Data paths
team_data_path = "C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"

team_ids = [
    "2d88773b-5756-4ddc-be2b-6455ed3ee661", ## Souls Heart
    "d19ba1db-0a65-4197-8f84-d7c44cfd835b", ## Fisher College
    "d74c5ac9-c23e-445f-b2b4-9ff1a7d0b6ab", ## NIP Impact
    "828dc640-5b66-4fc9-bc17-3a63f61fa23a", ## Foxnyr
    "2972bfee-55d7-40f4-be0b-9297425c5a3e", ## Senshi Esports
    "6bb0008b-e18f-4723-a265-cc6f8538efe4", ## Myth Esports
    "cdc647c5-a646-42dd-9652-61daf227eaef", ## AOMA
    "b8df37a1-1f7e-4845-bfb9-313b2a6bbf42", ## mCon esports
    "6d76087e-7743-43e4-b906-5070e7c10211", ## Souls Heart AC
    "70ca9226-d8fe-45b2-8bbf-daa3e306f6ad", ## EK VIOLET
    "bad5d18c-0823-4195-b8df-dd4aa4027dfe", ## Ethereon
    "41a52178-4858-48c1-afc9-bfa0a5b07959", ## Local Vuurwolf
    "981c75dd-63e2-4981-b84e-3bfb07edd752", ## Silly Strikers
    "faf8cb9b-0834-41c8-b126-863c518204ce", ## Voice_enable 0
    "d2361f5f-ef61-426b-804a-48da947df944", ## PAAARFUL
    "7e716052-f488-4a6d-aaf9-3afcf3a89fcc", ## FullShock Gamers
    "f1d7ef7b-40d6-4713-b07c-c87803e89a1b", ## Connector One
    "3ed29691-3b98-44bf-b410-e5236a4943b3", ## Born2Throw
    "c8af50fd-7e4e-47aa-8ba4-8cf3c6bfa292", ## Nocturn eSports
    "c3c0b091-d318-4485-8ba5-d15c4935b21e", ## Ignite Red
    "4efe161d-abdf-4e17-92c9-b6347ec08672", ## Yapybaras
    "ba981d0d-6a5c-401b-acf0-6c4d328d1413", ## Mysod
    "49a279dc-0ceb-4d8b-a5e3-e365fde0e0f7", ## HURRICANE
    "f5793789-233a-4f4c-b36b-e0bf41b25399", ## Bangs
    "94c9ebe8-ce25-4847-b05c-5f062824c5b5", ## monkaRON
    "6540d97f-013a-4dea-8c1e-054860c7f66d", ## Okedan
    "8aead11a-89ce-4261-b232-41ae95db4da0", ## BruinFruit
    "9936cc1d-09cf-4599-9781-d4b907435ae2", ## CHINABOYS
    "81d0d801-8766-42b6-876d-e1d590a228b1", ## BAKSTEEN
    "37b89252-fe56-4dea-9c8e-26f0baf9b72e", ## Team Shonach
    "4e53c613-424b-4e02-99dc-1b57a9654311", ## Goeievraag
    "922f83a8-270b-4e4f-ac60-217742586b18", ## Zonked Main
    "9f7239a9-81b4-4e5c-9b9c-c1c27c78a643", ## ECV Esports
    "28b271f8-c368-4d5b-856e-ec95dffc0cb9", ## WHATEVER
    "e13fb541-65b3-44de-9e50-4cf8bf5ee00d", ## Stabiel
    "faf3bf48-f560-440a-a418-4d22e49569fd", ## Zonked NXT
    "276a9c32-e303-4107-bbca-4bbf53a121cf" ## Zero K-D
]


async def store_team_data_async(faceit_data, faceit_data_v1, output_path):
    """Main asynchronous function to fetch and store team data."""
    global team_ids

    team_dict = {}

    # Create tasks for all teams
    tasks = [process_team_data(faceit_data, faceit_data_v1, team) for team in team_ids]

    # Gather results
    results = await asyncio.gather(*tasks)

    # Build the team dictionary
    for team_name, team_data in results:
        team_dict[team_name] = team_data

    # Write data to JSON
    with open(output_path, "w") as outfile:
        json.dump(team_dict, outfile, indent=4)
    print(f"Data successfully written to {output_path}")


async def fetch_team_details(faceit_data, faceit_data_v1, team):
    """Fetch all required team details asynchronously"""
    team_details = await asyncio.to_thread(faceit_data.team_details, team)
    league_team_details = await asyncio.to_thread(faceit_data_v1.league_team_details, team)
    championship_id = league_team_details['payload'][0]["league_seasons_info"][0]["season_standings"][0]["championship_id"]
    league_team_matches = await asyncio.to_thread(faceit_data_v1.league_team_matches, team, championship_id)
    return team_details, league_team_details, league_team_matches


async def process_team_data(faceit_data, faceit_data_v1, team):
    """Process data for a single team and return the team dictionary."""
    team_details, league_team_details, league_team_matches = await fetch_team_details(faceit_data, faceit_data_v1, team)

    # Process league details (removing unnecessary keys, make season number the index and keep only current season, remove 'team_role' from members list)
    df_league_details = pd.DataFrame(league_team_details['payload'][0]['league_seasons_info'])
    df_league_details = df_league_details.set_index('season_number').loc["52"]
    df_league_details['team_members'] = [
        {key: value for key, value in player.items() if key not in ['team_role']}
        for player in df_league_details['team_members']
    ]

    # Filter matches (remove unnecessary keys, remove matches with a 'bye')
    df_matches = pd.DataFrame(league_team_matches['payload']['items'])
    df_matches = df_matches[~df_matches['factions'].apply(lambda factions: any(faction['id'] == "bye" for faction in factions))]

    # Construct team dictionary
    team_data = {
        "team_id": team_details['team_id'],
        "avatar": team_details.get('avatar', ''),
        "members": df_league_details['team_members'],
        "ESEA": {
            "league_id": df_league_details['season_standings'][0]['championship_id'],
            "league_level": df_league_details['season_standings'][0]['division_name'],
            "wins_losses": [
                df_league_details['season_standings'][0]['wins'],
                df_league_details["season_standings"][0]["losses"],
            ],
            "matches": [
                {
                    "status": row['origin']['state'],
                    "time": row['origin']['schedule'],
                    "opponent_id": next(faction['id'] for faction in row['factions'] if faction['id'] != team),
                    "opponent_name": '',
                    "match_id": row['origin']['id'],
                }
                for _, row in df_matches.iterrows()
            ],
        },
    }
    
    # Fetch opponent names asynchronously
    opponent_details = await fetch_all_opponents(faceit_data, team_data["ESEA"]["matches"])

    # Add opponent names to the matches
    for match, details in zip(team_data["ESEA"]["matches"], opponent_details):
        match["opponent_name"] = details.get("name")
    
    print(f'{team_details["name"]} data processed successfully!')
    return team_details['name'], team_data


## WRAPPER FOR ASYNC FUNCTION
def store_team_data(output_path="C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json"):
    global faceit_data, faceit_data_v1
    
    try:
        # Try to get the current event loop
        loop = asyncio.get_running_loop()
        if loop.is_running():
            # If the loop is already running (Discord bot context), we create a new task
            loop.create_task(store_team_data_async(faceit_data, faceit_data_v1, output_path))
            return
    except:
        asyncio.run(store_team_data_async(faceit_data, faceit_data_v1, output_path))


async def fetch_opponent_name(faceit_data, opponent_id):
    # Run the team_details function asynchronously
    return await asyncio.to_thread(faceit_data.team_details, opponent_id)


async def fetch_all_opponents(faceit_data, matches):
    # Create tasks for all API requests
    tasks = [
        asyncio.create_task(fetch_opponent_name(faceit_data, match['opponent_id']))
        for match in matches
    ]

    # Gather all results (wait for all tasks to complete)
    results = await asyncio.gather(*tasks)
    return results
    

def read_team_data():
    """Returns team_data as a pandas dataframe"""
    global team_data_path

    with open(team_data_path, "r") as file:
        team_data = json.load(file)
    
    team_data = pd.DataFrame(team_data)

    return team_data


## Run this code only when program is run directly
if __name__ == "__main__":
    
    store_team_data()
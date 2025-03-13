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


api_keys = load_api_keys()

faceit_data = FaceitData(api_keys.get("FACEIT_TOKEN"))
faceit_data_v1 = FaceitData_v1()

# Create a semaphore to limit the number of concurrent requests
SEMAPHORE = asyncio.Semaphore(5)  # Adjust the limit based on API restrictions

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

    queue = asyncio.Queue()

    for team in team_ids:
        await queue.put(team)

    async def worker():
        while not queue.empty():
            team = await queue.get()
            try:
                team_name, team_data = await process_team_data(faceit_data, faceit_data_v1, team)
                team_dict[team_name] = team_data
            except Exception as e:
                print(f"Error fetching data for team {team}: {e}")
            queue.task_done()

    ## Create tasks for all workers
    num_workers = 5 # Adjust the number of concurrent workers
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    await queue.join() # Wait for queue to be fully processed

    # Automatically write data to JSON
    temp_output_path = output_path + ".temp"
    with open(temp_output_path, "w") as outfile:
        json.dump(team_dict, outfile, indent=4)

    # Rename after writing is complete
    os.replace(temp_output_path, output_path)
    print(f"Data successfully written to {output_path}")


async def fetch_team_details(faceit_data, faceit_data_v1, team):
    """Fetch all required team details asynchronously"""
    league_team_matches = {}
    
    async with SEMAPHORE: # Limit the number of concurrent requests
        try:
            team_details = await asyncio.to_thread(faceit_data.team_details, team)
            await asyncio.sleep(0.5) # Add a small delay to avoid rate limiting
            league_team_details = await asyncio.to_thread(faceit_data_v1.league_team_details, team)

            for championship in league_team_details['payload'][0]["league_seasons_info"][0]["season_standings"]:
                championship_id = championship["championship_id"]
                await asyncio.sleep(0.5) # Add a small delay to avoid rate limiting
                league_team_matches[championship_id] = await asyncio.to_thread(faceit_data_v1.league_team_matches, team, championship_id)
        except Exception as e:
            print(f"Error fetching data for team {team}: {e}")
            return None, None, None
    
    return team_details, league_team_details, league_team_matches


async def process_team_data(faceit_data, faceit_data_v1, team):
    """Process data for a single team and return the team dictionary."""
    team_details, league_team_details, league_team_matches = await fetch_team_details(faceit_data, faceit_data_v1, team)

    # Process league details (removing unnecessary keys, make season number the index and remove 'team_role' from members list)
    df_league_details = pd.DataFrame(league_team_details['payload'][0]['league_seasons_info'])
    df_league_details = df_league_details.set_index('season_number').loc["52"]
    df_league_details['team_members'] = [
        {key: value for key, value in player.items() if key not in ['team_role']}
        for player in df_league_details['team_members']
    ]

    # Filter matches (remove unnecessary keys, remove matches with a 'bye')
    df_matches_dict = {}
    for stage_id in league_team_matches:
        df_matches = pd.DataFrame(league_team_matches[stage_id]['payload']['items'])
        df_matches = df_matches[~df_matches['factions'].apply(lambda factions: any(faction['id'] == "bye" for faction in factions))]
        df_matches_dict[stage_id] = df_matches

    # Construct team dictionary
    team_data = {
        "team_id": team_details['team_id'],
        "avatar": team_details.get('avatar', ''),
        "members": df_league_details['team_members'],
        "ESEA": {
            "league_level": df_league_details['season_standings'][0]['division_name'],
            "division_id": df_league_details['season_standings'][0]['division_id'],
            "stages": [
                {
                    "stage_id": stage['stage_id'],
                    "stage_name": stage['stage_name'],
                    "wins_losses": [
                        stage['wins'],
                        stage['losses'],
                    ],
                    "championship_id": stage['championship_id'],
                    "matches": [
                        {
                        "status": row['origin']['state'],
                        "time": row['origin']['schedule'],
                        "opponent_id": next(faction['id'] for faction in row['factions'] if faction['id'] != team),
                        "opponent_name": '',
                        "match_id": row['origin']['id'],
                        }
                        for _, row in df_matches_dict[stage['championship_id']].iterrows()
                        if pd.notna(row['origin'])
                    ]
                }
                for stage in df_league_details['season_standings']
            ]
        },
    }
    
    ## Fetch opponent names and add them to the matches for each stage
    for stage in team_data["ESEA"]["stages"]:
        # Fetch opponent names for all matches in the stage
        opponent_details = await fetch_all_opponents(faceit_data, stage["matches"])

        # Add opponent names to the matches
        for match, details in zip(stage["matches"], opponent_details):
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
    """Fetch the opponent name for a given opponent_id."""
    # Run the team_details function asynchronously
    async with SEMAPHORE: # Limit the number of concurrent requests
        backoff = 1 # Initial backoff time (in seconds)
        for attempt in range(5): # Retry up to 5 times
            try:
                return await asyncio.to_thread(faceit_data.team_details, opponent_id)
            except Exception as e:
                if attempt == 4:
                    print(f"Failed to fetch opponent {opponent_id} after retries: {e}")
                    return {"id": opponent_id, "name": "Unknown"} # Fallback placeholder
                wait_time = backoff + random.uniform(0, 0.5) # Add slight jitter delay to wait time
                print(f"Retrying opponent {opponent_id} in {wait_time:.2f}s due to error: {e}")
                await asyncio.sleep(wait_time)
                backoff *= 2 ## Exponential increase in backoff time


async def fetch_all_opponents(faceit_data, matches):
    """Fetch opponent names for all matches asynchronously."""
    queue = asyncio.Queue()

    ## Enqueue all opponent ID's
    for match in matches:
        await queue.put(match['opponent_id'])

    results = {}
    
    ## Creating the worker function
    async def worker():
        while not queue.empty():
            opponent_id = await queue.get()
            opponent_details = await fetch_opponent_name(faceit_data, opponent_id)
            results[opponent_id] = opponent_details
            queue.task_done()

    ## Create tasks for all workers
    num_workers = 5 # Adjust the number of concurrent workers
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    await queue.join() # Wait for queue to be fully processed

    return [results[match['opponent_id']] for match in matches]
    

def read_team_data():
    """Returns team_data as a pandas dataframe"""
    global team_data_path

    with open(team_data_path, "r") as file:
        team_data = json.load(file)
    
    team_data = pd.DataFrame(team_data)

    return team_data


def process_hub_data(return_items=100) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns the data from the recent hub matches

    Args:
        return_items (int | str): Specifies the amount of matches that will be gathered. It can be: (default=100) 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
    
    Returns:
        tuple [list[dict], list[dict], list[dict]]
            Containing three lists of dicts:
                - batch_data_match
                - batch_data_player
                - batch_data_team

    """
    try:
        # Try to get the current event loop
        loop = asyncio.get_running_loop()
        if loop.is_running():
            # Run the async function inside the existing event loop
            return loop.run_until_complete(process_hub_data_async(return_items))
    except:
        # No event loop running, so we can safely use asyncio.run()
        return asyncio.run(process_hub_data_async(return_items))


async def process_hub_data_async(return_items=100) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Async function to return the data from the recent hub matches

    Args:
        return_items (int | str): Specifies the amount of matches that will be gathered. It can be: (default=100) 
            - An integer, which will return the latests n matches where n is the integer
            - The string "ALL", which will return all matches played in the hub since the start
    
    Returns:
        tuple [list[dict], list[dict], list[dict]]
            Containing three lists of dicts:
                - batch_data_match
                - batch_data_player
                - batch_data_team

    """
    ## Gather the matches data depended on the input argument given
    if return_items == "ALL": # Gather all matches from the hub
        data = {"items": []}
        i = 0
        while len(data['items']) % 100 == 0: # Break the loop once all matches have been found
            data_batch = faceit_data.hub_matches("801f7e0c-1064-4dd1-a960-b2f54f8b5193", starting_item_position=int(i), return_items=100)

            # Extend new data to the data list
            data["items"].extend(data_batch["items"])

            # Print the length of the 
            print(f"Current length of data list: {len(data["items"])}")
            # Count up
            i += 100
    elif isinstance(return_items, int):
        if return_items > 100: # If more than 100 items are called, it will loop through the function untill all match data is fetched
            data = {"items": []}
            requested_count = return_items
            amount_called = 0
            while requested_count > 0:
                # Determine how many items to fetch in this batch
                batch = min(requested_count, 20)
                
                # Gather data
                data_batch = faceit_data.hub_matches("801f7e0c-1064-4dd1-a960-b2f54f8b5193", starting_item_position=int(amount_called), return_items=batch)

                # Extend new data to the data list
                data["items"].extend(data_batch["items"])

                # Decrease the remaining amount of items to return
                requested_count -= batch
                amount_called += batch
        else: # If return_items < 100, just call the function once
            data = faceit_data.hub_matches("801f7e0c-1064-4dd1-a960-b2f54f8b5193", return_items=return_items)
    else:
        raise TypeError(f"return_items: ({return_items}) was not of type int or the required string")

    ## Remove all unnecessary keys from the match data
    for match in data["items"]:
        ## Remove all unnecessary keys from the first level of the dictionary
        for key in ["version", "game", "region", "competition_type", "calculate_elo", "chat_room_id", "detailed_results", "faceit_url"]:
            try:
                match.pop(key, None)
            except KeyError:
                continue

        ## Remove all unnecessary keys from the "teams" dictionary
        if "teams" in match:
            for faction in match["teams"].values():
                for key in ["faction_id", "leader", "avatar", "substituted", "type"]:
                    try:
                        faction.pop(key, None)
                    except KeyError:
                        continue

                ## Remove all unnecessary keys from the "roster" dictionary
                for player in faction["roster"]:
                    for key in ["avatar", "membership", "game_player_name", "anticheat_required", "game_skill_level"]:
                        try:
                            player.pop(key, None)
                        except KeyError:
                            continue

        ## Remove all unnecessary keys from the "voting" dictionary
        if "voting" in match:
            for key in ["voted_entity_types", "location"]:
                try:
                    match["voting"].pop(key, None)
                except KeyError:
                    continue

            if "map" in match["voting"]:
                for key in ["entities"]:
                    try:
                        match["voting"]["map"].pop(key, None)
                    except KeyError:
                        continue
        
    #Remove both the "items" indent and the match if the status is not "FINISHED"
    data = [match for match in data["items"] if (match["status"] == "FINISHED" )]
    
    ## Creating the queue and the rest of the async functionality
    queue = asyncio.Queue()

    # Adding all matches to the queue
    j = 0
    for match in data:
        print(f"{j} matches added to queue")
        j+=1
        await queue.put(match)

    ## Create the worker *and initializing the batch_data_xxx lists
    batch_data_match = []
    batch_data_team = []
    batch_data_player = []

    async def worker():
        while not queue.empty():
            match = await queue.get()
            try:
                single_batch_data_match, single_batch_data_player, single_batch_data_team = await batch_hub_data(match)
                batch_data_match.extend(single_batch_data_match)
                batch_data_player.extend(single_batch_data_player)
                batch_data_team.extend(single_batch_data_team)

                print(f"Current length of the batch list: {len(batch_data_match)}")
            except Exception as e:
                print(f"Error fetching data for a match: {e}")
            queue.task_done()
    

    ## Create tasks for all workers
    num_workers = 5 # Adjust the number of concurrent workers
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    await queue.join() # Wait for queue to be fully processed

    # Modify the keys so they can be used in a MySQL database
    batch_data_match = modify_keys(batch_data_match)
    batch_data_player = modify_keys(batch_data_player)
    batch_data_team = modify_keys(batch_data_team)

    # print(json.dumps(match,indent=4), "\n")
    # print(json.dumps(stats,indent=4))

    return batch_data_match, batch_data_player, batch_data_team

async def batch_hub_data(match):
    
    stats = await asyncio.to_thread(faceit_data.match_stats, match["match_id"])

    single_batch_data_match = []
    single_batch_data_team = []
    single_batch_data_player = []

    # Sorting the keys in the nested dict to the order stays constant
    sorted_keys = sorted(stats['rounds'][0]['round_stats'].keys())
    match_data = {
        "match_id": stats['rounds'][0]['match_id'],
        "competition_id": match['competition_id'],
        "competition_name": match['competition_name'],
        "game_mode": stats['rounds'][0]['game_mode'],
        "best_of": stats['rounds'][0]['best_of'],
        "match_round": stats['rounds'][0]['match_round'],
        "start_time": match["started_at"],
        "demo": match['demo_url'][0],
        **{key: stats['rounds'][0]['round_stats'][key] for key in sorted_keys}
        }
    
    single_batch_data_match.append(match_data)

    for team in stats['rounds'][0]['teams']:
        # Sorting the keys in the nested dict to the order stays constant
        sorted_keys = sorted(team["team_stats"].keys())
        team_data = {
            "team_id": team['team_id'],
            "match_id": stats['rounds'][0]['match_id'],
            "team_name": team['team_stats']['Team'],
            **{key: team["team_stats"][key] for key in sorted_keys}
            }

        single_batch_data_team.append(team_data)

        for player in team['players']:
            # Sorting the keys in the nested dict to the order stays constant
            sorted_keys = sorted(player['player_stats'].keys())
            player_data = {
                "player_id": player['player_id'],
                "nickname": player['nickname'],
                "team_id": team['team_id'],
                "match_id": stats['rounds'][0]['match_id'],
                **{key: float(player['player_stats'][key]) 
                        if "." in player['player_stats'][key] else int(player['player_stats'][key]) 
                        for key in sorted_keys
                    } ## Convert all values to floats and unpack all key-value combinations from the player_stats dict into player_data dict
                }

            single_batch_data_player.append(player_data)

    # print(single_batch_data_match[0]["match_id"])

    return single_batch_data_match, single_batch_data_player, single_batch_data_team

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

## Run this code only when program is run directly
if __name__ == "__main__":
    
    process_hub_data(return_items=200)
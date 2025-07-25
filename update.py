## Imports
from database.db_down import gather_players, gather_upcoming_matches, gather_event_players, gather_event_teams, gather_event_matches, gather_internal_event_ids
from database.db_up import upload_data
from data_processing.faceit_api.sliding_window import RequestDispatcher
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.logging_config import function_logger
from data_processing.dp_general import process_matches, process_team_details_batch, process_player_details_batch
from data_processing.dp_events import process_teams_benelux_esea, gather_esea_matches, gather_hub_matches, gather_event_details, process_esea_season_data, modify_keys
from data_processing.dp_benelux import get_benelux_leaderboard_players

import pandas as pd
import sys
import asyncio

import os
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")

# === 5 minute update interval ===
async def update_matches():
    """ Update matches from the database """
    ## Main logic
    try:
        df_upcoming = gather_upcoming_matches()
        
        match_ids = df_upcoming['match_id'].tolist()
        event_ids = df_upcoming['event_id'].tolist()
        
        if not match_ids:
            function_logger.info("No upcoming matches found.")
            return 
        if not event_ids:
            function_logger.info("No event IDs found for upcoming matches.")
            return
        try:
            async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
                async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1:
                    df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(match_ids, event_ids, faceit_data, faceit_data_v1)

        except Exception as e:
            function_logger.error(f"Error while fetching match details: {e}")
            return 
        
        event_ids = df_matches['event_id'].unique().tolist()
        if event_ids and isinstance(event_ids, list):
            df_events = gather_internal_event_ids(event_ids=event_ids)
            df_matches = df_matches.merge(
                df_events[['event_id', 'internal_event_id']],
                on='event_id',
                how='left'
            )
        
        upload_data("matches", df_matches)
        upload_data("teams_matches", df_teams_matches)
        upload_data("teams", df_teams)
        upload_data("maps", df_maps)
        upload_data("teams_maps", df_teams_maps)
        upload_data("players_stats", df_players_stats)
        upload_data("players", df_players)
        
    except Exception as e:
        function_logger.error(f"An error occurred during the update matches process: {e}")
        return

async def update_esea_teams_benelux():
    try:
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData_v1(dispatcher) as faceit_data_v1: 
                # Updates the teams_benelux data to the database
                df_teams_benelux = await process_teams_benelux_esea(faceit_data_v1=faceit_data_v1, season_number="ALL")
        
        # Gather the events and seasons data
        event_ids = df_teams_benelux['event_id'].tolist()
        if not isinstance(event_ids, list) or not event_ids:
            event_ids = [event_ids]
        
        team_ids = df_teams_benelux['team_id'].tolist()
        if not isinstance(team_ids, list) or not team_ids:
            team_ids = [team_ids]
          
        df_event_players = gather_event_players(event_ids=event_ids, team_ids=team_ids, PAST=True)
        # Use this dataframe to replace players_main and players_sub in df_teams_benelux for each team_id, event_id combination
        if not df_event_players.empty:
            # Merge on both keys, prioritizing df_event_players data
            df_teams_benelux = df_teams_benelux.set_index(['team_id', 'event_id'])
            df_event_players = df_event_players.set_index(['team_id', 'event_id'])

            # Only update overlapping rows/columns
            df_teams_benelux.update(df_event_players)

            # Optionally, re-add new rows if df_event_players has any team/event pairs not in df_teams_benelux
            new_rows = df_event_players[~df_event_players.index.isin(df_teams_benelux.index)]
            df_teams_benelux = pd.concat([df_teams_benelux, new_rows])

            df_teams_benelux.reset_index(inplace=True)
         
        df_teams_benelux = modify_keys(df_teams_benelux)
        
        if not isinstance(df_teams_benelux, pd.DataFrame) or df_teams_benelux.empty:
            function_logger.warning("No teams found for the Benelux ESEA events.")
            return
        
        # Gather team  and player data
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1: 
                
                team_ids = df_teams_benelux['team_id'].unique().tolist()
                player_ids = df_teams_benelux['players_main'].explode().apply(pd.Series).drop_duplicates(subset='player_id')['player_id'].unique().tolist()
                if isinstance(team_ids, str):
                    team_ids = [team_ids]
                if isinstance(player_ids, str):
                    player_ids = [player_ids]
                
                df_teams = await process_team_details_batch(team_ids=team_ids, faceit_data=faceit_data)
                df_players = await process_player_details_batch(player_ids=player_ids, faceit_data_v1=faceit_data_v1)
                
                if not isinstance(df_teams, pd.DataFrame) or df_teams.empty:
                    function_logger.warning("No team details found for the Benelux ESEA teams.")
                else:
                    upload_data("teams", df_teams)
                if not isinstance(df_players, pd.DataFrame) or df_players.empty:
                    function_logger.warning("No player details found for the Benelux ESEA teams.")
                else:
                    upload_data("players", df_players)
    
        if isinstance(df_teams_benelux, pd.DataFrame) and not df_teams_benelux.empty:
            # Remove all data from the teams_benelux table
            upload_data("teams_benelux", pd.DataFrame(), clear=True)
            
            upload_data("teams_benelux", df_teams_benelux)
    
    except Exception as e:
        function_logger.error(f"Error updating teams_benelux table: {e}", exc_info=True)


# === Hourly update interval ===
async def update_new_matches_hub():
    """ Gathers and updates new matches from the Benelux Hub """
    hub_id = "801f7e0c-1064-4dd1-a960-b2f54f8b5193"  # Benelux Hub ID
    try:
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1:
                ## Gathering matches in hub
                df_hub_matches = await gather_hub_matches(hub_id, faceit_data=faceit_data)
                
                if df_hub_matches.empty:
                    function_logger.info("No matches found for the hub.")
                    return
                
                match_ids_database = gather_event_matches(event_ids=[hub_id])
                df_matches_new = df_hub_matches[~df_hub_matches['match_id'].isin(match_ids_database)].drop_duplicates()
                
                match_ids = df_matches_new['match_id'].unique().tolist()
                if isinstance(match_ids, str):
                    match_ids = [match_ids]
                event_ids = [hub_id]*len(match_ids)
                
                if not match_ids or not event_ids:
                    function_logger.info("No new matches found for Hub.")
                    return
                
                df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
                    match_ids=match_ids, 
                    event_ids=event_ids, 
                    faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
                )
                
                df_events = gather_internal_event_ids(event_ids=event_ids)
                df_matches = df_matches.merge(
                    df_events[['event_id', 'internal_event_id']],
                    on='event_id',
                    how='left'
                )
        
        upload_data("matches", df_matches)
        upload_data("teams_matches", df_teams_matches)
        upload_data("teams", df_teams)
        upload_data("maps", df_maps)
        upload_data("teams_maps", df_teams_maps)
        upload_data("players_stats", df_players_stats)
        upload_data("players", df_players)
                
    except Exception as e:
        function_logger.error(f"Error updating Benelux Hub matches: {e}")
        return

async def update_leaderboard(elo_cutoff=2000):
    try:
        # Gather the leaderboard data from the API
        df_leaderboard = await get_benelux_leaderboard_players(elo_cutoff=elo_cutoff)

        # Gather the df_players and df_players_country dataframes from the database
        df_players = gather_players(benelux=False)
        
        # Check if the dataframes are valid and not empty
        if df_leaderboard.empty:
            function_logger.warning("No leaderboard data found. Skipping update.")
            raise ValueError("Leaderboard data is empty or not a DataFrame.")
        
        ## ----- For the players table -----
        await update_leaderboard_players(df_leaderboard=df_leaderboard, df_players=df_players)
            
        # ## ----- For the players_country table -----
        # await update_leaderboard_players_country(df_leaderboard=df_leaderboard)

    except Exception as e:
        function_logger.error(f"Error updating leaderboard: {e}")
        raise

async def update_leaderboard_players(df_leaderboard: pd.DataFrame, df_players: pd.DataFrame):
    try:
        if df_players.empty:
            function_logger.warning("No players data found. Skipping update.")
            raise ValueError("Players data is empty or not a DataFrame.")
        
        # Check if there are new players in the leaderboard
        df_new_players = df_leaderboard[~df_leaderboard['player_id'].isin(df_players['player_id'])]

        # Gather player data from these new players and the players in df_players
        if df_new_players.empty:
            function_logger.info("No new players found in the leaderboard for players table.")
            player_ids = df_players['player_id'].tolist()
        else:
            player_ids = df_players['player_id'].tolist() + df_new_players['player_id'].tolist()
            
        async with RequestDispatcher(request_limit=350, interval=10, concurrency=5) as dispatcher:
            async with FaceitData_v1(dispatcher) as faceit_data_v1: 
                df_players_new = await process_player_details_batch(player_ids, faceit_data_v1)
            
        # Update the players table with the new players
        function_logger.info(f"Found {len(df_players_new)} players to upload to players table.")
        upload_data('players', df_players_new)
    except Exception as e:
        function_logger.error(f"Error updating players table: {e}")
        raise

async def update_leaderboard_players_country(df_leaderboard: pd.DataFrame):
    """ Update the players_country table with new players from the leaderboard """
    # try:
    #     df_players_country = gather_players_country()
            
    #     if df_players_country.empty:
    #         function_logger.warning("No players_country data found. Skipping update.")
    #         raise ValueError("Players_country data is empty or not a DataFrame.")
        
    #     df_new_players_country = df_leaderboard[~df_leaderboard['player_id'].isin(df_players_country['player_id'])]
        
    #     if df_new_players_country.empty:
    #         function_logger.info("No new players found in the leaderboard for players_country table.")
    #         print("No new players found in the leaderboard for players_country table. Skipping update.")
    #     else:
    #         # Check benelux for the players in new_players_country
    #         player_ids = df_new_players_country['player_id'].tolist()
    #         df_players_country_details = run_async(process_player_country_details(player_ids))

    #         if df_players_country_details.empty:
    #             function_logger.warning("No player details found for the new players in the leaderboard.")
    #             raise ValueError("Player details for new players are empty or not a DataFrame.")
            
    #         df_predict = predict(df_players_country_details)

    #         if df_predict.empty:
    #             function_logger.warning("No predictions made for the new players in the leaderboard.")
    #             raise ValueError("Predictions for new players are empty or not a DataFrame.")
            
    #         # For the new players with a '1' prediction, update the players_country tabl        
    #         df_predict = df_predict.merge(
    #             df_leaderboard[['player_id', 'player_name']],
    #             on='player_id',
    #             how='left'
    #         )
            
    #         df_predict_benelux = df_predict.loc[df_predict['prediction'] == 1].rename(columns={'bnlx_country': 'country'})[['player_id', 'player_name', 'country']]
    #         df_predict_non_benelux = df_predict.loc[df_predict['prediction'] == 0].rename(columns={'all_country': 'country'})[['player_id', 'player_name', 'country']]
            
    #         df_predict_to_upload = pd.concat([df_predict_benelux, df_predict_non_benelux], ignore_index=True)
            
    #         if df_predict_to_upload.empty:
    #             function_logger.warning("No players to upload to players_country table.")
    #             raise ValueError("No players to upload to players_country table.")
            
    #         # Ask user for confirmation before uploading
    #         confirm = False
    #         print(f"Found {len(df_predict_to_upload)} new players to upload to players_country table.")
            
    #         # Simple regex for 2-letter lowercase country codes
    #         valid_country_code = re.compile(r"^[a-zA-Z]{2}$")
            
    #         while not confirm:
    #             isSure = input("Do you want to upload these players? (y/n/manual): ").strip().lower()
    #             if isSure in ['y', 'yes']:
    #                 print("Uploading new players to players_country table...")
    #                 confirm = True
    #             elif isSure in ['n', 'no']:
    #                 print("Skipping upload of new players to players_country table.")
    #                 return
    #             elif isSure in ['manual']:
                    
    #                 for idx, row in df_predict_to_upload.iterrows():
    #                     player_name = row['player_name']
    #                     current_country = row['country']
                        
    #                     print(f"\nPlayer: {player_name} - [{idx}/{len(df_predict_to_upload)}]")
    #                     print(f"Current Country: {current_country}")
    #                     print(df_predict.loc[df_predict['player_id'] == row['player_id']][['benelux_sum', 'friend_frac', 'is_benelux_hub', 'InHub']].to_string(index=False))
    #                     print(f"Faceit profile: https://www.faceit.com/en/players/{player_name}")
    #                     sys.stdout.flush()  # Forces output
                        
    #                     while True:
    #                         user_input = input("Press 'y' to confirm or type the correct country code: ").strip().lower()
                            
    #                         if user_input.lower() == 'y':
    #                             print("✅ Country confirmed.")
    #                             break
    #                         elif valid_country_code.match(user_input):
    #                             df_predict_to_upload.at[idx, 'country'] = user_input
    #                             print(f"✅ Updated country to: {user_input}")
    #                             break
    #                         else:
    #                             print("❌ Invalid input. Please enter 'y' or a valid 2-letter country code (e.g., us, de).")
                    
    #                 confirm = True
        
    #         # Update the players_country table with the new players
    #         upload_data('players_country', df_predict_to_upload)
    # except Exception as e:
    #     function_logger.error(f"Error updating players_country table: {e}")
    #     raise
    
# === Daily update interval ===
async def update_new_matches_esea():
    """ Gathers and updates new matches from ESEA events """
    try:
        df_event_teams = gather_event_teams(ONGOING=True, ESEA=True)
        
        if df_event_teams.empty:
            function_logger.warning("No teams found for ongoing ESEA events.")
            return
        
        event_ids = df_event_teams['event_id'].tolist()
        team_ids = df_event_teams['team_id'].tolist()
        
        if not isinstance(event_ids, list):
            if event_ids is None:
                function_logger.warning("No event IDs found for ongoing ESEA events.")
                return
            event_ids = [event_ids]
        if not isinstance(team_ids, list):
            if team_ids is None:
                function_logger.warning("No team IDs found for ongoing ESEA events.")
                return
            team_ids = [team_ids]
        
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1:
                df_esea_matches = await gather_esea_matches(
                    team_ids, 
                    event_ids, 
                    faceit_data_v1=faceit_data_v1, 
                    match_amount="ALL",
                    match_amount_type="ANY",
                    from_timestamp=0,
                ) # Get the match ids from the teams
                
                if df_esea_matches.empty:
                    function_logger.info("No matches found for ongoing ESEA events.")
                    return
                
                # Create dataframe with match_id and event_id for unique match_ids
                match_ids_database = gather_event_matches(event_ids=df_event_teams['event_id'].tolist())

                df_matches_events = df_esea_matches[~df_esea_matches['match_id'].isin(match_ids_database)][['match_id', 'event_id']].drop_duplicates()
                
                match_ids = df_matches_events['match_id'].unique().tolist()
                if isinstance(match_ids, str):
                    match_ids = [match_ids]
                event_ids = df_matches_events['event_id'].to_list()

                if not match_ids or not event_ids:
                    function_logger.info("No new matches found for ESEA events.")
                    return
                
                ## Processing matches in esea
                df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(
                    match_ids=match_ids, 
                    event_ids=event_ids, 
                    faceit_data=faceit_data, faceit_data_v1=faceit_data_v1
                )
                
                df_events = gather_internal_event_ids(event_ids=event_ids)
                df_matches = df_matches.merge(
                    df_events[['event_id', 'internal_event_id']],
                    on='event_id',
                    how='left'
                )
                
        upload_data("matches", df_matches)
        upload_data("teams_matches", df_teams_matches)
        upload_data("teams", df_teams)
        upload_data("maps", df_maps)
        upload_data("teams_maps", df_teams_maps)
        upload_data("players_stats", df_players_stats)
        upload_data("players", df_players)
        
    except Exception as e:
        function_logger.error(f"Error updating ESEA matches: {e}")


# === Weekly update interval ===
async def update_hub_events():
    hub_id = "801f7e0c-1064-4dd1-a960-b2f54f8b5193"  # Benelux Hub ID
    try:
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData_v1(dispatcher) as faceit_data_v1:
                df_events = await gather_event_details(event_id=hub_id, event_type="hub", faceit_data_v1=faceit_data_v1)
                
                if isinstance(df_events, pd.DataFrame) and not df_events.empty:
                    ## Add internal_event_id to df_events (a combination of event_id and stage_id)
                    df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)
                    
                    df_events = modify_keys(df_events)
                    
                    if isinstance(df_events, pd.DataFrame) and not df_events.empty:
                        upload_data("events", df_events)
         
    except Exception as e:
        function_logger.error(f"Error updating hub events: {e}")

async def update_esea_seasons_events():
    try:
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData_v1(dispatcher) as faceit_data_v1:
                
                # Gather df_seasons and df_events
                df_seasons, df_events = await process_esea_season_data(faceit_data_v1=faceit_data_v1)
                
                ## Add internal_event_id to df_events and df_matches (a combination of event_id and stage_id)
                df_events['internal_event_id'] = df_events['event_id'].astype(str) + "_" + df_events['stage_id'].astype(str)

                df_seasons = modify_keys(df_seasons)
                df_events = modify_keys(df_events)
                
                if isinstance(df_seasons, pd.DataFrame) and not df_seasons.empty:
                    upload_data("seasons", df_seasons)
                
                if isinstance(df_events, pd.DataFrame) and not df_events.empty:
                    upload_data("events", df_events)    
    except Exception as e:
        function_logger.error(f"Error updating seasons and events tables: {e}")

# === Main function to run all updates ===
async def main():
    if len(sys.argv) > 2:
        print("Usage: python cron_tasks.py [every5min|hourly|daily]")
        sys.exit(1)
    
    task = sys.argv[1]
    
    if task == "every5min":
        await update_matches()
        await update_esea_teams_benelux()
    elif task == "hourly":
        await update_new_matches_hub()
        await update_leaderboard(elo_cutoff=2000)
    elif task == "daily":
        await update_new_matches_esea()
    elif task == "weekly":
        await update_hub_events()
        await update_esea_seasons_events()
    else:
        print(f"Unknown task {task}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
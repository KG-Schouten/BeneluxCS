## Imports
from database.db_down import gather_players
from database.db_down_update import gather_upcoming_matches, gather_event_players, gather_event_teams, gather_event_matches, gather_internal_event_ids, gather_elo_snapshot, gather_league_teams_merged, gather_league_team_avatars, gather_league_teams, gather_ongoing_matches
from database.db_up import upload_data
from data_processing.api.sliding_window import RequestDispatcher
from data_processing.api.faceit_v4 import FaceitData
from data_processing.api.faceit_v1 import FaceitData_v1
from data_processing.dp_general import process_matches, process_team_details_batch, process_player_details_batch, gather_event_details
from data_processing.dp_events import process_teams_benelux_esea, gather_esea_matches, gather_hub_matches, process_esea_season_data, modify_keys
from data_processing.dp_benelux import get_benelux_leaderboard_players

from BeneluxWebb.website import socketio

import pandas as pd
import requests
from pathlib import Path
from io import BytesIO
import io
from PIL import Image
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from logs.update_logger import get_logger

import os
from dotenv import load_dotenv
load_dotenv()
FACEIT_TOKEN = os.getenv("FACEIT_TOKEN")

update_logger = get_logger("update_logger")

# === General functions ===
async def update_matches(match_ids: list, event_ids: list):
    update_logger.info(f"[START] Updating matches: {len(match_ids)} matches to process.")
    try:
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1:
                df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await process_matches(match_ids, event_ids, faceit_data, faceit_data_v1)

    except Exception as e:
        update_logger.error(f"Error while fetching match details: {e}")
        return 
    
    if not isinstance(df_matches, pd.DataFrame) or df_matches.empty:
        update_logger.info("No match details found for matches.")
        return
    
    event_ids = df_matches['event_id'].unique().tolist()
    if event_ids and isinstance(event_ids, list):
        df_events = gather_internal_event_ids(event_ids=event_ids)
        df_matches = df_matches.merge(
            df_events[['event_id', 'internal_event_id']],
            on='event_id',
            how='left'
        )
    
    dataframes = {
        "matches": df_matches,
        "teams_matches": df_teams_matches,
        "teams": df_teams,
        "maps": df_maps,
        "teams_maps": df_teams_maps,
        "players_stats": df_players_stats,
        "players": df_players,
    }
    
    for name, df in dataframes.items():
        if not df.empty:
            upload_data(name, df)
            
            if name == "matches":
                try:
                    socketio.emit('match_update', {'match_ids': match_ids})
                except Exception:
                    pass
            
        else:
            update_logger.debug(f"No data to upload for {name}.")
    
    update_logger.info(f"[END] Updated matches: {len(match_ids)} matches processed.")

async def update_streamers(streamer_ids: list = [], streamer_names: list = []):
    from data_processing.api.twitch import get_twitch_streamer_info, get_twitch_stream_info
    
    update_logger.info("[START] Updating streamer information.")
    
    info_streams = get_twitch_stream_info(streamer_ids=streamer_ids, streamer_names=streamer_names)
    
    remaining_names = []
    remaining_ids = []
    
    live_ids = {s['user_id'] for s in info_streams}
    live_logins = {s['user_login'].lower() for s in info_streams}

    remaining_ids = [sid for sid in streamer_ids if sid not in live_ids]
    remaining_names = [sname for sname in streamer_names if sname.lower() not in live_logins]
    
    if not remaining_ids and not remaining_names:
        info_streamer = []
    else:
        info_streamer = get_twitch_streamer_info(streamer_ids=remaining_ids, streamer_names=remaining_names)
    
    streamers = []
    if info_streams:
        for stream in info_streams:
            try:
                streamers.append(
                    {
                        'user_id': stream['user_id'],
                        'user_name': stream['user_name'],
                        'user_login': stream['user_login'],
                        'platform': 'twitch',
                        'live': True,
                        'viewer_count': stream['viewer_count'],
                        'game': stream['game_name'],
                        'thumbnail_url': stream['thumbnail_url'],
                        'stream_title': stream['title'],
                    }
                )
            except Exception as e:
                update_logger.error(f"Error processing stream data: {stream} - {e}", exc_info=True)
                continue
    
    if info_streamer:
        for streamer in info_streamer:
            try:
                if not any(s['user_id'] == streamer.get('id') for s in streamers):
                    streamers.append(
                        {
                            'user_id': streamer.get('id'),
                            'user_name': streamer.get('display_name'),
                            'user_login': streamer.get('login'),
                            'platform': 'twitch',
                            'live': False,
                            'viewer_count': 0,
                            'game': '',
                        }
                    )
            except Exception as e:
                update_logger.error(f"Error processing streamer data: {streamer} - {e}", exc_info=True)
                continue
        
    if streamers:
        df_streamers = pd.DataFrame(streamers)
        upload_data('streams', df_streamers, preserve_existing=True)
        
        try:
            socketio.emit('streamer_update', {'streamer_ids': streamer_ids, 'streamer_names': streamer_names})
        except Exception:
            pass
        
        update_logger.info(f"[END] Updated streamer information for {len(df_streamers)} streamers.")
    else:
        update_logger.info("No streamer data to update.")

# === Minute update interval ===
async def update_ongoing_matches():
    try:
        df_ongoing = gather_ongoing_matches()
        
        if df_ongoing.empty:
            return

        update_logger.info(f"[START] Updating ongoing matches: Found {len(df_ongoing)} matches.")
        
        match_ids = df_ongoing['match_id'].tolist()
        event_ids = df_ongoing['event_id'].tolist()
        
        if not match_ids or not event_ids:
            update_logger.error("No match_id or event_id found.")
            return
        
        await update_matches(match_ids, event_ids)
        
        update_logger.info(f"[END] Updating ongoing matches: Updated {len(df_ongoing)} matches.")
        
    except Exception as e:
        update_logger.error(f"An error occurred during the update ongoing matches process: {e}", exc_info=True)
        return

# === 20 Minutes update interval ===
async def update_upcoming_matches():
    """ Update matches from the database """
    ## Main logic
    try:
        df_upcoming = gather_upcoming_matches()
        
        if df_upcoming.empty:
            return
        
        update_logger.info(f"[START] Updating upcoming matches: Found {len(df_upcoming)} matches.")
        
        match_ids = df_upcoming['match_id'].tolist()
        event_ids = df_upcoming['event_id'].tolist()
        
        if not match_ids:
            update_logger.info("No upcoming matches found.")
            return 
        if not event_ids:
            update_logger.info("No event IDs found for upcoming matches.")
            return
        
        await update_matches(match_ids, event_ids)
        
        update_logger.info(f"[END] Updating upcoming matches: Updated {len(df_upcoming)} matches.")
        
    except Exception as e:
        update_logger.error(f"An error occurred during the upcoming matches update: {e}", exc_info=True)
        return

async def update_esea_teams_benelux(team_ids: list = [], event_ids: list = [], season_numbers: list = []):
    try:
        update_logger.info("[START] Starting update of ESEA Benelux teams.")
        
        if team_ids or event_ids or season_numbers:
            clear = False  # Do not clear if specific IDs are provided
        else:
            clear = True  # Clear the table if no specific IDs are provided
        
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData_v1(dispatcher) as faceit_data_v1: 
                # Updates the teams_benelux data to the database                
                df_teams_benelux = await process_teams_benelux_esea(faceit_data_v1=faceit_data_v1, team_ids=team_ids, event_ids=event_ids, season_numbers=season_numbers)
        
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

            # # Optionally, re-add new rows if df_event_players has any team/event pairs not in df_teams_benelux
            # new_rows = df_event_players[~df_event_players.index.isin(df_teams_benelux.index)]
            # df_teams_benelux = pd.concat([df_teams_benelux, new_rows])

            df_teams_benelux.reset_index(inplace=True)
        
        if not isinstance(df_teams_benelux, pd.DataFrame) or df_teams_benelux.empty:
            update_logger.warning("No teams found for the Benelux ESEA events.")
            return
        
        # Gather team  and player data
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1: 
                
                team_ids = df_teams_benelux['team_id'].unique().tolist()
                player_ids = (
                    pd.concat([df_teams_benelux['players_main'], df_teams_benelux['players_sub']])
                    .explode()
                    .apply(pd.Series)
                    .drop_duplicates(subset='player_id')
                    ['player_id']
                    .unique()
                    .tolist()
                )

                if isinstance(team_ids, str):
                    team_ids = [team_ids]
                if isinstance(player_ids, str):
                    player_ids = [player_ids]
                
                df_teams = await process_team_details_batch(team_ids=team_ids, faceit_data=faceit_data)
                df_players = await process_player_details_batch(player_ids=player_ids, faceit_data_v1=faceit_data_v1)
                
                if not isinstance(df_teams, pd.DataFrame) or df_teams.empty:
                    update_logger.warning("No team details found for the Benelux ESEA teams.")
                else:
                    upload_data("teams", df_teams)
                
                if not isinstance(df_players, pd.DataFrame) or df_players.empty:
                    update_logger.warning("No player details found for the Benelux ESEA teams.")
                else:
                    upload_data("players", df_players)
    
        if isinstance(df_teams_benelux, pd.DataFrame) and not df_teams_benelux.empty:
            upload_data("teams_benelux", df_teams_benelux, clear=clear)

        update_logger.info("[END] Finished update of ESEA Benelux teams.")
        
    except Exception as e:
        update_logger.error(f"Error updating teams_benelux table: {e}", exc_info=True)
        return

# === Hourly update interval ===
async def update_new_matches_hub():
    """ Gathers and updates new matches from the Benelux Hub """
    hub_id = "801f7e0c-1064-4dd1-a960-b2f54f8b5193"  # Benelux Hub ID
    try:
        update_logger.info("[START] Updating new matches from Benelux Hub.")
        
        async with RequestDispatcher(request_limit=100, interval=10, concurrency=5) as dispatcher:
            async with FaceitData(FACEIT_TOKEN, dispatcher) as faceit_data, FaceitData_v1(dispatcher) as faceit_data_v1:
                ## Gathering matches in hub
                df_hub_matches = await gather_hub_matches(hub_id, faceit_data=faceit_data)
                
                if df_hub_matches.empty:
                    update_logger.info("No matches found for the hub.")
                    return
                
                match_ids_database = gather_event_matches(event_ids=[hub_id])
                df_matches_new = df_hub_matches[~df_hub_matches['match_id'].isin(match_ids_database)].drop_duplicates()
                
                match_ids = df_matches_new['match_id'].unique().tolist()
                if isinstance(match_ids, str):
                    match_ids = [match_ids]
                event_ids = [hub_id]*len(match_ids)
                
                if not match_ids or not event_ids:
                    update_logger.info("No new matches found for Hub.")
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
            
        dataframes = {
            "matches": df_matches,
            "teams_matches": df_teams_matches,
            "teams": df_teams,
            "maps": df_maps,
            "teams_maps": df_teams_maps,
            "players_stats": df_players_stats,
            "players": df_players,
        }
        
        for name, df in dataframes.items():
            if not df.empty:
                upload_data(name, df)
            else:
                update_logger.debug(f"No data to upload for {name}.")
        
        update_logger.info("[END] Finished updating new matches from Benelux Hub.")
             
    except Exception as e:
        update_logger.error(f"Error updating Benelux Hub matches: {e}", exc_info=True)
        return

async def update_leaderboard(elo_cutoff=2000):
    try:
        update_logger.info("[START] Updating leaderboard players.")
        
        # Gather the leaderboard data from the API
        df_leaderboard = await get_benelux_leaderboard_players(elo_cutoff=elo_cutoff)

        # Gather the df_players dataframe from the database
        # Gather the df_players dataframe from the database
        df_players = gather_players(benelux=False)
        
        # Check if the dataframes are valid and not empty
        if df_leaderboard.empty:
            update_logger.warning("No leaderboard data found. Skipping update.")
            return
        
        ## ----- For the players table -----
        await update_leaderboard_players(df_leaderboard=df_leaderboard, df_players=df_players)

        update_logger.info("[END] Finished updating leaderboard players.")
    except Exception as e:
        update_logger.error(f"Error updating leaderboard: {e}", exc_info=True)
        return

async def update_leaderboard_players(df_leaderboard: pd.DataFrame, df_players: pd.DataFrame):
    try:
        update_logger.info("[START] Updating leaderboard players table.")
        if df_players.empty:
            update_logger.warning("No players data found. Skipping update.")
            return
        
        # Check if there are new players in the leaderboard
        df_new_players = df_leaderboard[~df_leaderboard['player_id'].isin(df_players['player_id'])]

        # Gather player data from these new players and the players in df_players
        if df_new_players.empty:
            update_logger.info("No new players found in the leaderboard for players table.")
            player_ids = df_players['player_id'].tolist()
        else:
            player_ids = df_players['player_id'].tolist() + df_new_players['player_id'].tolist()
            
        async with RequestDispatcher(request_limit=350, interval=10, concurrency=5) as dispatcher:
            async with FaceitData_v1(dispatcher) as faceit_data_v1: 
                df_players_new = await process_player_details_batch(player_ids, faceit_data_v1)
            
        # Update the players table with the new players
        # update_logger.debug(f"Found {len(df_players_new)} players to upload to players table.")
        if not df_players_new.empty:
            upload_data('players', df_players_new)
        
        update_logger.info("[END] Finished updating leaderboard players table.")
    except Exception as e:
        update_logger.error(f"Error updating players table: {e}", exc_info=True)
        return

async def update_elo_leaderboard():
    """ Update the elo_leaderboard_daily table with daily snapshots """
    try:
        update_logger.info("[START] Updating elo_leaderboard_daily table.")
        
        df_elo = gather_elo_snapshot()
        if df_elo.empty:
            update_logger.warning("No elo snapshot data found. Skipping update.")
            return
        
        today = date.today()
        df_elo['date'] = today
        
        upload_data('elo_leaderboard_daily', df_elo)
        
        update_logger.info("[END] Finished updating elo_leaderboard_daily table.")
        
    except Exception as e:
        update_logger.error(f"Error updating elo_leaderboard table: {e}", exc_info=True)
        return
    
async def update_new_matches_esea():
    """ Gathers and updates new matches from ESEA events """
    try:
        update_logger.info("[START] Updating new matches from ESEA.")
        
        df_event_teams = gather_event_teams(ONGOING=True, ESEA=True)
        
        if df_event_teams.empty:
            update_logger.warning("No teams found for ongoing ESEA events.")
            return
        
        event_ids = df_event_teams['event_id'].tolist()
        team_ids = df_event_teams['team_id'].tolist()
        
        if not isinstance(event_ids, list):
            if event_ids is None:
                update_logger.warning("No event IDs found for ongoing ESEA events.")
                return
            event_ids = [event_ids]
        if not isinstance(team_ids, list):
            if team_ids is None:
                update_logger.warning("No team IDs found for ongoing ESEA events.")
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
                    update_logger.info("No matches found for ongoing ESEA events.")
                    return
                
                # Create dataframe with match_id and event_id for unique match_ids
                match_ids_database = gather_event_matches(event_ids=df_event_teams['event_id'].tolist())

                df_matches_events = df_esea_matches[~df_esea_matches['match_id'].isin(match_ids_database)][['match_id', 'event_id']].drop_duplicates()
                
                match_ids = df_matches_events['match_id'].unique().tolist()
                if isinstance(match_ids, str):
                    match_ids = [match_ids]
                event_ids = df_matches_events['event_id'].to_list()

                if not match_ids or not event_ids:
                    update_logger.info("No new matches found for ESEA events.")
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
               
        dataframes = {
            "matches": df_matches,
            "teams_matches": df_teams_matches,
            "teams": df_teams,
            "maps": df_maps,
            "teams_maps": df_teams_maps,
            "players_stats": df_players_stats,
            "players": df_players,
        }
        
        for name, df in dataframes.items():
            if not df.empty:
                upload_data(name, df)
            else:
                update_logger.debug(f"No data to upload for {name}.")
            
        update_logger.info("[END] Finished updating new matches from ESEA.")
        
    except Exception as e:
        update_logger.error(f"Error updating ESEA matches: {e}", exc_info=True)
        return

# === Daily update interval ===
async def update_league_teams():
    try:
        update_logger.info("[START] Updating league_teams table.")
        
        # Gather the league teams data from the API
        df_teams, team_names_updated = gather_league_teams_merged()
            
        if isinstance(team_names_updated, list) and team_names_updated:
            update_logger.info(f"Found {len(team_names_updated)} teams with updated names.")
            update_logger.info(team_names_updated)
        else:
            update_logger.info("No teams found for the league_teams update.")
            
        if isinstance(df_teams, pd.DataFrame) and not df_teams.empty:
            upload_data("league_teams", df_teams, clear=False)
        else:
            update_logger.info("No teams found for the league_teams update.")
            return
        
        update_logger.info("[END] Finished updating league_teams table.")
        
        
    except Exception as e:
        update_logger.error(f"Error updating league_teams table: {e}", exc_info=True)
        return

async def update_team_avatars():
    try:
        update_logger.info("[START] Updating team avatars.")
        
        df_avatars = gather_league_team_avatars()
        
        data = []
        for _, row in df_avatars.iterrows():
            team_id = row['team_id']
            season_number = row['season_number']
            team_name = row['team_name']
            division_name = row['division_name']
            avatar_url = row['avatar']
            
            if not avatar_url or not isinstance(avatar_url, str) or not avatar_url.startswith("http"):
                continue
            
            try:
                response = requests.get(avatar_url)
                response.raise_for_status()
                
                image = Image.open(BytesIO(response.content))
                
                png_bytes_io = BytesIO()
                image.save(png_bytes_io, format='PNG')
                png_bytes = png_bytes_io.getvalue()
                
                data.append({
                    'team_id': team_id,
                    'season_number': season_number,
                    'team_name': team_name,
                    'division_name': division_name,
                    'avatar': png_bytes
                })
            
            except Exception as e:
                update_logger.error(f"Error downloading or processing avatar for team {team_id}: {e}")
                continue
        
        if data:
            df_team_leagues = pd.DataFrame(data)
            upload_data("league_teams", df_team_leagues)
        
        update_logger.info("[END] Finished updating team avatars.")
        
        return  

    except Exception as e:
        update_logger.error(f"Error updating team avatars: {e}", exc_info=True)
        return

def save_avatar(team_id, season_number, avatar_bytes, project_root, master=False):
    try:
        update_logger.debug(f"Saving avatar for {team_id}_{season_number}, master={master}")
        
        if master:
            save_folder = Path(project_root) / "BeneluxWebb" / "static" / "img" / "avatars_master"
            filename = f"{team_id}_{season_number}.png"
        else:
            save_folder = Path(project_root) / "BeneluxWebb" / "static" / "img" / "avatars"
            filename = f"{team_id}_{season_number}.webp"
        
        save_folder.mkdir(parents=True, exist_ok=True)

        file_path = save_folder / filename
        
        # Save avatar_bytes to file
        if not avatar_bytes:
            return f"No avatar data for {team_id}_{season_number}"
        
        if master:
            # Save the PNG bytes directly
            with open(file_path, "wb") as f:
                f.write(avatar_bytes)
            return_print = f"Saved (master) {team_id}_{season_number}"
        else:
            # Convert from avatar_bytes (PNG) to WebP
            image = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA")
            image.save(file_path, "WEBP", quality=80, method=6)
            return_print = f"Saved {team_id}_{season_number}"
        
        return return_print
        
    except Exception as e:
        update_logger.error(f"Error saving avatar for {team_id}_{season_number}: {e}", exc_info=True)
        return

async def update_local_team_avatars():
    update_logger.info("[START] Updating local team avatars.")
    
    df = gather_league_teams()

    # Determine project root (always BeneluxCS)
    try:
        # Running from a Python file
        project_root = Path(__file__).resolve().parent
    except NameError:
        # Running in Jupyter, fallback to cwd
        cwd = Path(os.getcwd()).resolve()
        # If cwd is inside BeneluxCS, use it; otherwise look for BeneluxCS in path
        for parent in [cwd] + list(cwd.parents):
            if parent.name == "BeneluxCS":
                project_root = parent
                break
        else:
            update_logger.critical("Cannot determine project root (BeneluxCS).")
            return

    tasks = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        for _, row in df.iterrows():
            avatar_data = row["avatar"]
            if not avatar_data:
                update_logger.debug(f"No avatar for {row['team_id']}, season {row['season_number']}")
                continue
            
            avatar_bytes = bytes(avatar_data)
            tasks.append(executor.submit(
                save_avatar,
                row["team_id"],
                row["season_number"],
                avatar_bytes,
                project_root,
                master=False
            ))

        for future in as_completed(tasks):
            update_logger.debug(future.result())
    
    update_logger.info("[END] Finished updating local team avatars.")

   
# === Weekly update interval ===
async def update_hub_events():
    update_logger.info("[START] Updating hub events.")
    
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
        
        update_logger.info("[END] Finished updating hub events.")
        
    except Exception as e:
        update_logger.error(f"Error updating hub events: {e}", exc_info=True)

async def update_esea_seasons_events():
    try:
        update_logger.info("[START] Updating ESEA seasons and events tables.")
        
        # Gather the seasons and events data from the API
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
        
        update_logger.info("[END] Finished updating ESEA seasons and events tables.")
           
    except Exception as e:
        update_logger.error(f"Error updating seasons and events tables: {e}", exc_info=True)
        return

# === Streamer updates ===
async def update_twitch_streams_benelux():
    from data_processing.api.twitch import get_twitch_streams_benelux
    
    update_logger.info("[START] Updating Twitch streams for Benelux streamers.")
    
    streams = get_twitch_streams_benelux()
    
    try:
        if streams:
            streamer_ids = [stream['user_id'] for stream in streams]
            
            if streamer_ids:
                await update_streamers(streamer_ids=streamer_ids)
            else:
                update_logger.error(f"Issue gathering streamer IDs: {streams}")    
        
        update_logger.info(f"[END] Updated Twitch streams for Benelux streamers. Found {len(streams)} streamers.")
        
    except Exception as e:
        update_logger.error(f"Error updating Twitch streams for Benelux streamers: {e}", exc_info=True)
        return

async def update_live_streams():
    from database.db_down_update import gather_live_streams
    
    try:
        stream_ids = gather_live_streams()
        
        if not stream_ids:
            return
        
        update_logger.info(f"[START] Updating live streams: Found {len(stream_ids)} streamers to check.")
        
        await update_streamers(streamer_ids=stream_ids)
        
        update_logger.info(f"[END] Updated live streams: Processed {len(stream_ids)} streamers.")
        
    except Exception as e:
        update_logger.error(f"An error occurred while updating live streams: {e}", exc_info=True)
        return

async def update_eventsub_subscriptions():
    from data_processing.api.twitch import get_twitch_eventSub_subscriptions, twitch_eventsub_subscribe, twitch_eventsub_unsubscribe
    from database.db_down_update import gather_streamers
    
    update_logger.info("[START] Updating Twitch EventSub subscriptions.")
    
    try:
        # Get existing subscriptions
        existing_subscriptions = get_twitch_eventSub_subscriptions()
        subscription_ids = [
            {
                'user_id': sub['condition']['broadcaster_user_id'],
                'subscription_id': sub['id']
            }
            for sub in existing_subscriptions
        ]
        subscribed_streamer_ids_set = set([sub['condition']['broadcaster_user_id'] for sub in existing_subscriptions])
        
        # Get streamers from database
        df_streamers = gather_streamers(platforms=['twitch'])
        db_streamer_ids = df_streamers['user_id'].tolist()
        streamer_ids_set = set(db_streamer_ids)
        
        # Get lists of new and removed streamer IDs
        new_streamer_ids = list(streamer_ids_set - subscribed_streamer_ids_set)
        removed_streamer_ids = list(subscribed_streamer_ids_set - streamer_ids_set)
        removed_subscriptions = [sub['subscription_id'] for sub in subscription_ids if sub['user_id'] in removed_streamer_ids]
        
        # Subscribe to new streamers
        update_logger.debug(f"New streamer IDs to subscribe: {new_streamer_ids}")
        if new_streamer_ids:
            twitch_eventsub_subscribe(streamer_ids=new_streamer_ids)
        
        # Unsubscribe from removed streamers
        update_logger.debug(f"Subscription IDs to unsubscribe: {removed_subscriptions}")
        if removed_subscriptions:
            twitch_eventsub_unsubscribe(subscription_ids=removed_subscriptions)
    
        update_logger.info("[END] Updated Twitch EventSub subscriptions.")
        
    except Exception as e:
        update_logger.error(f"An error occurred while updating EventSub subscriptions: {e}", exc_info=True)
       

if __name__ == "__main__":
    pass
    # asyncio.run(update_esea_teams_benelux())
    # asyncio.run(update_league_teams())
    # asyncio.run(update_team_avatars())
    # asyncio.run(update_esea_seasons_events())
    # asyncio.run(update_local_team_avatars())
    # asyncio.run(update_new_matches_esea())
    # asyncio.run(update_league_teams())
    # asyncio.run(update_team_avatars())
    # asyncio.run(update_local_team_avatars())
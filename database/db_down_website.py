# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timezone, timedelta, time
from dateutil.relativedelta import relativedelta
import pandas as pd
import json
import re
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
import base64

from database.db_manage import start_database, close_database
from database.db_down import get_player_aliases

from logs.update_logger import get_logger

function_logger = get_logger("functions")

# =============================
#       General functions
# =============================
def safe_load_json(value):
    # Handle PostgreSQL jsonb format
    if value is None:
        return []
    if isinstance(value, (list, dict)):
        return value if isinstance(value, list) else []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    return []

def compute_division_rank(division_name):
    division_order = {"Advanced": 0, "Main": 1, "Intermediate": 2, "Entry": 3}
    if division_name in division_order:
        return division_order[division_name]
    elif division_name and division_name.lower().startswith("open"):
        match = re.search(r'(\d+)', division_name)
        return 4 + (100 - int(match.group(1))) if match else 999
    return 999

def gather_columns_mapping():
    columns_mapping = {
        'adr':                  {'name': 'ADR',         'round': 0},
        'k_r_ratio':            {'name': 'K/R',         'round': 2},
        'k_d_ratio':            {'name': 'K/D',         'round': 2, 'good': 1.05, 'bad': 0.95},
        'headshots_percent':    {'name': 'HS %',        'round': 0},
        'hltv':                 {'name': 'HLTV',    'round': 2, 'good': 1.05, 'bad': 0.95},
        'maps_played':          {'name': 'Maps Played', 'round': 0},
    }
    
    return columns_mapping

def start_of_day(date):
    return datetime.combine(date, time.min)

def end_of_day(date):
    return datetime.combine(date, time.max)

def get_date_range(option: str):
    now = datetime.now()
    
    # Normalize input for case-insensitive matching
    normalized_option = option.strip().lower()
    
    match normalized_option:
        case "all time":
            return []
        case "this week":
            monday = now - timedelta(days=now.weekday())
            start = start_of_day(monday)
            end = end_of_day(now)
        case "last week":
            last_monday = now - timedelta(days=now.weekday() + 7)
            last_sunday = last_monday + timedelta(days=6)
            start = start_of_day(last_monday)
            end = end_of_day(last_sunday)
        case "this month":
            start = start_of_day(now.replace(day=1))
            end = end_of_day(now)
        case "last month":
            first_of_this_month = now.replace(day=1)
            last_day_last_month = first_of_this_month - timedelta(days=1)
            start = start_of_day(last_day_last_month.replace(day=1))
            end = end_of_day(last_day_last_month)
        case "last 3 months":
            three_months_ago = (now.replace(day=1) - relativedelta(months=3))
            start = start_of_day(three_months_ago)
            end = end_of_day(now)
        case "last 6 months":
            six_months_ago = (now.replace(day=1) - relativedelta(months=6))
            start = start_of_day(six_months_ago)
            end = end_of_day(now)
        case "this year":
            start = start_of_day(now.replace(month=1, day=1))
            end = end_of_day(now)
        case _:
            print(f"Unknown date range option: {option}")
            return []
    return [
        int(start.replace(tzinfo=timezone.utc).timestamp()),
        int(end.replace(tzinfo=timezone.utc).timestamp())
    ]

def bytes_to_data_url(b: bytes, mime="image/png") -> str:
    """Convert bytes to a data URL that can be used in <img src=''>."""
    if b is None:
        return None
    encoded = base64.b64encode(b).decode("utf-8")
    return f"data:{mime};base64,{encoded}"

def gather_current_streams() -> list:
    """ Gathers current live streams from the database """
    load_dotenv()
    use_fake = os.getenv("USE_FAKE_DATA", "false").lower() == "true"
    
    if use_fake:
        with open("database/fake_data/fake_streamers.json", "r", encoding="utf-8") as f:
            fake_data = json.load(f)
            print("Using fake upcoming matches data")
            return fake_data
    
    db, cursor = start_database()
    try:
        query = """
            SELECT
                user_id,
                user_name,
                user_login,
                platform,
                viewer_count,
                streamer_type
            FROM streams
            WHERE live = TRUE AND game = 'Counter-Strike'
            ORDER BY viewer_count DESC
        """
        
        cursor.execute(query)
        res = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        current_streams = [
            {
                col: (safe_load_json(val) if col == "streamer_type" else val)
                for col, val in zip(cols, row)
            }
            for row in res
        ]
        
        return current_streams
        
    except Exception as e:
        function_logger.error(f"Error gathering current streams: {e}")
        return []
    
    finally:
        close_database(db)
        
# =============================
#           ESEA Page
# =============================

def gather_esea_season_info() -> list:
    """ Gathers ESEA season number and associated event start/end dates """
    
    db, cursor = start_database()
    try:
        query = """
            SELECT 
                s.season_number,
                e.event_start,
                e.event_end,
                e.event_banner,
                e.registration_end,
                e.roster_lock
            FROM seasons s
            LEFT JOIN events e ON s.event_id = e.event_id
            WHERE s.season_number > 49
            ORDER BY s.season_number DESC
        """
        
        cursor.execute(query)
        res = cursor.fetchall()

        # Use a dict to keep the first occurrence of each unique season_number
        unique_seasons = {}
        for row in res:
            season_number = row[0]
            if season_number not in unique_seasons:
                unique_seasons[season_number] = {
                    'season_number': season_number,
                    'event_start': row[1],
                    'event_end': row[2],
                    'event_banner': row[3],
                    'registration_end': row[4],
                    'roster_lock': row[5]
                }

        # Convert dict values to list and sort descending by season_number
        season_info = sorted(unique_seasons.values(), key=lambda x: x['season_number'], reverse=True)
        
        return season_info

    except Exception as e:
        function_logger.error(f"Error gathering ESEA season info: {e}")
        raise

    finally:
        close_database(db)

def gather_esea_teams_benelux(szn_number: int | str = "ALL") -> dict:
    db, cursor = start_database()
    try:   
        df_teams_benelux = gather_teams_benelux(szn_number=szn_number)
        if df_teams_benelux.empty:
            function_logger.warning("No ESEA teams found in the Benelux region.")
            return {}
        
        df_teams_benelux["division_sort_rank"] = df_teams_benelux["division_name"].apply(compute_division_rank)

        
        # Pre-load all team names, player data, matches, and map stats for efficiency
        all_team_ids = df_teams_benelux['team_id'].unique().tolist()
        all_season_numbers = df_teams_benelux['season_number'].unique().tolist()
        
        # Batch load all player data
        all_player_ids = set()
        for _, row in df_teams_benelux.iterrows():
            if pd.notna(row['stage_name']) and 'regular' in str(row['stage_name']).lower():
                players_main = safe_load_json(row['players_main'])
                players_sub = safe_load_json(row['players_sub'])
                players_coach = safe_load_json(row['players_coach'])
                for p in players_main + players_sub + players_coach:
                    if isinstance(p, dict) and 'player_id' in p:
                        all_player_ids.add(p['player_id'])

        players_data = {}
        if all_player_ids:
            player_ids_list = list(all_player_ids)
            placeholders = ','.join(f"'{pid}'" for pid in player_ids_list)
            cursor.execute(f"""
                SELECT
                    p.player_id,
                    p.player_name AS player_name,
                    p.avatar AS player_avatar,
                    COALESCE(pc.country, p.country) AS player_country,
                    p.faceit_elo AS player_elo,
                    p.faceit_level AS player_faceit_level
                FROM players p
                LEFT JOIN players_country pc ON p.player_id = pc.player_id
                WHERE p.player_id IN ({placeholders})
            """)
            for row in cursor.fetchall():
                players_data[row[0]] = {
                    'player_id': row[0],
                    'player_name': row[1],
                    'player_avatar': row[2],
                    'player_country': row[3],
                    'player_elo': row[4],
                    'player_faceit_level': row[5]
                }
        
        # Batch load all matches data
        matches_data = {}
        if all_team_ids and all_season_numbers:            
            
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_season_teams (
                    season_number INTEGER,
                    team_id TEXT
                ) ON COMMIT DROP
            """)
            
            cursor.execute("""
                INSERT INTO temp_season_teams (season_number, team_id)
                SELECT DISTINCT s.season_number, tm.team_id
                FROM matches m
                JOIN seasons s ON m.event_id = s.event_id
                JOIN teams_matches tm ON tm.match_id = m.match_id
                WHERE tm.team_id = ANY(%s) 
                AND s.season_number = ANY(%s)
            """, (list(all_team_ids), list(all_season_numbers)))
            
            cursor.execute("""
                WITH team_matches AS (
                    SELECT 
                        m.match_id, m.match_time, m.winner_id, m.status, m.score,
                        tm.team_id AS our_id, tm.team_name AS our_name,
                        opp.team_id AS opp_id, opp.team_name AS opp_name, opp.avatar AS opp_avatar,
                        s.season_number
                    FROM matches m
                    JOIN seasons s ON m.event_id = s.event_id
                    JOIN teams_matches tm ON tm.match_id = m.match_id
                    JOIN teams_matches opp ON opp.match_id = m.match_id AND opp.team_id != tm.team_id
                    JOIN temp_season_teams tst ON s.season_number = tst.season_number AND tm.team_id = tst.team_id
                ),
                map_counts AS (
                    SELECT match_id, COUNT(DISTINCT match_round) AS map_count FROM maps GROUP BY match_id
                ),
                map_scores AS (
                    SELECT match_id, team_id, SUM(COALESCE(team_win, 0)) AS win_count
                    FROM teams_maps
                    GROUP BY match_id, team_id
                ),
                bo1_scores AS (
                    SELECT match_id, score FROM maps WHERE match_round = 1
                ),
                match_maps AS (
                    SELECT match_id, ARRAY_AGG(map ORDER BY match_round) AS maps_played
                    FROM maps
                    GROUP BY match_id
                )
                SELECT
                    t.season_number,
                    t.our_id AS team_id,
                    t.match_id,
                    t.match_time,
                    t.status,
                    t.opp_id,
                    t.opp_name,
                    t.opp_avatar,
                    CASE
                        WHEN t.status = 'FINISHED' THEN
                            CASE
                                WHEN t.our_id = t.winner_id THEN 'W'
                                ELSE 'L'
                            END
                        ELSE NULL
                    END AS result,
                    COALESCE(mc.map_count, 1) AS map_count,
                    COALESCE(ms.win_count, 0) AS our_score,
                    COALESCE(ms_opp.win_count, 0) AS opp_score,
                    bs.score AS bo1_score,
                    mm.maps_played
                FROM team_matches t
                LEFT JOIN map_counts mc ON t.match_id = mc.match_id
                LEFT JOIN map_scores ms ON t.match_id = ms.match_id AND ms.team_id = t.our_id
                LEFT JOIN map_scores ms_opp ON t.match_id = ms_opp.match_id AND ms_opp.team_id = t.opp_id
                LEFT JOIN bo1_scores bs ON t.match_id = bs.match_id
                LEFT JOIN match_maps mm ON t.match_id = mm.match_id
                ORDER BY t.season_number, t.our_id, t.match_time DESC
            """)
            
            all_match_rows = cursor.fetchall()
            for row in all_match_rows:
                season_num = row[0]
                team_id = row[1]
                if (team_id, season_num) not in matches_data:
                    matches_data[(team_id, season_num)] = []
                matches_data[(team_id, season_num)].append(row[2:])

            for key in matches_data:
                if matches_data[key]:
                    cols = [desc[0] for desc in cursor.description][2:]
                    matches_data[key] = pd.DataFrame(matches_data[key], columns=cols)
        
        # Batch load player stats data
        player_stats_data = {}
        if all_team_ids and all_season_numbers:  
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_season_teams_players (
                    season_number INTEGER,
                    team_id TEXT,
                    player_id TEXT
                ) ON COMMIT DROP
            """)
            
            cursor.execute("""
                INSERT INTO temp_season_teams_players (season_number, team_id, player_id)
                SELECT DISTINCT s.season_number, tm.team_id, ps.player_id
                FROM players_stats ps
                JOIN matches m ON ps.match_id = m.match_id
                JOIN seasons s ON m.event_id = s.event_id
                JOIN teams_matches tm ON tm.match_id = m.match_id
                WHERE tm.team_id = ANY(%s) 
                AND s.season_number = ANY(%s)
            """, (list(all_team_ids), list(all_season_numbers)))
            
            cursor.execute("""
                WITH player_match_stats AS (
                    SELECT
                        s.season_number,
                        ps.team_id,
                        ps.player_id,
                        p.player_name,
                        COALESCE(pc.country, p.country) AS country,
                        ps.match_id,
                        ps.match_round,
                        ps.adr,
                        ps.headshots,
                        ps.kills,
                        ps.deaths,
                        ps.hltv
                    FROM players_stats ps
                    JOIN players p ON ps.player_id = p.player_id
                    JOIN matches m ON ps.match_id = m.match_id
                    JOIN seasons s ON m.event_id = s.event_id
                    LEFT JOIN players_country pc ON p.player_id = pc.player_id
                    JOIN temp_season_teams_players t 
                        ON s.season_number = t.season_number
                    AND ps.team_id = t.team_id
                    AND ps.player_id = t.player_id
                )
                SELECT
                    season_number,
                    team_id,
                    player_id,
                    player_name,
                    country,
                    COUNT(DISTINCT (match_id, match_round)) AS maps_played,
                    AVG(adr) AS adr,
                    SUM(headshots) * 100.0 / NULLIF(SUM(kills), 0) AS headshots_percent,
                    SUM(kills) * 1.0 / NULLIF(SUM(deaths), 0) AS k_d_ratio,
                    AVG(hltv) AS hltv
                FROM player_match_stats
                GROUP BY season_number, team_id, player_id, player_name, country
                ORDER BY season_number, team_id, player_id
            """)
            
            all_rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            for row in all_rows:
                season_num = row[0]
                team_id = row[1]
                player_id = row[2]
                key = (player_id, team_id, season_num)
                player_stats_data[key] = dict(zip(cols[3:], row[3:]))
        
        # Batch load map stats data
        map_stats_data = {}
        
        map_pools = {}
        if all_season_numbers:
            placeholders = ','.join(['%s'] * len(all_season_numbers))
            cursor.execute(f"""
                SELECT s.season_number, e.maps 
                FROM events e
                JOIN seasons s ON e.event_id = s.event_id
                WHERE s.season_number IN ({placeholders})
            """, all_season_numbers)
            
            for row in cursor.fetchall():
                season_num, maps = row
                # Handle PostgreSQL jsonb format properly
                if maps:
                    # For PostgreSQL, maps is already a Python list
                    if isinstance(maps, list):
                        map_pools[season_num] = maps
                    # For string format (SQLite compatibility)
                    elif isinstance(maps, str):
                        try:
                            parsed = json.loads(maps)
                            map_pools[season_num] = parsed if isinstance(parsed, list) else []
                        except (json.JSONDecodeError, TypeError):
                            map_pools[season_num] = []
                    else:
                        map_pools[season_num] = []
                else:
                    map_pools[season_num] = []
        
        if all_team_ids and all_season_numbers:
            # Create a temporary table for map stats query
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_map_stats (
                    season_number INTEGER,
                    team_id TEXT
                ) ON COMMIT DROP
            """)
            
            # Insert combinations into temp table
            map_stats_combos = []
            for season_num in all_season_numbers:
                for team_id in all_team_ids:
                    map_stats_combos.append((season_num, team_id))
            
            insert_values = ','.join(cursor.mogrify("(%s,%s)", combo).decode('utf-8') 
                                    for combo in map_stats_combos)
            cursor.execute(f"INSERT INTO temp_map_stats VALUES {insert_values}")
            
            # Execute optimized query for all team map stats at once
            cursor.execute("""
                SELECT
                    s.season_number,
                    tm.team_id,
                    ma.map,
                    COUNT(tm.team_id) as played,
                    SUM(COALESCE(tm.team_win, 0)) as won
                FROM teams_maps tm
                JOIN matches m ON tm.match_id = m.match_id
                JOIN seasons s ON m.event_id = s.event_id
                JOIN maps ma ON ma.match_id = tm.match_id AND ma.match_round = tm.match_round
                JOIN temp_map_stats tms ON s.season_number = tms.season_number AND tm.team_id = tms.team_id
                WHERE ma.map IS NOT NULL
                GROUP BY s.season_number, tm.team_id, ma.map
                ORDER BY s.season_number, tm.team_id, ma.map
            """)
            
            # Process all map stats and organize by (team_id, season_number)
            map_stats_by_map = {}
            for row in cursor.fetchall():
                season_num, team_id, map_name, played, won = row
                key = (team_id, season_num)
                
                if key not in map_stats_by_map:
                    map_stats_by_map[key] = {}
                
                # Calculate winrate
                winrate = round((won / played) * 100, 1) if played > 0 else 0
                
                # Store map stats
                map_stats_by_map[key][map_name] = {
                    "map_name": map_name,
                    "played": played,
                    "won": int(won),
                    "winrate": winrate
                }
            
            # Ensure all maps from pool exist in final output for each team/season
            for key in map_stats_combos:
                team_id, season_num = key[1], key[0]  # Reversed from combos
                final_map_stats = []
                map_pool = map_pools.get(season_num, [])
                
                # Get map stats for this team/season combo, defaulting to empty dict
                team_map_stats = map_stats_by_map.get((team_id, season_num), {})
                
                # Ensure all maps from pool exist in final output
                for map_name in map_pool:
                    stats = team_map_stats.get(map_name, {
                        "map_name": map_name,
                        "played": 0,
                        "won": 0,
                        "winrate": 0.0
                    })
                    final_map_stats.append(stats)
                
                # Store the complete map stats for this team/season
                map_stats_data[(team_id, season_num)] = final_map_stats
        
        # Now, construct the final nested dictionary output
        esea_data = {}
        for season_number, group_season in df_teams_benelux.sort_values(by=["season_number"], ascending=False).groupby("season_number", sort=False):
            esea_data[season_number] = {}
            for division_name, group_division in group_season.sort_values(by=["division_sort_rank"]).groupby('division_name', sort=False):
                esea_data[season_number][division_name] = []
                
                team_ids = group_division['team_id'].unique().tolist()
                if not team_ids:
                    function_logger.warning(f"No teams found for division {division_name} in season {season_number}.")
                    continue
                 
                for team_id, group_team in group_division.groupby('team_id'):
                    if not isinstance(group_team, pd.DataFrame) or group_team.empty:
                        function_logger.warning(f"No data found for team {team_id} in division {division_name}, season {season_number}.")
                        continue
                    
                    team_name = group_team['team_name'].iloc[0]
                    nickname = group_team['nickname'].iloc[0]
                    # team_avatar = bytes_to_data_url(group_team['team_avatar'].iloc[0]) if group_team['team_avatar'].iloc[0] else None
                    region_name = group_team['region_name'].iloc[0]
                    
                    
                    stages = [
                        {
                            'stage_name': stage,
                            'placement': json.loads(group_team.loc[group_team['stage_name'] == stage, 'placement'].iloc[0]), # type: ignore
                            'wins': group_team.loc[group_team['stage_name'] == stage, 'wins'].iloc[0], # type: ignore
                            'losses': group_team.loc[group_team['stage_name'] == stage, 'losses'].iloc[0] # type: ignore
                        }
                        for stage in group_team['stage_name'].unique().tolist()
                    ]
                    
                    stages.sort(key=lambda x: (1 if 'regular' in x['stage_name'].lower() else 0, x['stage_name']))

                    # Get all rows in the group as a list of dicts
                    players_main_all = group_team.loc[
                        group_team['stage_name'].str.contains('regular', case=False, na=False),
                        'players_main'
                    ].apply(safe_load_json).tolist()
                    players_sub_all = group_team.loc[
                        group_team['stage_name'].str.contains('regular', case=False, na=False),
                        'players_sub'
                    ].apply(safe_load_json).tolist()
                    players_coach_all = group_team.loc[
                        group_team['stage_name'].str.contains('regular', case=False, na=False),
                        'players_coach'
                    ].apply(safe_load_json).tolist()
                    
                    
                    # Pick the longest list from each category
                    players_main = max(players_main_all, key=len, default=[])
                    players_sub = max(players_sub_all, key=len, default=[])
                    players_coach = max(players_coach_all, key=len, default=[])

                    # Make sure there are no players in both main and sub lists
                    players_sub = [p for p in players_sub if p['player_id'] not in [pm['player_id'] for pm in players_main]]
                    
                    # Use pre-loaded player data
                    country_order = ['nl', 'be', 'lu']
                    players_main = [players_data.get(p['player_id'], p) for p in players_main if p['player_id'] in players_data]
                    players_main.sort(
                        key=lambda p: (
                            0 if p['player_country'] in country_order else 1,
                            country_order.index(p['player_country']) if p['player_country'] in country_order else 99,
                            p['player_country']
                        )
                    )
                    players_sub = [players_data.get(p['player_id'], p) for p in players_sub if p['player_id'] in players_data]
                    players_sub.sort(
                        key=lambda p: (
                            0 if p['player_country'] in country_order else 1,
                            country_order.index(p['player_country']) if p['player_country'] in country_order else 99,
                            p['player_country']
                        )
                    )
                    players_coach = [players_data.get(p['player_id'], p) for p in players_coach if p['player_id'] in players_data]

                    # Use pre-loaded matches data
                    df_matches = matches_data.get((team_id, season_number), pd.DataFrame())

                    map_stats_key = (str(team_id), int(season_number) if isinstance(season_number, (int, float)) else season_number)
                    map_stats = map_stats_data.get(map_stats_key, [])
                    
                    # If not found, try with original types (fallback)
                    if not map_stats:
                        map_stats = map_stats_data.get((team_id, season_number), [])
                    
                    recent_matches = []
                    upcoming_matches = []

                    for row in df_matches.itertuples(index=False):
                        if row.status == 'FINISHED':
                            if row.our_score == 0 and row.opp_score == 0:
                                score = "FFW" if row.result == "W" else "FFL"
                            elif row.map_count == 1 and row.bo1_score:
                                score = row.bo1_score
                            else:
                                score = f"{int(row.our_score)}/{int(row.opp_score)}"

                            recent_matches.append({
                                'match_id': row.match_id,
                                'result': row.result,
                                'opponent_id': row.opp_id,
                                'opponent': row.opp_name,
                                'opponent_avatar': row.opp_avatar,
                                'score': score,
                                'match_time': int(row.match_time) if pd.notna(row.match_time) else 0,
                                'maps_played': row.maps_played,
                            })

                        else:  # Upcoming matches
                            upcoming_matches.append({
                                'match_id': row.match_id,
                                'opponent_id': row.opp_id,
                                'opponent': row.opp_name,
                                'opponent_avatar': row.opp_avatar,
                                'match_time': int(row.match_time)
                            })

                    recent_matches.sort(key=lambda x: x['match_time'])
                    upcoming_matches.sort(key=lambda x: x['match_time'])

                    # Limit matches
                    match_limit = 4
                    recent_matches = recent_matches[-match_limit:]
                    upcoming_matches = upcoming_matches[:match_limit]

                    # Create the player_stats dict
                    player_stats = []
                    for (p_id, t_id, s_num), stats in player_stats_data.items():
                        if t_id == team_id and s_num == season_number:
                            player_stats.append({
                                "player_id": p_id,
                                **stats
                            })
                        
                    # Order players by hltv
                    player_stats_main = sorted(player_stats, key=lambda p: p["maps_played"], reverse=True)[:5]
                    player_stats_main.sort(key=lambda x: x['hltv'], reverse=True)
                    player_stats_sub = [p for p in player_stats if p not in player_stats_main]
                    player_stats_sub.sort(key=lambda x: x["hltv"], reverse=True)
                    
                    player_stats = player_stats_main + player_stats_sub
                    
                    team_dict = {
                        'team_id': team_id,
                        'team_name': team_name,
                        'nickname': nickname,
                        # 'team_avatar': team_avatar,
                        'players_main': players_main,
                        'players_sub': players_sub,
                        'players_coach': players_coach,
                        'stages': stages,
                        'season_number': season_number,
                        'region_name': region_name,
                        'division_name': division_name,
                        'matches': recent_matches,
                        'upcoming_matches': upcoming_matches,
                        'map_stats': map_stats,
                        'player_stats': player_stats
                    }

                    esea_data[season_number][division_name].append(team_dict)
        
        # Sort teams by standing within each division
        for season in esea_data:
            for division in esea_data[season]:
                esea_data[season][division].sort(key=lambda x: x['stages'][0]['placement']['left'], reverse=False)
        
        return esea_data

    except Exception as e:
        function_logger.error(f"Error gathering ESEA teams: {e}", exc_info=True)
        return {}

    finally:
        close_database(db)

def gather_teams_benelux(szn_number: int | str = "ALL") -> pd.DataFrame:
    """
    Gathers the teams from the Benelux region from the database
    """
    db, cursor = start_database()
    
    params = []
    if szn_number != "ALL":
        where_clause = "WHERE s.season_number = %s"
        params.append(szn_number)
    else:
        where_clause = ""

    try:
        query = f"""
            SELECT 
                tb.team_id, 
                tb.event_id, 
                tb.placement, 
                tb.wins, 
                tb.losses, 
                tb.players_main, 
                tb.players_sub, 
                tb.players_coach,
                lt.team_name,
                t.avatar AS team_avatar_current,
                t.nickname,
                s.season_number,
                s.region_name,
                s.division_name,
                s.stage_name
            FROM teams_benelux tb
            LEFT JOIN teams t ON tb.team_id = t.team_id
            LEFT JOIN seasons s ON tb.event_id = s.event_id
            LEFT JOIN league_teams lt ON tb.team_id = lt.team_id AND s.season_number = lt.season_number
            {where_clause}
        """
        
        cursor.execute(query, params)
        res = cursor.fetchall()
        
        df_teams_benelux = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        
        return df_teams_benelux
    
    except Exception as e:
        function_logger.error(f"Error gathering teams: {e}")
        return pd.DataFrame()
    
    finally:   
        close_database(db)
        
def gather_esea_map_stats(team_id, szn_number) -> list:
    db, cursor = start_database()
    try:
        # Gather map pool from events
        cursor.execute("""
            SELECT 
                e.maps 
            FROM events e
            LEFT JOIN seasons s ON e.event_id = s.event_id
            WHERE s.season_number = %s
        """, (szn_number,))
        rows = cursor.fetchall()
        map_pool = []
        for row in rows:
            if row[0]:
                try:
                    parsed = json.loads(row[0])
                    if isinstance(parsed, list):
                        map_pool = parsed
                        break  # use first valid one found
                except Exception:
                    continue
                
        # Gather team map stats
        cursor.execute("""
            SELECT
                tm.team_id,
                tm.team_win,
                ma.map
            FROM teams_maps tm
            LEFT JOIN maps ma ON ma.match_id = tm.match_id AND ma.match_round = tm.match_round
            LEFT JOIN matches m ON tm.match_id = m.match_id
            INNER JOIN seasons s ON m.event_id = s.event_id
            WHERE tm.team_id = %s AND s.season_number = %s
        """, (team_id, szn_number))

        map_rows = cursor.fetchall()
        df_team_maps = pd.DataFrame(map_rows, columns=[desc[0] for desc in cursor.description])
                
        # --- Calculate stats per map
        map_stats_dict = {}
        if not df_team_maps.empty:
            df_team_maps = df_team_maps[df_team_maps["map"].notna()]  # drop nulls just in case
            map_group = df_team_maps.groupby("map")
            for map_name, group in map_group:
                played = len(group)
                won = group["team_win"].sum()
                winrate = round((won / played) * 100, 1) if played > 0 else 0
                map_stats_dict[map_name] = {
                    "map_name": map_name,
                    "played": played,
                    "won": int(won),
                    "winrate": winrate
                }

        # --- Step 3: Ensure all maps from pool exist in final output
        final_map_stats = []
        for map_name in map_pool:
            stats = map_stats_dict.get(map_name, {
                "map_name": map_name,
                "played": 0,
                "won": 0,
                "winrate": 0.0
            })
            final_map_stats.append(stats)

        return final_map_stats
    except Exception as e:
        function_logger.error(f"Error gathering ESEA map stats: {e}")
        return []
    
    finally:
        close_database(db)

def gather_esea_seasons_divisions() -> tuple:
    """ Gathers all ESEA seasons and divisions from the database"""
    db, cursor = start_database()
    try:
        # Gather all seasons and divisions from teams_benelux
        cursor.execute("SELECT DISTINCT season_number, division_name FROM seasons WHERE event_id IN (SELECT event_id FROM teams_benelux)")
        rows = cursor.fetchall()
        season_numbers = [row[0] for row in rows if row[0] is not None]
        division_names = [row[1] for row in rows if row[1] is not None]
        
        season_numbers = sorted(set(season_numbers), reverse=True)  # Unique and sorted descending
        division_names = sorted(set(division_names), key=lambda x: (x.lower(), x))
        
        return season_numbers, division_names
    except Exception as e:
        function_logger.error(f"Error gathering ESEA seasons and divisions: {e}")
        return [], []
    finally:
        close_database(db)

def get_upcoming_matches() -> tuple:
    db, cursor = start_database()
    try:
        now = datetime.now()
        start_of_day = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
        end_of_day = int((datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)).timestamp())

        load_dotenv()
        use_fake = os.getenv("USE_FAKE_DATA", "false").lower() == "true"
        
        if use_fake:
            with open("database/fake_data/fake_matches_upcoming.json", "r", encoding="utf-8") as f:
                fake_data = json.load(f)
                print("Using fake upcoming matches data")
                return fake_data, 100
        
        query = """
            WITH match_teams AS (
                SELECT
                    m.match_id,
                    m.match_time,
                    m.status,
                    m.score,
                    s.division_name,
                    tm.team_id,
                    COALESCE(tb.team_name, t.team_name) AS team_name,
                    CASE WHEN tb.team_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_benelux,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.match_id 
                        ORDER BY CASE WHEN tb.team_id IS NOT NULL THEN 0 ELSE 1 END, tm.team_id
                    ) AS team_rank,
                    (
                        SELECT json_agg(
                                json_build_object(
                                    'match_round', tmap.match_round,
                                    'final_score', tmap.final_score,
                                    'team_win', tmap.team_win
                                ) ORDER BY tmap.match_round
                            )
                        FROM teams_maps tmap
                        WHERE tmap.match_id = m.match_id
                        AND tmap.team_id = tm.team_id
                    ) AS maps
                FROM matches m
                JOIN seasons s ON m.event_id = s.event_id
                JOIN teams_matches tm ON m.match_id = tm.match_id
                LEFT JOIN teams t ON tm.team_id = t.team_id
                LEFT JOIN teams_benelux tb 
                    ON tm.team_id = tb.team_id 
                    AND m.event_id = tb.event_id
                WHERE m.match_time >= %s
            )
            SELECT
                mt1.match_id,
                mt1.match_time,
                mt1.status,
                mt1.score,
                mt1.division_name,

                mt1.team_id AS team1_id,
                mt1.team_name AS team1_name,
                mt1.is_benelux AS team1_is_benelux,
                mt1.maps AS team1_maps,

                mt2.team_id AS team2_id,
                mt2.team_name AS team2_name,
                mt2.is_benelux AS team2_is_benelux,
                mt2.maps AS team2_maps
            FROM match_teams mt1
            JOIN match_teams mt2 
            ON mt1.match_id = mt2.match_id
            AND mt1.team_rank = 1
            AND mt2.team_rank = 2
            ORDER BY mt1.match_time ASC;
        """
        
        with db.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (start_of_day,))
            data = cursor.fetchall()
        
        matches = []
        for row in data:
            match = {
                "match_id": row["match_id"],
                "match_time": row["match_time"],
                "status": row["status"],
                "score": row["score"],
                "map_count": len(row["score"]) if (row["team1_maps"] is not None and row["team2_maps"] is not None) else None,
                "division_name": row["division_name"],
            }
            team1 = {
                "team_id": row["team1_id"],
                "team_name": row["team1_name"],
                "is_benelux": row["team1_is_benelux"],
                "maps": row["team1_maps"],
                "map_wins": sum(score.get(str(row["team1_id"]), 0) for score in row["score"] if row["score"] and isinstance(row["score"], list))
            }
            team2 = {
                "team_id": row["team2_id"],
                "team_name": row["team2_name"],
                "is_benelux": row["team2_is_benelux"],
                "maps": row["team2_maps"],
                "map_wins": sum(score.get(str(row["team2_id"]), 0) for score in row["score"] if row["score"] and isinstance(row["score"], list))
            }

            # Determine which is the Benelux team
            if team1["is_benelux"] and not team2["is_benelux"]:
                match["team"] = team1
                match["opponent"] = team2
            elif team2["is_benelux"] and not team1["is_benelux"]:
                match["team"] = team2
                match["opponent"] = team1
            elif team1["is_benelux"] and team2["is_benelux"]:
                # If both teams are Benelux, we can still include the match
                match["team"] = team1
                match["opponent"] = team2
            else:
                continue  # Skip matches where both teams are Benelux or neither is
            
            # Add match
            matches.append(match)

        # Step 2: Sort the division groups
        sorted_matches = sorted(
            matches,
            key=lambda m: (m["match_time"], compute_division_rank(m["division_name"]))
        )
        
        return sorted_matches, end_of_day

    except Exception as e:
        function_logger.error(f"Error fetching today's matches: {e}", exc_info=True)
        return {}, 0
    finally:
        close_database(db)

def get_esea_player_of_the_week() -> list:
    
    load_dotenv()
    use_fake = os.getenv("USE_FAKE_DATA", "false").lower() == "true"
    
    if use_fake:
        with open("database/fake_data/fake_potw.json", "r", encoding="utf-8") as f:
            fake_data = json.load(f)
            print("Using fake upcoming matches data")
            return fake_data
    
    db, cursor = start_database()
    try:
        start_of_week = int((datetime.now(timezone.utc) - timedelta(days=datetime.now(timezone.utc).weekday())).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        
        query = """
            SELECT
                p.player_id,
                p.player_name,
                p.avatar,
                COALESCE(pc.country, p.country) AS country,

                COUNT(DISTINCT CONCAT(m.match_id, '-', m.match_round)) AS maps_played,

                ROUND(AVG(ps.kills)::numeric, 0) AS kills,
                ROUND(AVG(ps.hltv)::numeric, 2) AS hltv,
                ROUND(SUM(ps.headshots)::numeric * 100.0 / NULLIF(SUM(ps.kills), 0), 0) AS headshot_percentage,
                ROUND(AVG(ps.adr)::numeric, 0) AS adr,
                SUM(ps.knife_kills) AS knife_kills,
                SUM(ps.penta_kills) AS penta_kills,
                SUM(ps.zeus_kills) AS zeus_kills

            FROM maps m
            JOIN matches ma ON m.match_id = ma.match_id
            INNER JOIN seasons s ON ma.event_id = s.event_id
            JOIN players_stats ps ON m.match_id = ps.match_id AND m.match_round = ps.match_round
            JOIN players p ON ps.player_id = p.player_id
            JOIN players_country pc ON p.player_id = pc.player_id

            WHERE ma.match_time >= %s
            AND COALESCE(pc.country, p.country) IN ('nl', 'be', 'lu')

            GROUP BY
                p.player_id,
                p.player_name,
                p.avatar,
                COALESCE(pc.country, p.country)
            ORDER BY
                maps_played DESC,
                hltv DESC,
                headshot_percentage DESC,
                adr DESC

        """
        
        cursor.execute(query, (start_of_week,))
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
        
        if df.empty:
            return []
        
        # Get the players with specific highest stats
        top_stats = []
        stats_to_check = {
            "hltv": "HLTV Rating",
            "headshot_percentage": "Headshots",
            "adr": "ADR",
            "knife_kills": "Knife Kills",
            "penta_kills": "Aces",
            "zeus_kills": "Zeus Kills"
        }
        
        player_set = set()
        for stat, desc in stats_to_check.items():
            df_sorted = df.sort_values(by=['maps_played', stat, 'kills'], ascending=[False, False, False]).head(5)
            for index, row in df_sorted.iterrows():
                value = row[stat]
                if value > 0 and row['player_id'] not in player_set:
                    top_stats.append(
                        {
                            "stat": stat,
                            "description": desc,
                            "value": f"{int(value)} %" if stat == "headshot_percentage" else value,
                            "player": row.to_dict()
                        }
                    )
                    player_set.add(row['player_id'])
                    break  # Only take the top player for this stat
                
        return top_stats
        
    except Exception as e:
        function_logger.error(f"Error fetching player of the week: {e}", exc_info=True)
        return []
    
    finally:
        close_database(db)

# =============================
#   Stats Page
# =============================
def gather_filter_options():
    db, cursor = start_database()
    
    try:
        # Gather events
        cursor.execute("""
            SELECT event_type
            FROM (
                SELECT DISTINCT event_type
                FROM events
            ) t
            ORDER BY LOWER(event_type), event_type;
        """)
        events = [row[0] for row in cursor.fetchall() if row[0]]
        
        # Gather seasons
        cursor.execute("""
            SELECT season_number
            FROM (
                SELECT DISTINCT season_number
                FROM seasons
                WHERE event_id IN (SELECT event_id FROM teams_benelux)
            ) t
            WHERE season_number IS NOT NULL
            ORDER BY season_number DESC;
        """)
        seasons = [row[0] for row in cursor.fetchall()]

        # Gather divisions (still needs Python sort because of custom rank)
        cursor.execute("""
            SELECT division_name
            FROM (
                SELECT DISTINCT division_name
                FROM seasons
                WHERE event_id IN (SELECT event_id FROM teams_benelux)
            ) t
            WHERE division_name IS NOT NULL;
        """)
        divisions = [row[0] for row in cursor.fetchall()]
        divisions = sorted(divisions, key=lambda d: compute_division_rank(d))

        # Gather maps_played range
        cursor.execute("""
            SELECT
                COUNT(DISTINCT ps.match_id || '-' || ps.match_round) AS maps_played
            FROM players_stats ps 
            GROUP BY ps.player_id
            ORDER BY maps_played DESC          
        """)
        players_maps = cursor.fetchall()
        max_maps = players_maps[0][0]
        
        # Move up int to next multiple of n
        if max_maps:
            n = 5
            max_maps = ((max_maps + (n-1)) // n) * n if max_maps % n != 0 else max_maps
        else:
            max_maps = 50
        # Gather teams
        teams = gather_filter_teams()
        
        return {
            "events": events,
            "seasons": seasons,
            "divisions": divisions,
            "max_maps": max_maps,
            "teams": teams
        }
        
    except Exception as e:
        function_logger.error(f"Error gathering filter options: {e}", exc_info=True)
        return {
            "events": [],
            "seasons": [],
            "divisions": [],
            "stages": []
        }
    finally:
        close_database(db)

def gather_filter_teams():
    db, cursor = start_database()
    
    try:
        cursor.execute("""
            WITH ranked_teams AS (
                SELECT
                    t.team_name,
                    t.team_id,
                    t.event_id,
                    tm.avatar,
                    e.event_start,
                    ROW_NUMBER() OVER (PARTITION BY t.team_name ORDER BY e.event_start DESC) AS rn
                FROM teams_benelux t
                JOIN events e ON t.event_id = e.event_id
                JOIN teams tm ON t.team_id = tm.team_id
            )
            SELECT
                team_name,
                json_agg(ARRAY[team_id::text, event_id::text] ORDER BY event_start DESC) AS team_ids,
                MAX(CASE WHEN rn = 1 THEN avatar END) AS avatar
            FROM ranked_teams
            GROUP BY team_name
            ORDER BY team_name;
        """)
        
        results = cursor.fetchall()
        teams = [
            {
                "team_name": row[0],
                "team_ids": row[1],
                "avatar": row[2]
            }
            for row in results
        ]
        
        return teams
        
    except Exception as e:
        function_logger.error(f"Error gathering filter teams: {e}", exc_info=True)
        return []
    finally:
        close_database(db)


# =============================
#    Stats ELO Leaderboard Page
# =============================
def gather_elo_ranges() -> list:
    """ Gathers the ELO ranges from the database """
    db, cursor = start_database()
    try:
        cursor.execute("""
            SELECT DISTINCT faceit_elo
            FROM players
            WHERE faceit_elo IS NOT NULL
            ORDER BY faceit_elo;
        """)
        rows = cursor.fetchall()
        elo_range = [rows[0][0], rows[-1][0]] if rows else []
        
        n = 100
        if elo_range and (elo_range[1] - elo_range[0]) % n != 0:
            elo_range[1] = ((elo_range[1] + (n-1)) // n) * n
            elo_range[0] = (elo_range[0] // n) * n
        
        return elo_range
    except Exception as e:
        function_logger.error(f"Error gathering ELO ranges: {e}", exc_info=True)
        return []
    finally:
        close_database(db)

def gather_elo_leaderboard(
    countries=None,
    elo=[]
    ) -> list:
    db, cursor = start_database()
    
    try:
        params = []
        conditions = []
        if not countries:
            countries = ['nl','be','lu']
        
        if countries:
            placeholders = ','.join(['%s'] * len(countries))
            conditions.append(f"(pc.country IN ({placeholders}) OR (pc.country IS NULL AND p.country IN ({placeholders})))")
            params.extend(countries)
            params.extend(countries)
        
        # Create the WHERE clause
        where_clause = f"AND {' AND '.join(conditions)}" if conditions else ""
        
        query = f"""
            SELECT
                el.player_id,
                el.faceit_elo,
                el.date,
                p.player_name,
                p.avatar,
                COALESCE(pc.country, p.country) AS country
            FROM elo_leaderboard_daily el
            JOIN players p ON el.player_id = p.player_id
            LEFT JOIN players_country pc ON p.player_id = pc.player_id
            WHERE el.date >= CURRENT_DATE - INTERVAL '7 days'
            AND p.faceit_elo > 2000
            {where_clause}           
        """
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        df = df.sort_values(["player_id", "date"])
        
        df["elo_change"] = df.groupby("player_id")["faceit_elo"].diff()
        df["elo_change"] = df["elo_change"].fillna(0).astype(int)
        
        df["rank"] = df.groupby("date")["faceit_elo"].rank(method="first", ascending=False)
        df["rank_change"] = df.groupby("player_id")["rank"].diff() * -1
        df["rank_change"] = df["rank_change"].fillna(0).astype(int)
        
        official_names, aliases = get_player_aliases(cursor)
        
        if isinstance(elo, list) and elo:
            if len(elo) != 2:
                elo = []
            else:
                elo = [int(val) for val in elo if isinstance(val, (int, float)) or (isinstance(val, str) and val.isdigit())]
        
        output = []
        for player_id, group in df.groupby("player_id"):
            group = group.sort_values("date", ascending=False)
            history = []

            latest = group.iloc[0]
            
            if elo:
                if latest["faceit_elo"] < elo[0] or latest["faceit_elo"] > elo[1]:
                    continue
            
            for _, row in group.iterrows():
                history.append({
                    "date": row["date"].isoformat() if isinstance(row["date"], (pd.Timestamp, datetime)) else str(row["date"]),
                    "faceit_elo": int(row["faceit_elo"]),  # ensure native int
                    "elo_change": int(row["elo_change"]) if pd.notna(row["elo_change"]) else 0,
                    "rank": int(row["rank"]) if pd.notna(row["rank"]) else None,
                    "rank_change": int(row["rank_change"]) if pd.notna(row["rank_change"]) else 0
                })
                       
            output.append({
                "player_id": str(player_id),  # ensure native int
                "player_name": str(latest["player_name"]),
                "avatar": str(latest["avatar"]) if latest["avatar"] else "",
                "alias": str(aliases.get(player_id, '')),
                "country": str(latest["country"]),
                "faceit_elo": int(latest["faceit_elo"]),
                "rank": int(latest["rank"]) if pd.notna(latest["rank"]) else None,
                "history": history
            })
        
        return output
        
    except Exception as e:
        function_logger.error(f"Error gathering ELO leaderboard: {e}", exc_info=True)
        return []
    finally:
        close_database(db)

# =============================
#       Stats Player Page
# =============================     
def gather_stat_table_columns():
    db, cursor = start_database()
    
    try:
        # Get stat columns and build AVG expressions
        non_stat_cols = {'player_id', 'player_name', 'team_id', 'match_id', 'match_round'}
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'players_stats'
        """)
        all_columns = [row[0] for row in cursor.fetchall()]
        stat_columns = [col for col in all_columns if col not in non_stat_cols]
        
        return stat_columns
    except Exception as e:
        function_logger.error(f"Error gathering stat table columns: {e}", exc_info=True)
        return []
    finally:
        close_database(db)

def gather_player_stats_esea(
    events=[],
    countries=[],
    seasons=[],
    divisions=[],
    stages=[],
    timestamp=None,
    maps_played=[],
    teams=[],
    ):
    
    # Convert timestamp to start_date and end_date
    if timestamp:
        timestamp = get_date_range(timestamp)
    else:
        timestamp = []
        
    # Make sure maps_played is a list of two integers
    maps_played = [int(val) for val in maps_played if isinstance(val, (int, float)) or (isinstance(val, str) and val.isdigit())]
    
    db, cursor = start_database()
    try:
        # === Get valid (player_id, team_id, event_id) combos ===
        valid_combos = set()
        if events:
            if 'hub' in events:
                cursor.execute(""" 
                    SELECT ps.player_id, ps.team_id, e.event_id
                    FROM players_stats ps
                    JOIN matches m ON ps.match_id = m.match_id
                    JOIN events e ON m.event_id = e.event_id
                    WHERE e.event_type = 'hub'
                """)
                valid_combos.update(cursor.fetchall())
            elif 'esea' in events:
                cursor.execute("""
                    SELECT ps.player_id, ps.team_id, m.event_id
                    FROM players_stats ps
                    LEFT JOIN matches m ON ps.match_id = m.match_id
                    WHERE (ps.team_id, m.event_id) IN (
                        SELECT tb.team_id, tb.event_id
                        FROM teams_benelux tb
                    )
                """)
                valid_combos.update(cursor.fetchall())
            
        # Build WHERE clause and parameters
        conditions = []
        params = []
        
        if events:
            if 'esea' in events and 'hub' in events:
                # Both selected, so get all from both queries above
                pass
            elif 'esea' in events:
                conditions.append("(ps.team_id, m.event_id) IN (SELECT tb.team_id, tb.event_id FROM teams_benelux tb)")
            elif 'hub' in events:
                conditions.append("e.event_type = 'hub'")
        
        if countries:
            placeholders = ','.join(['%s'] * len(countries))
            conditions.append(f"(pc.country IN ({placeholders}) OR (pc.country IS NULL AND p.country IN ({placeholders})))")
            params.extend(countries)
            params.extend(countries)

        if seasons:
            placeholders = ','.join(['%s'] * len(seasons))
            conditions.append(f"s.season_number IN ({placeholders})")
            params.extend(seasons)

        if divisions:
            like_clauses = ' OR '.join(["s.division_name LIKE %s"] * len(divisions))
            conditions.append(f"({like_clauses})")
            params.extend([f"%{d}%" for d in divisions])

        if stages:
            like_clauses = ' OR '.join(["LOWER(s.stage_name) LIKE %s"] * len(stages))
            conditions.append(f"({like_clauses})")
            params.extend([f"%{stage.lower()}%" for stage in stages])
        
        if timestamp and len(timestamp) == 2:
            start_date, end_date = timestamp
            conditions.append("m.match_time >= %s")
            params.append(int(start_date))
            
            conditions.append("m.match_time <= %s")
            params.append(int(end_date))
            
        if teams:
            placeholders = ','.join(['%s'] * len(teams))
            conditions.append(f"LOWER(tb.team_name) IN ({placeholders})")
            params.extend([t.lower() for t in teams if isinstance(t, str)]) 
            print(params, teams)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        stat_columns = gather_stat_table_columns()
        avg_expressions = [f'AVG(ps."{col}") AS "{col}"' for col in stat_columns]


        query = f"""
            SELECT
               ps.player_id,
               COALESCE(pc.country, p.country) AS country,
               COUNT(DISTINCT ps.match_id || '-' || ps.match_round) AS maps_played,
               ROUND(
                    CASE 
                        WHEN COUNT(DISTINCT ps.match_id || '-' || ps.match_round) > 0 
                        THEN (SUM(tm.team_win) * 100.0 / COUNT(DISTINCT ps.match_id || '-' || ps.match_round))
                        ELSE 0
                    END, 1
                ) AS map_win_pct,
               {', '.join(avg_expressions)}
            
            FROM players_stats ps
            
            JOIN matches m ON ps.match_id = m.match_id
            JOIN teams_maps tm ON ps.match_id = tm.match_id AND ps.match_round = tm.match_round AND ps.team_id = tm.team_id
            JOIN events e ON m.event_id = e.event_id
            LEFT JOIN seasons s ON m.event_id = s.event_id
            
            LEFT JOIN teams_benelux tb ON ps.team_id = tb.team_id AND m.event_id = tb.event_id
            LEFT JOIN players p ON ps.player_id = p.player_id
            LEFT JOIN players_country pc ON p.player_id = pc.player_id
            
            {where_clause}
            
            GROUP BY 
                ps.player_id, 
                COALESCE(pc.country, p.country)    
        """
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in rows]
        
        official_names, aliases = get_player_aliases(cursor)
        
        output = []
        for row in results:
            if maps_played and len(maps_played) == 2:
                if int(row["maps_played"]) < maps_played[0] or int(row["maps_played"]) > maps_played[1]:
                    continue
            
            flat_stats = {col: float(row[col]) for col in stat_columns}
            player_id = row["player_id"]
            output.append({
                "player_id": player_id,
                "player_name": official_names.get(player_id),
                "alias": aliases.get(player_id, ''),
                "country": row["country"],
                "maps_played": int(row["maps_played"]),
                "map_win_pct": float(row["map_win_pct"]),
                **flat_stats
            })
        
        return output

    except Exception as e:
        function_logger.error(f"Error gathering player stats: {e}", exc_info=True)
        return [], []
    finally:
        close_database(db)
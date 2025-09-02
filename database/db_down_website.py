# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timezone, timedelta
import pandas as pd
import json
import re
from collections import defaultdict
from dotenv import load_dotenv

from database.db_manage import start_database, close_database
from database.db_down import fuzzy_search, get_player_aliases
from data_processing.faceit_api.logging_config import function_logger

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
                e.event_banner
            FROM seasons s
            LEFT JOIN events e ON s.event_id = e.event_id
            WHERE s.event_id IN (
                SELECT event_id FROM teams_benelux
            )
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
                    'event_banner': row[3]
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
        df_teams_benelux = gather_teams_benelux()
        if df_teams_benelux.empty:
            function_logger.warning("No ESEA teams found in the Benelux region.")
            return {}
        
        # Gather info on ESEA szn if szn_number is not "ALL"
        if szn_number != "ALL":
            query = """
                SELECT
                    s.season_number,
                    e.event_start,
                    e.event_end
                FROM seasons s
                LEFT JOIN events e ON s.event_id = e.event_id
                WHERE s.season_number = %s
            """
            
            cursor.execute(query, (szn_number,))
            res = cursor.fetchone()
            if not res:
                function_logger.warning(f"No ESEA season found for season number {szn_number}.")
                return {'season_number': szn_number, 'event_start': 0, 'event_end': 0}
            szn_info = {
                'season_number': res[0],
                'event_start': res[1],
                'event_end': res[2]
            }
        else:
            szn_info = {'season_number': 'ALL', 'event_start': 0, 'event_end': 0}
        
        esea_data = {}

        division_order = {"Advanced": 0, "Main": 1, "Intermediate": 2, "Entry": 3}
        def compute_division_rank(division_name):
            if division_name in division_order:
                return division_order[division_name]
            elif division_name and division_name.lower().startswith("open"):
                match = re.search(r'(\d+)', division_name)
                return 4 + (100 - int(match.group(1))) if match else 999
            return 999

        df_teams_benelux["division_sort_rank"] = df_teams_benelux["division_name"].apply(compute_division_rank)

        if szn_number != "ALL":
            df_teams_benelux = df_teams_benelux[df_teams_benelux['season_number'] == szn_number]
            if df_teams_benelux.empty:
                function_logger.warning(f"No ESEA teams found for season {szn_number}.")
                return {}

        # Pre-load all team names, player data, matches, and map stats for efficiency
        all_team_ids = df_teams_benelux['team_id'].unique().tolist()
        all_season_numbers = df_teams_benelux['season_number'].unique().tolist()
        
        # Batch load team names
        import time
        cur_time = int(time.time())
        if all_team_ids and szn_info['event_start'] < cur_time < szn_info['event_end']:
            placeholders = ', '.join(['%s'] * len(all_team_ids))
            cursor.execute(f"""
                SELECT team_id, team_name 
                FROM teams
                WHERE team_id IN ({placeholders})
            """, all_team_ids)
            team_names_data = {row[0]: row[1] for row in cursor.fetchall()}
        else:
            team_names_data = {}
        
        # Batch load all player data
        all_player_ids = set()
        for _, row in df_teams_benelux.iterrows():
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
                    # match_limit = 12
                    # if len(matches_data[key]) > match_limit:
                    #     matches_data[key] = matches_data[key].head(match_limit)

        
        # Batch load player stats data
        player_stats_data = {}
        if all_team_ids and all_season_numbers and all_player_ids:  
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
                AND ps.player_id = ANY(%s)
            """, (list(all_team_ids), list(all_season_numbers), list(all_player_ids)))
            
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

        for season_number, group_season in df_teams_benelux.sort_values(by=["season_number"], ascending=False).groupby("season_number", sort=False):
            esea_data[season_number] = {}
            for division_name, group_division in group_season.sort_values(by=["division_sort_rank"]).groupby('division_name', sort=False):
                esea_data[season_number][division_name] = []
                
                team_ids = group_division['team_id'].unique().tolist()
                if not team_ids:
                    function_logger.warning(f"No teams found for division {division_name} in season {season_number}.")
                    continue
                 
                for team_id, group_team in group_division.groupby('team_id'):
                    team_name = group_team['team_name'].iloc[0]
                    team_name_cur = team_names_data.get(team_id, team_name)
                    nickname = group_team['nickname'].iloc[0]
                    team_avatar = group_team['team_avatar'].iloc[0]
                    region_name = group_team['region_name'].iloc[0]
                    
                    stages = [
                        {
                            'stage_name': stage,
                            'placement': json.loads(group_team.loc[group_team['stage_name'] == stage, 'placement'].iloc[0]),
                            'wins': group_team.loc[group_team['stage_name'] == stage, 'wins'].iloc[0],
                            'losses': group_team.loc[group_team['stage_name'] == stage, 'losses'].iloc[0]
                        }
                        for stage in group_team['stage_name'].unique().tolist()
                    ]
                    
                    stages.sort(key=lambda x: (1 if 'regular' in x['stage_name'].lower() else 0, x['stage_name']))

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
                    players_main = [players_data.get(p['player_id'], p) for p in players_main if p['player_id'] in players_data]
                    players_sub = [players_data.get(p['player_id'], p) for p in players_sub if p['player_id'] in players_data]
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
                    player_stats.sort(key=lambda x: x['hltv'], reverse=True)
                            
                    # print(f"{player_stats} \n\n")
                    
                    team_dict = {
                        'team_id': team_id,
                        'team_name': team_name,
                        'team_name_cur': team_name_cur,
                        'nickname': nickname,
                        'team_avatar': team_avatar,
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

        # # Sort teams by team_name within each division
        # for season in esea_data:
        #     for division in esea_data[season]:
        #         esea_data[season][division].sort(key=lambda x: x['team_name'])
        
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

def gather_teams_benelux() -> pd.DataFrame:
    """
    Gathers the teams from the Benelux region from the database
    """
    db, cursor = start_database()
    
    try:
        query = """
            SELECT 
                tb.team_id, 
                tb.event_id, 
                tb.team_name, 
                tb.placement, 
                tb.wins, 
                tb.losses, 
                tb.players_main, 
                tb.players_sub, 
                tb.players_coach,
                t.avatar AS team_avatar,
                t.nickname,
                s.season_number,
                s.region_name,
                s.division_name,
                s.stage_name
            FROM teams_benelux tb
            LEFT JOIN teams t ON tb.team_id = t.team_id
            LEFT JOIN seasons s ON tb.event_id = s.event_id
        """
        
        cursor.execute(query)
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
        now = datetime.now(timezone.utc)
        start_of_day = int(datetime(now.year, now.month, now.day, tzinfo=timezone.utc).timestamp())
        end_of_day = int((datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)).timestamp())

        load_dotenv()
        use_fake = os.getenv("USE_FAKE_DATA", "false").lower() == "true"
        
        if use_fake:
            with open("database/fake_data/fake_matches_upcoming.json", "r", encoding="utf-8") as f:
                fake_data = json.load(f)
                print("Using fake upcoming matches data")
                return fake_data, end_of_day
        
        query = """
            WITH match_teams AS (
                SELECT
                    m.match_id,
                    m.match_time,
                    m.status,
                    m.score,
                    s.division_name,
                    tm.team_id,
                    COALESCE(tb.team_name, tm.team_name) AS team_name,
                    t.avatar,
                    CASE WHEN tb.team_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_benelux,
                    ROW_NUMBER() OVER (PARTITION BY m.match_id ORDER BY CASE WHEN tb.team_id IS NOT NULL THEN 0 ELSE 1 END) AS team_rank
                FROM matches m
                JOIN seasons s ON m.event_id = s.event_id
                JOIN teams_matches tm ON m.match_id = tm.match_id
                LEFT JOIN teams t ON tm.team_id = t.team_id
                LEFT JOIN teams_benelux tb ON tm.team_id = tb.team_id AND m.event_id = tb.event_id
                WHERE m.match_time >= %s
            ),
            map_counts AS (
                SELECT match_id, COUNT(DISTINCT match_round) AS map_count
                FROM teams_maps
                GROUP BY match_id
            ),
            team_scores AS (
                SELECT
                    tm.match_id,
                    tm.team_id,
                    SUM(COALESCE(tm.team_win, 0)) AS map_wins,
                    SUM(COALESCE(tm.final_score, 0)) AS round_wins
                FROM teams_maps tm
                GROUP BY tm.match_id, tm.team_id
            )
            SELECT
                mt1.match_id,
                mt1.match_time,
                mt1.status,
                mt1.score AS live_score,
                mt1.division_name,
                COALESCE(mc.map_count, 0) AS map_count,

                mt1.team_id AS team1_id,
                mt1.team_name AS team1_name,
                mt1.avatar AS team1_avatar,
                mt1.is_benelux AS team1_is_benelux,
                COALESCE(ts1.map_wins,0) AS team1_map_wins,
                COALESCE(ts1.round_wins,0) AS team1_round_wins,

                mt2.team_id AS team2_id,
                mt2.team_name AS team2_name,
                mt2.avatar AS team2_avatar,
                mt2.is_benelux AS team2_is_benelux,
                COALESCE(ts2.map_wins,0) AS team2_map_wins,
                COALESCE(ts2.round_wins,0) AS team2_round_wins

            FROM match_teams mt1
            JOIN match_teams mt2 ON mt1.match_id = mt2.match_id AND mt1.team_rank = 1 AND mt2.team_rank = 2
            LEFT JOIN team_scores ts1 ON mt1.match_id = ts1.match_id AND mt1.team_id = ts1.team_id
            LEFT JOIN team_scores ts2 ON mt2.match_id = ts2.match_id AND mt2.team_id = ts2.team_id
            LEFT JOIN map_counts mc ON mt1.match_id = mc.match_id
            ORDER BY mt1.match_time ASC
        """

        cursor.execute(query, (start_of_day, ))
        rows = cursor.fetchall()

        # Sort by division name
        division_order = {"Advanced": 0, "Main": 1, "Intermediate": 2, "Entry": 3}
        def compute_division_rank(division_name):
            if division_name in division_order:
                return division_order[division_name]
            elif division_name and division_name.lower().startswith("open"):
                match = re.search(r'(\d+)', division_name)
                return 4 + (100 - int(match.group(1))) if match else 999
            return 999
        
        grouped_matches = defaultdict(list)
        for row in rows:
            match = {
                "match_id": row[0],
                "match_time": row[1],
                "status": row[2],
                "live_score": row[3],
                "division_name": row[4],
                "map_count": row[5],
            }
            team1 = {
                "team_id": row[6],
                "team_name": row[7],
                "team_avatar": row[8],
                "is_benelux": row[9],
                "map_wins": row[10],
                "round_wins": row[11]
            }
            
            team2 = {
                "team_id": row[12],
                "team_name": row[13],
                "team_avatar": row[14],
                "is_benelux": row[15],
                "map_wins": row[16],
                "round_wins": row[17]
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
            
            # Add match to the appropriate division group
            grouped_matches[match["division_name"]].append(match)

        # Step 2: Sort the division groups
        sorted_grouped_matches = {
            division: grouped_matches[division]
            for division in sorted(grouped_matches.keys(), key=compute_division_rank)
        }

        print(sorted_grouped_matches)
        return sorted_grouped_matches, end_of_day

    except Exception as e:
        function_logger.error(f"Error fetching today's matches: {e}")
        return {}, 0
    finally:
        close_database(db)
  
def gather_player_stats_esea(
    countries=None,
    seasons=None,
    divisions=None,
    stages=None,
    timestamp_start=0,
    timestamp_end=0,
    team_ids=None,
    search_player_name=""
):
    countries = countries or []
    seasons = seasons or []
    divisions = divisions or []
    stages = stages or []
    team_ids = team_ids or []

    db, cursor = start_database()
    try:
        # Get valid (player_id, team_id, event_id) combos
        cursor.execute("SELECT team_id, event_id, players_main, players_sub FROM teams_benelux")
        valid_combos = set()
        for team_id, event_id, main_json, sub_json in cursor.fetchall():
            def safe_load_json_for_validation(value):
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
            
            main_players = safe_load_json_for_validation(main_json)
            sub_players = safe_load_json_for_validation(sub_json)
            
            for player in main_players + sub_players:
                if isinstance(player, dict):
                    pid = player.get('player_id')
                    if pid:
                        valid_combos.add((pid, team_id, event_id))

        # Get stat columns and build AVG expressions
        non_stat_cols = {'player_id', 'player_name', 'team_id', 'match_id', 'match_round'}
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'players_stats'
        """)
        all_columns = [row[0] for row in cursor.fetchall()]
        stat_columns = [col for col in all_columns if col not in non_stat_cols]
        avg_expressions = [f'AVG(ps."{col}") AS "{col}"' for col in stat_columns]

        # Build WHERE clause and parameters
        conditions = []
        params = []

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

        if timestamp_start > 0:
            conditions.append("m.match_time >= %s")
            params.append(timestamp_start)

        if timestamp_end > 0:
            conditions.append("m.match_time <= %s")
            params.append(timestamp_end)

        if team_ids:
            placeholders = ','.join(['%s'] * len(team_ids))
            conditions.append(f"ps.team_id IN ({placeholders})")
            params.extend(team_ids)

        if search_player_name:
            conditions.append("p.player_name LIKE %s")
            params.append(f"%{search_player_name}%")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # Query player stats
        query = f"""
            SELECT 
                ps.player_id,
                COALESCE(pc.country, p.country) AS country,
                p.avatar,
                COUNT(DISTINCT ps.match_id || '-' || ps.match_round) AS maps_played,
                ROUND(100.0 * SUM(tm.team_win) / COUNT(DISTINCT ps.match_id || '-' || ps.match_round), 1) AS map_win_pct,
                ps.team_id, 
                s.event_id, 
                {', '.join(avg_expressions)}
            FROM players_stats ps
            LEFT JOIN teams_maps tm ON ps.match_id = tm.match_id AND ps.match_round = tm.match_round AND ps.team_id = tm.team_id
            LEFT JOIN matches m ON ps.match_id = m.match_id
            INNER JOIN seasons s ON m.event_id = s.event_id
            LEFT JOIN players p ON ps.player_id = p.player_id
            LEFT JOIN players_country pc ON p.player_id = pc.player_id
            {where_clause}
            GROUP BY 
                ps.player_id,
                COALESCE(pc.country, p.country),
                p.avatar,
                ps.team_id,
                s.event_id
        """
        cursor.execute(query, params)
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        stat_field_names = col_names[7:]

        # Filter rows using whitelist
        filtered_stats = {}
        player_countries = {}
        player_avatars = {}
        player_maps_played = {}
        player_map_win_pct = {}

        for row in rows:
            player_id, country, avatar, maps_played, map_win_pct, team_id, event_id = row[:7]
            if (player_id, team_id, event_id) in valid_combos:
                filtered_stats.setdefault(player_id, []).append(row[7:])
                player_countries.setdefault(player_id, country)
                player_avatars.setdefault(player_id, avatar)
                player_maps_played.setdefault(player_id, maps_played)
                player_map_win_pct.setdefault(player_id, map_win_pct)

        from statistics import mean

        avg_results = {}
        for player_id, stat_lists in filtered_stats.items():
            transposed = zip(*stat_lists)
            avg_results[player_id] = [
                mean(x for x in col if x is not None) if any(x is not None for x in col) else 0
                for col in transposed
            ]

        # Use separate alias gathering function
        official_names, aliases = get_player_aliases(cursor)

        output = []
        for player_id, stat_values in avg_results.items():
            output.append({
                "player_id": player_id,
                "player_name": official_names.get(player_id),
                "aliases": aliases.get(player_id) or [],
                "avatar": player_avatars.get(player_id),
                "country": player_countries.get(player_id),
                "maps_played": player_maps_played.get(player_id, 0),
                "map_win_pct": player_map_win_pct.get(player_id, 0.0),
                "avg_stats": dict(zip(stat_field_names, stat_values))
            })

        return output, stat_field_names

    except Exception as e:
        function_logger.error(f"Error gathering player stats: {e}", exc_info=True)
        return [], []
    finally:
        close_database(db)

# =============================
#       Leaderboard Page
# =============================
def gather_leaderboard(**kwargs) -> list:
    """ Gathers the leaderboard from the database with optional filtering and includes aliases """
    countries = kwargs.get('countries', None)
    search = kwargs.get('search', '').strip()
    min_elo = kwargs.get('min_elo', 0)
    max_elo = kwargs.get('max_elo', None)

    db, cursor = start_database()
    try:
        # Build base query with all filters applied at database level for better performance
        query = """
            SELECT 
                p.player_id, 
                p.player_name,
                COALESCE(pc.country, p.country) AS country,
                p.avatar, 
                p.faceit_elo, 
                p.faceit_level,
                ROW_NUMBER() OVER (ORDER BY p.faceit_elo DESC) as rank
            FROM players p
            LEFT JOIN players_country pc ON p.player_id = pc.player_id
        """

        conditions = []
        params = []

        if countries:
            placeholders = ', '.join(['%s'] * len(countries))
            conditions.append(f"(pc.country IN ({placeholders}) OR (pc.country IS NULL AND p.country IN ({placeholders})))")
            params.extend(countries)
            params.extend(countries)
            # Apply 2000 elo threshold only when countries filter is active
            conditions.append("p.faceit_elo > %s")
            params.append(2000)

        # Apply min/max elo filters at database level
        if min_elo is not None and min_elo > 0:
            conditions.append("p.faceit_elo >= %s")
            params.append(min_elo)

        if max_elo is not None:
            conditions.append("p.faceit_elo <= %s")
            params.append(max_elo)

        # Apply search filter at database level using PostgreSQL's ILIKE for case-insensitive search
        if search:
            conditions.append("p.player_name ILIKE %s")
            params.append(f"%{search}%")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY p.faceit_elo DESC"

        # Execute query directly with cursor for better control
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        columns = ['player_id', 'player_name', 'country', 'avatar', 'faceit_elo', 'faceit_level', 'rank']
        players_list = []
        
        for i, row in enumerate(rows, 1):
            player_dict = dict(zip(columns, row))
            # Override rank with sequential numbering for filtered results
            player_dict['index'] = i
            players_list.append(player_dict)

        # If we have a search term and didn't find exact matches, try fuzzy search
        if search and not players_list:
            # Fallback: get all players and do fuzzy search in Python
            fallback_query = """
                SELECT 
                    p.player_id, 
                    p.player_name,
                    COALESCE(pc.country, p.country) AS country,
                    p.avatar, 
                    p.faceit_elo, 
                    p.faceit_level
                FROM players p
                LEFT JOIN players_country pc ON p.player_id = pc.player_id
            """
            
            fallback_conditions = []
            fallback_params = []
            
            if countries:
                placeholders = ', '.join(['%s'] * len(countries))
                fallback_conditions.append(f"(pc.country IN ({placeholders}) OR (pc.country IS NULL AND p.country IN ({placeholders})))")
                fallback_params.extend(countries)
                fallback_params.extend(countries)
                fallback_conditions.append("p.faceit_elo > %s")
                fallback_params.append(2000)

            if min_elo is not None and min_elo > 0:
                fallback_conditions.append("p.faceit_elo >= %s")
                fallback_params.append(min_elo)

            if max_elo is not None:
                fallback_conditions.append("p.faceit_elo <= %s")
                fallback_params.append(max_elo)

            if fallback_conditions:
                fallback_query += " WHERE " + " AND ".join(fallback_conditions)
            
            fallback_query += " ORDER BY p.faceit_elo DESC"
            
            cursor.execute(fallback_query, fallback_params)
            fallback_rows = cursor.fetchall()
            
            # Apply fuzzy search
            all_names = [row[1] for row in fallback_rows]  # player_name is at index 1
            matched_names = fuzzy_search(search, all_names, limit=len(all_names), threshold=60)
            
            # Filter results
            for i, row in enumerate(fallback_rows, 1):
                if row[1] in matched_names:  # player_name
                    player_dict = dict(zip(columns[:-1], row))  # exclude rank column
                    player_dict['index'] = i
                    players_list.append(player_dict)

        # Fetch aliases only for the players in this leaderboard
        if players_list:
            player_ids = [p['player_id'] for p in players_list]
            _, aliases = get_player_aliases(cursor, player_ids=player_ids)
            # Add aliases to each player
            for player in players_list:
                player['aliases'] = aliases.get(player['player_id'], '')

        return players_list

    except Exception as e:
        function_logger.error(f"Error gathering players: {e}", exc_info=True)
        raise

    finally:
        close_database(db)
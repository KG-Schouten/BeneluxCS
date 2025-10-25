# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from psycopg2 import Error as PostgresError
import pandas as pd
from rapidfuzz import fuzz

from database.db_manage import start_database, close_database

from logs.update_logger import get_logger
function_logger = get_logger("functions")


def gather_event_players(event_ids: list, team_ids: list, PAST: bool = False) -> pd.DataFrame:
    """
    Gathers players for each (event_id, team_id) pair.
    Assumes event_ids and team_ids are equal-length lists, where each index defines a pair.
    """
    if len(event_ids) != len(team_ids):
        raise ValueError("event_ids and team_ids must be the same length.")

    db, cursor = start_database()
    
    try:
        base_query = """
            SELECT
                ps.player_id,
                ps.team_id,
                ps.match_id,
                p.player_name,
                m.event_id,
                e.event_end
            FROM players_stats ps
            LEFT JOIN players p ON ps.player_id = p.player_id
            LEFT JOIN matches m ON ps.match_id = m.match_id
            LEFT JOIN events e ON m.event_id = e.event_id
        """

        filters = []
        params = []

        # Create OR conditions for (event_id, team_id) pairs
        pair_clauses = []
        for ev_id, tm_id in zip(event_ids, team_ids):
            pair_clauses.append("(m.event_id = %s AND ps.team_id = %s)")
            params.extend([ev_id, tm_id])
        filters.append(" OR ".join(pair_clauses))

        if PAST:
            filters.append("e.event_end < extract(epoch from now())")

        if filters:
            base_query += " WHERE " + " AND ".join(f"({f})" for f in filters)

        cursor.execute(base_query, params)
        res = cursor.fetchall()
        columns = ['player_id', 'team_id', 'match_id', 'player_name', 'event_id', 'event_end']
        data = pd.DataFrame(res, columns=columns)

        event_players = []
        for (event_id, team_id), group in data.groupby(['event_id', 'team_id']):
            # Get player match counts, sorted descending
            player_counts = group.groupby('player_id').size().sort_values(ascending=False)

            # Split into top 5 and the rest
            main_ids = player_counts.head(5).index.tolist()

            players_main, players_sub = [], []
            for player_id in player_counts.index:
                player_name = group[group['player_id'] == player_id]['player_name'].iloc[0]
                player_data = {
                    'player_id': player_id,
                    'player_name': player_name
                }
                if player_id in main_ids:
                    players_main.append(player_data)
                else:
                    players_sub.append(player_data)

            event_players.append({
                'event_id': event_id,
                'team_id': team_id,
                'players_main': players_main,
                'players_sub': players_sub
            })

        return pd.DataFrame(event_players)

    except PostgresError as e:
        function_logger.error(f"Error gathering event players: {e}")
        return pd.DataFrame()
    except Exception as e:
        function_logger.error(f"Error gathering event players: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)

def gather_event_teams(event_ids: list = [], ONGOING: bool = False, ESEA: bool = False) -> pd.DataFrame:
    db,cursor = start_database()
    try:
        query_base = """
            SELECT
                tb.team_id,
                tb.event_id,
                e.event_end
            FROM teams_benelux tb
            LEFT JOIN events e ON tb.event_id = e.event_id
        """
        
        if ESEA:
            query_base += " INNER JOIN seasons s ON tb.event_id = s.event_id"
        
        filters = []
        params = []
        
        if event_ids:
            placeholders = ', '.join(['%s'] * len(event_ids))
            filters.append(f"tb.event_id IN ({placeholders})")
            params.extend(event_ids)
        
        if ONGOING:
            filters.append("e.event_end > EXTRACT(EPOCH FROM NOW())")
        
        if filters:
            query_base += " WHERE " + " AND ".join(filters)
        
        cursor.execute(query_base, params)
        res = cursor.fetchall()
        
        df_event_teams = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        # Remove rows with NaN values in 'team_id' or 'event_id'
        df_event_teams = df_event_teams.dropna(subset=['team_id', 'event_id'])
        
        return df_event_teams
        
    except Exception as e:
        function_logger.error(f"Error gathering event teams: {e}")
        return pd.DataFrame()

def gather_event_matches(event_ids: list) -> list:
    """ Gathers all matches for each (event_id, team_id) pair"""
    db, cursor = start_database()
    try:
        query_base = """
            SELECT
                match_id
            FROM matches
            WHERE event_id IN ({})
        """
        placeholders = ', '.join(['%s'] * len(event_ids))
        query_base = query_base.format(placeholders)
        cursor.execute(query_base, event_ids)
        res = cursor.fetchall()
        match_ids = [match[0] for match in res]
        
        return match_ids
    except Exception as e:
        function_logger.error(f"Error gathering event matches: {e}")
        return []
    finally:
        close_database(db)

def gather_internal_event_ids(event_ids: list) -> pd.DataFrame:
    db, cursor = start_database()
    try:
        query_base = """
            SELECT
                event_id,
                internal_event_id
            FROM events
            WHERE event_id IN ({})
        """
        placeholders = ', '.join(['%s'] * len(event_ids))
        query_base = query_base.format(placeholders)
        cursor.execute(query_base, event_ids)
        res = cursor.fetchall()
        df_events = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        return df_events
    except Exception as e:
        function_logger.error(f"Error gathering internal event IDs: {e}")
        return pd.DataFrame()

def gather_upcoming_matches() -> pd.DataFrame:
    db, cursor = start_database()
    try:
        query = """
            SELECT
                m.match_id,
                m.event_id,
                m.match_time,
                m.status
            FROM matches m
            WHERE m.status != 'FINISHED'
        """
        cursor.execute(query)
        res = cursor.fetchall()
        df_upcoming = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
    except Exception as e:
        function_logger.error(f"Error gathering upcoming matches: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)
    
    return df_upcoming  

def gather_elo_snapshot() -> pd.DataFrame:
    db, cursor = start_database()
    try:
        query = """
            SELECT
                player_id,
                faceit_elo
            FROM players
        """
        cursor.execute(query)
        res = cursor.fetchall()
        df_elo = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        return df_elo
    except Exception as e:
        function_logger.error(f"Error gathering elo snapshot: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)

def gather_league_teams() -> pd.DataFrame:
    db, cursor = start_database()
    try:
        query = """
            SELECT
                *
            FROM league_teams lt
        """
        cursor.execute(query)
        res = cursor.fetchall()
        df_league_teams = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        
        return df_league_teams
    except Exception as e:
        function_logger.error(f"Error gathering league teams: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)

def gather_league_teams_merged():
    db, cursor = start_database()
    try:
        query = """
            SELECT
                tm.team_id,
                tm.match_id,
                tm.team_name AS team_name_match,
                
                m.event_id,
                m.match_time,
                
                lt.team_name AS league_teams_name,
                
                s.season_number,
                s.division_name
                
            FROM teams_matches tm
            LEFT JOIN matches m ON tm.match_id = m.match_id
            INNER JOIN seasons s ON m.event_id = s.event_id
            INNER JOIN league_teams lt ON tm.team_id = lt.team_id AND s.season_number = lt.season_number
        """
        cursor.execute(query)
        res = cursor.fetchall()
        df = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        
        def determine_equal_name(name1: str, name2: str):
            # Normalize the names a bit more
            name1, name2 = name1.lower().replace('-', ' ').replace('_', ' '), name2.lower().replace('-', ' ').replace('_', ' ')
            
            fuzzRatio = fuzz.ratio(name1, name2)
            fuzzRatioPartial = fuzz.partial_ratio(name1, name2)
            
            return fuzzRatio, fuzzRatioPartial
        
        league_teams = []
        team_names_updated = []
        for team_id, team_group in df.groupby('team_id'):
            team_group = team_group.sort_values(by='season_number', ascending=False).reset_index(drop=True)
            for season_number, season_group in team_group.groupby('season_number'):
                season_group = season_group.sort_values(by='match_time', ascending=False).reset_index(drop=True)
                
                last_name = season_group.at[0, 'team_name_match']
                league_teams_name = season_group.at[0, 'league_teams_name']
                
                if pd.isna(league_teams_name) or pd.isna(last_name):
                    continue
                if isinstance(league_teams_name, str) and isinstance(last_name, str):
                    fuzzRatio, fuzzRatioPartial = determine_equal_name(last_name, league_teams_name)
                    
                    if fuzzRatio < 50:
                        team_names_updated.append([team_id, season_number, last_name, league_teams_name, fuzzRatio, fuzzRatioPartial])
                        team_name = last_name
                    else:
                        team_name = league_teams_name
                    
                    league_teams.append(
                        {
                            'team_id': team_id,
                            'season_number': season_number,
                            'team_name': team_name,
                            'division_name': season_group.at[0, 'division_name']
                        }
                    )
        
        df_league_teams = pd.DataFrame(league_teams)
        
        # Gather avatars for the teams
        pk_values = [(lt['team_id'], lt['season_number']) for lt in league_teams]
        
        query = """
            SELECT
                lt.team_id,
                lt.season_number,
                lt.avatar
            FROM league_teams lt
            WHERE (lt.team_id, lt.season_number) IN ({})
        """
        
        placeholders = ', '.join(['(%s, %s)'] * len(pk_values))
        query = query.format(placeholders)
        flat_params = [item for sublist in pk_values for item in sublist]
        cursor.execute(query, flat_params)
        res = cursor.fetchall()
        df_avatars = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        
        if not df_avatars.empty and not df_league_teams.empty:
            df_league_teams = df_league_teams.merge(df_avatars, on=['team_id', 'season_number'], how='left')
        
        return df_league_teams, team_names_updated
    except Exception as e:
        function_logger.error(f"Error gathering league teams merged: {e}")
        return pd.DataFrame(), []
    finally:
        close_database(db)
        
def gather_league_team_avatars():
    db, cursor = start_database()
    try:
        query = """
            SELECT
                lt.team_id,
                lt.season_number,
                lt.team_name,
                lt.division_name,
                MIN(t.avatar) AS avatar
            FROM league_teams lt
            LEFT JOIN teams t ON lt.team_id = t.team_id
            LEFT JOIN seasons s ON lt.season_number = s.season_number
            LEFT JOIN events e ON s.event_id = e.event_id
            WHERE e.event_end - 2629743 > EXTRACT(EPOCH FROM NOW()) -- Month before end of season
            GROUP BY lt.team_id, lt.season_number;
        """
        cursor.execute(query)
        res = cursor.fetchall()
        df_avatars = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        
        
        return df_avatars
    except Exception as e:
        function_logger.error(f"Error gathering league team avatars: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)
        
def gather_ongoing_matches() -> pd.DataFrame:
    db, cursor = start_database()
    try:
        query = """
            SELECT
                m.match_id,
                m.event_id,
                m.match_time,
                m.status
            FROM matches m
            WHERE m.status NOT IN ('FINISHED', 'CANCELLED', 'SCHEDULED')
        """
        cursor.execute(query)
        res = cursor.fetchall()
        df_ongoing = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
    except Exception as e:
        function_logger.error(f"Error gathering ongoing matches: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)
    
    return df_ongoing

def gather_teams_benelux_primary():
    db, cursor = start_database()
    try:
        query = "SELECT team_id, event_id, team_name FROM teams_benelux;"
        cursor.execute(query)
        res = cursor.fetchall()
        df_teams_benelux = pd.DataFrame(res, columns=['team_id', 'event_id', 'team_name'])
        return df_teams_benelux
    except Exception as e:
        function_logger.error(f"Error gathering teams benelux primary: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)
        
# =============================
#       Streamer updates
# =============================
def gather_streamers(streamer_ids: list = [], streamer_names: list = [], platforms: list = []) -> pd.DataFrame:
    from database.db_manage import start_database, close_database
    
    db, cursor = start_database()
    try:
        conditions = []
        params = []
        
        if streamer_ids:
            placeholders = ', '.join(['%s'] * len(streamer_ids))
            conditions.append(f"user_id IN ({placeholders})")
            params.extend(streamer_ids)
        if streamer_names:
            placeholders = ', '.join(['%s'] * len(streamer_names))
            conditions.append(f"LOWER(user_login) IN ({placeholders})")
            params.extend([name.lower() for name in streamer_names])
        if platforms:
            placeholders = ', '.join(['%s'] * len(platforms))
            conditions.append(f"platform IN ({placeholders})")
            params.extend(platforms)
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        query = f"""
            SELECT *
            FROM streams
            {where_clause}
        """
        cursor.execute(query, params)
        res = cursor.fetchall()
        
        df_streamers = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        
        return df_streamers
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()
    
    finally:
        close_database(db)

def link_twitch_streamer_to_faceit(streamer_name: str):
    from database.db_manage import start_database, close_database
    
    db, cursor = start_database()
    try:
        query = """
            SELECT p.player_id
            FROM players p
            WHERE LOWER(p.player_name) = LOWER(%s)
        """
        cursor.execute(query, (streamer_name,))
        res = cursor.fetchone()
        
        if res:
            return res[0]
        else:
            return None
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    finally:
        close_database(db)

def gather_live_streams() -> list:
    from database.db_manage import start_database, close_database
    
    db, cursor = start_database()
    try:
        query = "SELECT user_id FROM streams WHERE live = TRUE AND platform = 'twitch' AND game = 'Counter-Strike';"
        cursor.execute(query)
        result = cursor.fetchall()
        live_streamer_ids = [row[0] for row in result]
        return live_streamer_ids
    except Exception as e:
        print(f"An error occurred while gathering live streams: {e}")
        return []
    finally:
        close_database(db)
# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from psycopg2 import Error as PostgresError
import pandas as pd

from database.db_manage import start_database, close_database
from data_processing.faceit_api.logging_config import function_logger


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
    
def gather_last_match_time_database(event_ids: list = [], ONGOING: bool = False, ESEA: bool = False) -> int:
    db, cursor = start_database()
    try:
        query_base = """
            SELECT
                m.match_time
            FROM matches m
            LEFT JOIN events e ON m.event_id = e.event_id
        """
        
        if ESEA:
            query_base += " INNER JOIN seasons s ON m.event_id = s.event_id"
        
        query_base += " WHERE m.match_time IS NOT NULL AND m.status = 'FINISHED'  "
        
        filters = []
        params = []
        
        if event_ids:
            placeholders = ', '.join(['%s'] * len(event_ids))
            filters.append(f"m.event_id IN ({placeholders})")
            params.extend(event_ids)
        if ONGOING:
            filters.append("e.event_end > EXTRACT(EPOCH FROM NOW())")
        
        if filters:
            query_base += " AND " + " AND ".join(filters)
        
        query_base += " ORDER BY m.match_time DESC LIMIT 1"
        cursor.execute(query_base, params)
        res = cursor.fetchone()
        
        last_match_time = res[0] if res else 0
        if last_match_time is not None:
            return int(last_match_time)
        else:
            function_logger.warning("No last match time found.")
            return 0
            
    except Exception as e:
        function_logger.error(f"Error gathering last match time: {e}")
        return 0

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
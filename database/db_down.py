# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from psycopg2 import Error as PostgresError
import pandas as pd
import re
from rapidfuzz import process, fuzz
from collections import defaultdict

from database.db_manage import start_database, close_database
from data_processing.faceit_api.logging_config import function_logger

### ----------------------------
### GENERAL FUNCTIONS
### ----------------------------

def get_player_aliases(cursor, player_ids=None):
    """
    Returns (official_names, aliases) for players.
    If player_ids is provided, filters aliases only for those players.
    Otherwise returns for all players.
    """
    if player_ids:
        placeholders = ', '.join(['%s'] * len(player_ids))
        query = f"""
            SELECT 
                ps.player_id,
                ps.player_name AS player_name_match,
                p.player_name AS official_player_name,
                COUNT(*) AS usage_count
            FROM players_stats ps
            LEFT JOIN players p ON ps.player_id = p.player_id
            WHERE ps.player_id IN ({placeholders})
            GROUP BY ps.player_id, ps.player_name, p.player_name
        """
        cursor.execute(query, player_ids)
    else:
        query = """
            SELECT 
                ps.player_id,
                ps.player_name AS player_name_match,
                p.player_name AS official_player_name,
                COUNT(*) AS usage_count
            FROM players_stats ps
            LEFT JOIN players p ON ps.player_id = p.player_id
            GROUP BY ps.player_id, ps.player_name, p.player_name
        """
        cursor.execute(query)

    rows = cursor.fetchall()

    usage_map = defaultdict(list)
    official_names = {}

    for player_id, name_variant, official_name, usage_count in rows:
        usage_map[player_id].append((name_variant, usage_count))
        official_names[player_id] = official_name

    most_used_name_map = {}
    for player_id, variants in usage_map.items():
        variants_sorted = sorted(variants, key=lambda x: x[1], reverse=True)
        most_used_name_map[player_id] = variants_sorted[0][0] if variants_sorted else None

    aliases = {}
    for player_id, most_used_name in most_used_name_map.items():
        official_name = official_names.get(player_id)
        if most_used_name and official_name and not are_names_similar(most_used_name, official_name):
            aliases[player_id] = most_used_name
        else:
            aliases[player_id] = ''

    return official_names, aliases

def normalize_name(name):
    if not name:
        return ""
    name = name.lower()
    subs = {'1': 'i', '0': 'o', '3': 'e', '4': 'a', '5': 's', '7': 't'}
    for digit, letter in subs.items():
        name = name.replace(digit, letter)
    name = re.sub(r'[^a-z0-9]', '', name)
    return name

def are_names_similar(name1: str, name2: str, threshold: int = 75) -> bool:
    n1, n2 = normalize_name(name1), normalize_name(name2)
    return fuzz.ratio(n1, n2) >= threshold

def fuzzy_search(query: str, choices: list, scorer=fuzz.WRatio, limit=5, threshold=60) -> list:
    """
    Perform fuzzy search on a list of strings.

    Args:
        query (str): The search query.
        choices (list): List of strings to search.
        scorer (callable): Scoring function from rapidfuzz (default: WRatio).
        limit (int): Max number of results to return (default: 5).
        threshold (int): Minimum score threshold to include a result (default: 60).

    Returns:
        List of matched strings (only those scoring >= threshold), sorted by score descending.
    """
    results = process.extract(query, choices, scorer=scorer, limit=limit)
    filtered = [match for match, score, _ in results if score >= threshold]
    return filtered

### ----------------------------
### DATABASE DOWN FUNCTIONS
### ----------------------------

def gather_players_country() -> pd.DataFrame:
    """
    Gathers the players country from the database

    Returns:
        df
    """
    db, cursor = start_database()

    query = """
        SELECT *      
        FROM players_country p
    """

    try:
        cursor.execute(query)
        res = cursor.fetchall()
        columns = ['player_id','player_name', 'country']
        df = pd.DataFrame(res, columns=columns)

        return df

    except PostgresError as e:
        print(f"Error gathering players country: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error gathering players country: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)

def gather_players(**kwargs) -> pd.DataFrame:
    """ 
    Gathers players from the database with optional filtering
    
    Args:
        **kwargs:
            benelux (bool): If True, filters players from the Benelux region.
            name (str): Optional name filter to search for players by player_name.
            
    Returns:
        pd.DataFrame: DataFrame containing player information including player_id, player_name, country, avatar, faceit_elo, and faceit_level.
    """
    benelux = kwargs.get('benelux', False)
    name = kwargs.get('name', None)

    if name:
        function_logger.info(f"Gathering players with name filter: {name}")
    if benelux:
        function_logger.info("Gathering players from the Benelux region")

    db, cursor = start_database()
    try:
        query_base = """
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

        if benelux:
            query_base += " WHERE pc.country IN ('nl', 'be', 'lu')"

        cursor.execute(query_base)
        res = cursor.fetchall()
        columns = ['player_id', 'player_name', 'country', 'avatar', 'faceit_elo', 'faceit_level']
        df_players = pd.DataFrame(res, columns=columns)

        return df_players

    except PostgresError as e:
        function_logger.error(f"Error gathering players: {e}")
        return pd.DataFrame()
    except Exception as e:
        function_logger.error(f"Error gathering players: {e}")
        return pd.DataFrame()
    finally:   
        close_database(db)

def gather_league_teams(
    team_id: str | list = "ALL",
    season_number: str | int | list = "ALL"
    ) -> pd.DataFrame:
    
    """
    Gathers league teams from the database

    Returns:
        df
    """
    db, cursor = start_database()

    conditions = []
    params = []
    
    try:
        if team_id != "ALL":
            if isinstance(team_id, str):
                team_id = [team_id]
            placeholders = ', '.join(['%s'] * len(team_id))
            conditions.append(f"lt.team_id IN ({placeholders})")
            params.extend(team_id)
        if season_number != "ALL":
            if isinstance(season_number, (str, int)):
                season_number = [season_number]
            
            # Convert to digits only for safety
            season_number = [int(sn) for sn in season_number if str(sn).isdigit()]
            
            placeholders = ', '.join(['%s'] * len(season_number))
            conditions.append(f"lt.season_number IN ({placeholders})")
            params.extend(season_number)
    except Exception as e:
        print("Error processing gather_league_teams parameters:", e)
    
    query = f"""
        SELECT *      
        FROM league_teams lt
        {'WHERE ' + ' AND '.join(conditions) if conditions else ''}
    """

    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        
        return df

    except PostgresError as e:
        print(f"Error gathering league teams: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error gathering league teams: {e}")
        return pd.DataFrame()
    finally:
        close_database(db)

if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
from datetime import datetime
import pandas as pd

from database.db_up import *
from database.db_manage import start_database, close_database
        
def gather_upcoming_matches_esea() -> pd.DataFrame:
    """
    Gathers the upcoming matches in ESEA from the database
    """
    query = """
        SELECT 
            m.match_id, 
            m.event_id, 
            m.match_time, 
            m.status,
            s.season_number,
            s.division_name,
            s.stage_name,
            tm.team_id, 
            tm.team_name, 
            tb.team_name AS team_name_benelux
        FROM matches m
        INNER JOIN seasons s
            ON m.event_id = s.event_id
        LEFT JOIN teams_matches tm
            ON m.match_id = tm.match_id
        LEFT JOIN teams_benelux tb
            ON tm.team_id = tb.team_id
            AND m.event_id = tb.event_id
        WHERE m.status = 'SCHEDULED'
        ORDER BY m.event_id, m.match_time ASC
    """
    
    try:
        # Start the database and cursor
        db, cursor = start_database()

        # Get player data into dataframe
        cursor.execute(query)
        res = cursor.fetchall()
        df_results = pd.DataFrame(res, columns=[desc[0] for desc in cursor.description])
        
        ## Processing of data into new dataframe
        df_upcoming = (
            df_results
            .sort_values(by=['team_name_benelux'], ascending=True)
            .groupby('match_id', group_keys=False)
            .apply(
                lambda match: pd.Series({
                    'match_id': match.name,
                    'event_id': match['event_id'].iloc[0],
                    'match_time': safe_convert_to_datetime(match['match_time'].iloc[0]),
                    'season_number': match['season_number'].iloc[0],
                    'division_name': match['division_name'].iloc[0],
                    'stage_name': match['stage_name'].iloc[0],
                    'team_ids': match['team_id'].to_list(),
                    'team_names': match['team_name'].to_list(),
                    'is_benelux': match['team_name_benelux'].notna().to_list()
                }),
                include_groups=False 
            )
            .reset_index(drop=True)
        )
        return df_upcoming
        
    except psycopg2.Error as e:
        print(f"Error gathering upcoming matches: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error gathering upcoming matches: {e}")
        return pd.DataFrame()
    finally:
        # Close the database connection
        close_database(db, cursor)

def safe_convert_to_datetime(timestamp):
    try:
        return datetime.datetime.fromtimestamp(float(timestamp))
    except (ValueError, TypeError, OverflowError):
        return pd.NaT

def gather_stats(**kwargs) -> pd.DataFrame:
    return

def gather_players_country() -> pd.DataFrame:
    """
    Gathers the players country from the database

    Returns:
        df
    """
    
    ## Start the database and cursor
    db, cursor = start_database()

    query = f"""
        SELECT *      
        FROM players_country p
    """

    # Get player data into dataframe
    cursor.execute(query)
    res = cursor.fetchall()
    columns = ['player_id','player_name', 'country']
    df = pd.DataFrame(res, columns=columns)

    return df


if __name__ == "__main__":
    # Allow standalone execution
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
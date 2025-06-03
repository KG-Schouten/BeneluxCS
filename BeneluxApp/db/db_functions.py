import sqlite3
import pandas as pd

from database.db_manage import start_database, close_database

def fetch_filtered_data(filters):
    # Build query dynamically based on filters dict
    base_query = """
    SELECT
        ps.player_id,
        (SELECT player_name FROM players_stats ps2 WHERE ps2.player_id = ps.player_id ORDER BY ps2.match_id DESC LIMIT 1) AS player_name,
        COUNT(DISTINCT ps.match_id) AS matches_played,
        ROUND(AVG(ps.k_d_ratio), 2) AS k_d_ratio,
        ROUND(AVG(ps.hltv), 2) AS hltv
    FROM players_stats ps
    LEFT JOIN matches m ON ps.match_id = m.match_id
    LEFT JOIN events e ON m.internal_event_id = e.internal_event_id
    LEFT JOIN maps ma ON ps.match_id = ma.match_id AND ps.match_round = ma.match_round
    LEFT JOIN teams_matches tm ON ps.match_id = tm.match_id AND ps.team_id = tm.team_id
    WHERE 1=1
    """

    params = []
    if filters.get("event_type"):
        placeholders = ",".join("?" for _ in filters["event_type"])
        base_query += f" AND e.event_type IN ({placeholders})"
        params.extend(filters["event_type"])
    if filters.get("map"):
        placeholders = ",".join("?" for _ in filters["map"])
        base_query += f" AND ma.map IN ({placeholders})"
        params.extend(filters["map"])
    if filters.get("best_of"):
        placeholders = ",".join("?" for _ in filters["best_of"])
        base_query += f" AND m.best_of IN ({placeholders})"
        params.extend(filters["best_of"])
    
    # Time filter
    if filters.get("match_time"):
        filter_map = {
            "All time": None,
            "Last Month": "strftime('%s', 'now', '-1 month')",
            "Last 3 Months": "strftime('%s', 'now', '-3 months')",
            "Last 6 Months": "strftime('%s', 'now', '-6 months')",
            "Last Year": "strftime('%s', 'now', '-1 year')"
        }
        match_filter = filters["match_time"]
        if match_filter in filter_map:
            if filter_map[match_filter]:
                base_query += f" AND m.match_time >= {filter_map[match_filter]}"

    # Input filters (case-insensitive)
    if filters.get("event_name"):
        words = filters["event_name"].strip().lower().split()
        for w in words:
            base_query += " AND LOWER(e.event_name) LIKE ?"
            params.append(f"%{w}%")
    if filters.get("team_name"):
        base_query += " AND LOWER(tm.team_name) LIKE LOWER(?)"
        params.append(f"%{filters['team_name']}%")
    if filters.get("player_name"):
        base_query += " AND LOWER(ps.player_name) LIKE LOWER(?)"
        params.append(f"%{filters['player_name']}%")
    
    # Slider filter for matches played
    base_query += " GROUP BY ps.player_id HAVING COUNT(DISTINCT ps.match_id) >= ? ORDER BY matches_played DESC LIMIT 200"
    params.append(filters.get("matches_played", 0))  # Use 0 as default if no filter

    db, cursor = start_database()
    df = pd.read_sql_query(base_query, db, params=params)
    close_database(db, cursor)
    return df
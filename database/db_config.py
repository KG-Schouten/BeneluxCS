db_name = "BeneluxCS"

table_names = {
    "seasons": [
        ['event_id'],
    ],
    "teams_benelux": [
        ['team_id', 'event_id'],
        [('event_id', 'seasons')]
    ],
    "events": [
        ['internal_event_id']
    ],
    "matches": [
        ['match_id'],
        [('internal_event_id', 'events')]
    ],
    "maps": [
        ['match_id', 'match_round'],
        [('match_id', 'matches')]
    ],
    "players": [
        ['player_id']
    ],
    "teams": [
        ['team_id']
    ],
    "players_stats": [
        ['player_id', 'match_id', 'match_round'],
        [('player_id', 'players'), ('team_id', 'teams'), (('match_id', 'match_round'), 'maps')]
    ],
    "teams_matches": [
        ['match_id', 'team_id'],
        [('team_id', 'teams'), ('match_id', 'matches')]
    ],
    "teams_maps": [
        ['team_id', 'match_id', 'match_round'],
        [(('match_id', 'match_round'), 'maps'), ('team_id', 'teams')]
    ]
}

VALID_SQLITE_TYPES = {
    "INTEGER", "REAL", "TEXT", "NUMERIC", "BLOB"
}
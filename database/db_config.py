db_name = "BeneluxCS"

# Dictionaries for the table names and their useful values connected to it
table_names_esea = { # 'table_name' : create_table_query args (primary_keys, foreign_keys)
    "esea_teams": [
        ['team_id']
    ], 
    "esea_seasons": [
        ['season_id']
    ], 
    "esea_stages": [
        ['stage_id'], 
        [('season_id', 'esea_seasons')]
    ], 
    "esea_players": [
        ['player_id']
    ], 
    "esea_player_team_seasons": [
        ['player_id', 'team_id', 'season_id'], 
        [('player_id', 'esea_players'), ('team_id', 'esea_teams'), ('season_id', 'esea_seasons')]
    ], 
    "esea_matches": [
        ['match_id']
    ],
    "esea_maps": [
        ['match_id', 'match_round'], 
        [('match_id', 'esea_matches')]
    ],
    "esea_team_maps": [
        ['team_id', 'match_id', 'match_round'], 
        [('team_id', 'esea_teams'), (('match_id', 'match_round'), 'esea_maps')]
    ],
    "esea_player_stats": [
        ['player_id', 'match_id', 'match_round'], 
        [('player_id', 'esea_players'), ('team_id', 'esea_teams'), (('match_id', 'match_round'), 'esea_maps')]
    ]
}

table_names_hub = {
    "hub_matches": [
        ["match_id"]
    ],
    "hub_maps": [
        ['match_id', 'match_round'],
        [('match_id', 'hub_matches')]
    ],
    "hub_team_maps": [
        ['team_id', 'match_id', 'match_round'],
        [(('match_id', 'match_round'), 'hub_maps')]
    ],
    "hub_players": [
        ['player_id']
    ], 
    "hub_player_stats": [
        ["player_id", "match_id", "match_round"], 
        [('player_id', 'hub_players'), (('match_id', 'match_round'), 'hub_maps')]
    ]
}
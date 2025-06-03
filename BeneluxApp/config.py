# Dark mode configuration for the BeneluxApp
DARK_BG = "#2e2e2e"
DARK_FG = "#f0f0f0"
DARK_ACCENT = "#5a5a5a"
DARK_HOVER_BG = "#3a3a3a"

# Configuration for other stuff
FILTERMAXWIDTH = 200  # Maximum width for filter options

# Configuration for filters in the BeneluxApp
FILTERS = [
    {
        "label": "Event Type",
        "column": "event_type",
        "options": ['hub', 'esea', 'championship_hub', 'championship'], 
        "type": "checkbox",  # Specify type for UI rendering
    },
    {
        "label": "Maps",
        "column": "map",
        "options": ['de_dust2', 'de_inferno', 'de_mirage', 'de_nuke', 'de_overpass', 'de_vertigo', 'de_ancient', 'de_anubis', 'de_cache', 'de_train'], 
        "type": "checkbox",  # Specify type for UI rendering
    },
    {
        "label": "Best Of",
        "column": "best_of",
        "options": [1, 3, 5],
        "type": "checkbox",  # Specify type for UI rendering
    },
    {
        "label": "Time",
        "column": "match_time",
        "options": ['All time', 'Last Month', "Last 3 Months", 'Last 6 Months', 'Last Year'],
        "type": "radio",  # Specify type for UI rendering
    },
    {
        "label": "Event Name",
        "column": "event_name",
        "options": [],  # This can be populated dynamically from the database
        "type": "input",
    },
    {
        "label": "Teams",
        "column": "team_name",
        "options": [],  # This can be populated dynamically from the database
        "type": "input",
    },
    {
        "label": "Players",
        "column": "player_name",
        "options": [],  # This can be populated dynamically from the database
        "type": "input",
    },
    {
        "label": "Min. Matches Played",
        "column": "matches_played",
        "options": [],  # This can be populated dynamically from the database
        "type": "slider",
        "min": 0,
        "max": 500
    },
]
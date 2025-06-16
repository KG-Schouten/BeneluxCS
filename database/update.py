## This file will be run regularly to update the database with the new data

# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
from database.db_up import update_data, update_leaderboard

# --- Update championship data ---
try:
    championship_ids = {} # Format is {event_id: event_type}
    if championship_ids:
        for event_id, event_type in championship_ids.items():
            update_data("all", event_type, event_id=event_id)
except Exception as e:
    print(f"Error updating championship data: {e}")

# --- Update the hub data ---
try:
    hub_id = "801f7e0c-1064-4dd1-a960-b2f54f8b5193"
    update_data("new", "hub", event_id=hub_id)
except Exception as e:
    print(f"Error updating hub data: {e}")

# --- Update the ESEA data ---
try:
    update_data("new", "esea")
except Exception as e:
    print(f"Error updating ESEA data: {e}")

# --- Update the Leaderboard data ---
try:   
    asyncio.run(update_leaderboard())
except Exception as e:
    print(f"Error updating leaderboard data: {e}")
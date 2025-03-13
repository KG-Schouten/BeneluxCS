import json
from rapidfuzz import fuzz, process
from datetime import *
import sys  # Allows safe exit
import mysql.connector

# Path to the API keys file
API_KEYS_FILE = "tokens/api_keys.json"


## Find a team based on the fuzzy search of the user input
def find_team_name(user_input, team_data):
    """
    Finds the team name that best matches the user input using fuzzy search.
    """
    team_names = list(team_data.keys())
    best_match = process.extractOne(user_input, team_names, scorer=fuzz.WRatio)

    if best_match and best_match[1] > 70: # Confidence threshold
        return best_match[0] # Return the best match
    return None

def load_api_keys():
    """
    Loads API keys from a JSON file and ensures they are not missing.
    """
    try:
        with open(API_KEYS_FILE, 'r') as file:
            api_keys = json.load(file) # Load the API keys from the file
    except FileNotFoundError:
        print("Error: API keys file not found! Please create 'tokens/api_keys.json'.")
        sys.exit(1) # Exit if file is missing

    # Validate that required keys exist and are not empty
    required_keys = ["FACEIT_TOKEN", "DISCORD_TOKEN", "MYSQL_PASSWORD"]
    missing_keys = [key for key in required_keys if not api_keys.get(key)]

    if missing_keys:
        print(f"❌ Error: Missing API keys: {', '.join(missing_keys)}")
        print("➡️  Please update 'tokens/api_keys.json' with valid keys.")
        sys.exit(1)  # Exit program safely

    return api_keys  # Return the valid keys dictionary
    

## Only run if this script is run directly
if __name__ == "__main__":
    with open("C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json", 'r') as file:
        team_data = json.load(file)

    user_input = "Souls Heart"
    print(find_team_name(user_input, team_data))

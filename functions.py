import json
from rapidfuzz import fuzz, process
from datetime import *

## Find a team based on the fuzzy search of the user input
def find_team_name(user_input, team_data):
    
    team_names = list(team_data.keys())
    best_match = process.extractOne(user_input, team_names, scorer=fuzz.WRatio)

    if best_match and best_match[1] > 70: # Confidence threshold
        return best_match[0] # Return the best match
    return None


## Only run if this script is run directly
if __name__ == "__main__":
    with open("C:/Python codes/BeneluxCS/DiscordBot/data/team_data.json", 'r') as file:
        team_data = json.load(file)

    user_input = "Souls Heart"
    print(find_team_name(user_input, team_data))

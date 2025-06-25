# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rapidfuzz import fuzz, process
import sys  # Allows safe exit

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


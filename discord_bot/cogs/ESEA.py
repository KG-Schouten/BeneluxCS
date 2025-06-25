# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nextcord.ext import commands

class ESEA(commands.Cog):
    def __init__(self, client):
        self.client = client

    # --- Command to get the team data based on the team name ---
    
    # --- Command to get the upcoming matches of a team ---
    
    # --- Command to get the matches of today ---
    
    # --- Automatic matchday post ---
    

def setup(client : commands.Bot):
    client.add_cog(ESEA(client))
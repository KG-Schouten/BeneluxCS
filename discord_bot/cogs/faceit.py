# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nextcord.ext import commands

class faceit(commands.Cog):
    def __init__(self, client):
        self.client = client

    # --- Command to get player details from username ---
    
    # --- Command to get general player stats for esea/championships/hub matches ---
    
    # --- Command to get a the faceit elo ranking ---

    
    
def setup(client : commands.Bot):
    client.add_cog(faceit(client))
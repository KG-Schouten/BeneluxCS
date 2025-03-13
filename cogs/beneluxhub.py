import nextcord
from nextcord.ext import commands, tasks
import json
from datetime import datetime

from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1

from data_processing import store_team_data, read_team_data, team_data_path
from functions import find_team_name, load_api_keys

# Initialize the API keys
api_keys = load_api_keys()

class BeneluxHub(commands.Cog):
    def __init__(self, client):
        self.client = client

def setup(client : commands.Bot):
    client.add_cog(BeneluxHub(client))
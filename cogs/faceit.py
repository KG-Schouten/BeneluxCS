import nextcord
from nextcord.ext import commands, tasks
import json
from datetime import datetime, timedelta
import asyncio

# Module imports
from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1

from database.db_general import *
from database.db_down import *
from database.db_up import *

from data_processing import store_team_data, read_team_data, team_data_path
from functions import find_team_name, load_api_keys

# Initialize the API keys
api_keys = load_api_keys()

class faceit(commands.Cog):
    def __init__(self, client):
        self.client = client

    # Fetching data from FACEIT API
    @commands.command()
    async def faceit(self, ctx, username):
        faceit_data = FaceitData(api_keys.get("FACEIT_TOKEN"))

        player_details = faceit_data.player_details(username)

        player_id = player_details["player_id"]

        await ctx.send(player_id)


def setup(client : commands.Bot):
    client.add_cog(faceit(client))
import nextcord
from nextcord.ext import commands, tasks
import json
from datetime import datetime, timedelta
import asyncio

# Module imports
from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1

from database.db_general import *
from database.db_down import gather_hub_stats
from database.db_up import *

from data_processing import store_team_data, read_team_data, team_data_path
from functions import find_team_name, load_api_keys

# Initialize the API keys
api_keys = load_api_keys()

def format_leaderboard(df, title):
    i = 0
    leaderboard_str = f"**{title}**\n"
    for i, row in df.iterrows():
        leaderboard_str += f"{i+1}. {row.iloc[0]} - {row.iloc[1]}\n"
    return leaderboard_str

class BeneluxHub(commands.Cog):
    def __init__(self, client):
        self.client = client

    @commands.command()
    async def monthly(self, ctx):
        """
        Sends an embed with the hub stats of this month (30 days)
        """

        # Getting the dataframe for the stats
        df = gather_hub_stats()
        df['knife_kills'] = pd.to_numeric(df['knife_kills'], errors='coerce')

        # Getting the top 5's for certain columns
        matches_played = df.nlargest(n=3, columns='matches_played')[['nickname', 'matches_played']].reset_index(drop=True)
        winrate = df.nlargest(n=3, columns='wins')[['nickname', 'wins']].reset_index(drop=True)
        hltv = df.nlargest(n=3, columns='hltv')[['nickname', 'hltv']].reset_index(drop=True)
        knifes = df.nlargest(n=3, columns='knife_kills')[['nickname', 'knife_kills']].reset_index(drop=True)

        # embed creating
        embed = nextcord.Embed(
            title="BeneluxHub Monthly Stats",
            colour=nextcord.Colour.blue()
        )

        embed.set_author(name="Benelux Bot",
                         icon_url="https://pbs.twimg.com/profile_images/1796692667008507904/sU3cv9vT_400x400.jpg")
        
        embed.add_field(name="", value=format_leaderboard(matches_played, "Matches Played"), inline=False)
        embed.add_field(name="", value=format_leaderboard(winrate, "Winrate"), inline=False)
        embed.add_field(name="", value=format_leaderboard(hltv, "HLTV Rating"), inline=False)
        embed.add_field(name="", value=format_leaderboard(knifes, "Knife Kills"), inline=False)

        await ctx.send(embed=embed)


def setup(client : commands.Bot):
    client.add_cog(BeneluxHub(client))
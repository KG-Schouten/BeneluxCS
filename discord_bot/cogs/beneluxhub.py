import nextcord
from nextcord.ext import commands, tasks
import json
from datetime import datetime, timedelta
import asyncio
from itertools import batched

from database.db_down import *
from database.db_up import *

def create_leaderboard(embed, df):
    # Initializing values
    space = "\u1CBC"
    idx_emojis = [":first_place:", ":second_place:", ":third_place:"]
       
    # Make the strings for the title field and the three inline fields for this leaderboard (place, nickname, stat)
    title = df.index.name
    title_str = f"**{title}**"
    
    idx_str = ""
    name_str = ""
    stat_str = ""
    for i, row in df.iterrows():
        if i+1 <= len(idx_emojis):
            idx_str += f"{idx_emojis[i]}\n"
        else:
            idx_str += f"{i+1}.\n"
        name_str += f"{row.iloc[0]}\n"
        stat_str += f"{row.iloc[1]}\n"
    
    embed.add_field(name="", value="", inline=False)
    embed.add_field(name=title_str, value="", inline=False) # Title field
    embed.add_field(name="", value=idx_str, inline=True) # Index field
    embed.add_field(name="", value=name_str, inline=True) # Index field
    embed.add_field(name="", value=stat_str, inline=True) # Index field
    
    return embed

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

        n = 5
        # Getting the top 5's for certain columns
        matches_played = df.nlargest(n=n, columns='matches_played')[['nickname', 'matches_played']].reset_index(drop=True)
        matches_played.index.name = 'Matches Played'
        winrate = df.nlargest(n=n, columns='wins')[['nickname', 'wins']].reset_index(drop=True)
        winrate.index.name = 'Winrate'
        hltv = df.nlargest(n=n, columns='hltv')[['nickname', 'hltv']].reset_index(drop=True)
        hltv.index.name = 'HLTV Rating'
        knifes = df.nlargest(n=n, columns='knife_kills')[['nickname', 'knife_kills']].reset_index(drop=True)
        knifes.index.name = 'Knife Kills'
        
        df_list = [matches_played, winrate, hltv, knifes]
        
        # embed creating
        embed = nextcord.Embed(
            title="BeneluxHub Monthly Stats",
            colour=nextcord.Colour.blue()
        )

        embed.set_author(name="Benelux Bot",
                         icon_url="https://pbs.twimg.com/profile_images/1796692667008507904/sU3cv9vT_400x400.jpg")
    
        for df in df_list:
            embed = create_leaderboard(embed, df)

        await ctx.send(embed=embed)

def setup(client : commands.Bot):
    client.add_cog(BeneluxHub(client))
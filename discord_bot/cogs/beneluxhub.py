# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nextcord.ext import commands

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

    # --- Command to get the monthly stats of the BeneluxHub ---
    

def setup(client : commands.Bot):
    client.add_cog(BeneluxHub(client))
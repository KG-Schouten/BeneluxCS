# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from nextcord.ext import commands

class MainCog(commands.Cog):
    def __init__(self, client):
        self.client = client

    # Test command
    @commands.command()
    async def test(self, ctx):
        await ctx.send('Test command works!')
    
def setup(client : commands.Bot):
    client.add_cog(MainCog(client))
    
import nextcord
from nextcord.ext import commands
from datetime import datetime, timedelta
import asyncio

class MainCog(commands.Cog):
    def __init__(self, client):
        self.client = client

    # Test command
    @commands.command()
    async def test(self, ctx):
        await ctx.send('Test command works!')

def setup(client : commands.Bot):
    client.add_cog(MainCog(client))
    
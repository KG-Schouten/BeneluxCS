# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import nextcord
from nextcord.ext import commands
import os

# Load api keys from .env file
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

if not DISCORD_TOKEN:
    raise ValueError("DISCORD_TOKEN is not set in the environment variables.")

intents = nextcord.Intents.default()
intents.message_content = True
intents.members = True

client = commands.Bot(command_prefix='!', intents=intents)

extensions = {
    "main_cog",
    "ESEA",
    "faceit",
    "beneluxhub",
}


if __name__ == "__main__":
    for ext in extensions:
        client.load_extension("cogs." + ext)

@client.event
async def on_ready():
    print('Bot is ready')
    print('------------')

client.run(DISCORD_TOKEN)
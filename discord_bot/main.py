import nextcord
from nextcord.ext import commands
import os

# Load api keys from .env file
from dotenv import load_dotenv
load_dotenv()
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

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
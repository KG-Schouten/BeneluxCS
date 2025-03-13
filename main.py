import nextcord
from nextcord.ext import commands
from functions import load_api_keys

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

api_keys = load_api_keys()

if __name__ == "__main__":
    for ext in extensions:
        client.load_extension("cogs." + ext)

@client.event
async def on_ready():
    print('Bot is ready')
    print('------------')

client.run(api_keys.get("DISCORD_TOKEN"))
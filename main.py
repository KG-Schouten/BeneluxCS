import nextcord
from nextcord.ext import commands
from API_tokens import faceit_token, discord_token

intents = nextcord.Intents.default()
intents.message_content = True
intents.members = True

client = commands.Bot(command_prefix='!', intents=intents)

extensions = {
    "main_cog",
    "faceit",
}

if __name__ == "__main__":
    for ext in extensions:
        client.load_extension("cogs." + ext)

@client.event
async def on_ready():
    print('Bot is ready')
    print('------------')

client.run(discord_token)
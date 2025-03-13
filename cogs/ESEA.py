import nextcord
from nextcord.ext import commands, tasks
import json
from datetime import datetime

from faceit_api.faceit_v4 import FaceitData
from faceit_api.faceit_v1 import FaceitData_v1

from data_processing import store_team_data, read_team_data, team_data_path
from functions import find_team_name, load_api_keys

# Initialize the API keys
api_keys = load_api_keys()

class ESEA(commands.Cog):
    def __init__(self, client):
        self.client = client

    ## Command for refreshing ESEA team data
    @commands.command()
    async def refresh(self, ctx):
        store_team_data()
        await ctx.send('Data refreshed!')


    @commands.command()
    async def upcoming(self, ctx, team):
        """
        Sends an embed with upcoming matches and starts a timer to keep it updated.
        """

        # Opening the team data file
        with open(team_data_path, 'r') as file:
            team_data = json.load(file)
        
        # Use fuzzy matching to find the team name
        team_name = find_team_name(team, team_data)
        
        ## Embed creation
        embed = nextcord.Embed(
            title=f'{team_name} - Upcoming Matches',
            colour=nextcord.Colour.blue()
        )

        embed.set_author(name="Benelux Bot",
                         icon_url="https://pbs.twimg.com/profile_images/1796692667008507904/sU3cv9vT_400x400.jpg")

        embed.set_thumbnail(url=team_data[team_name]["avatar"])

        for stage in team_data[team_name]["ESEA"]["stages"]:
            for match in stage["matches"]:
                if match["status"] == "SCHEDULED":
                    ## Needed data
                    opponent_name = match["opponent_name"]
                    match_time = match["time"]
                    # match_time = datetime.fromtimestamp(match_time//1000).strftime('%a. %d %B %Y | %H:%M').upper()
                    timestamp = match_time // 1000
                    match_id = match["match_id"]
                    league_level = team_data[team_name]["ESEA"]["league_level"]
                    stage_name = stage["stage_name"]

                    ## Embed field
                    embed.add_field(name=f'vs. **{opponent_name}** in ESEA S52 {league_level} {stage_name}',
                                    value=f'**<t:{timestamp}>** - [Match Page](https://www.faceit.com/en/cs2/room/{match_id})',
                                    inline=False)
    
        await ctx.send(embed=embed)


    @commands.command()
    async def today(self, ctx):
        """
        Sends an embed with upcoming matches from today
        """

        df = read_team_data()

        today = datetime.today()

        ## Creating a dict with match data to create fields from
        match_dict = {} 

        ## Creating the embed
        embed = nextcord.Embed(
            title= f"MATCHDAY  - {today.strftime("%A, %d %B %Y")}",
            colour=nextcord.Colour.blue()
        )

        embed.set_author(name="Benelux Bot",
                         icon_url="https://pbs.twimg.com/profile_images/1796692667008507904/sU3cv9vT_400x400.jpg")



        for team_name, series in df.items():
            # For each stage, Get matches from a specific team
            for stage in series['ESEA']['stages']:
                matches = stage['matches']
                # matches = series[team_name].loc['ESEA']


                # Initialise the list for a league level if it does not exist yet
                if series['ESEA']['league_level'] not in match_dict:
                    match_dict[series['ESEA']['league_level']] = []

                # Append to list in match dict
                match_dict[series['ESEA']['league_level']].extend(
                    [
                        {
                            "name" : f'',
                            "value": f'> [**{team_name}** vs **{match['opponent_name']}**](https://www.faceit.com/en/cs2/room/{match['match_id']})\n> **<t:{match['time'] // 1000}:R>** - **<t:{match['time'] // 1000}:t>**'
                        }
                        for match in matches
                        if datetime.fromtimestamp(match['time'] // 1000).date() == today.date()
                        if not (datetime.fromtimestamp(match['time'] // 1000) > datetime.now() and match['status'] == "FINISHED")
                    ]
                )

        for div in match_dict.keys():
            # Create separate field for the Division name embed
            embed.add_field(
                name = f'**{div.upper()}**',
                value = '',
                inline = False
            )

            # Creating empty string to append embed data to
            embed_value = ''
            embed_value_list = []
            
            # Embed fields of matches. If no match in div found -> add field saying No match found
            if match_dict[div] == []:
                embed.add_field(
                        name = '',
                        value = "*No matches today*",
                        inline = False
                    )
            else:
                # Loop through matches in the division
                for match in match_dict[div]:
                    # If length of current string and added string is larger than value, append string to the list and continue with a new string
                    if len(embed_value) + len(match['value']) > 1024:
                        embed_value_list.append(embed_value)
                        embed_value = ''
                    
                    embed_value += match['value'] + '\n\n'
                
                # If embed_value is not empty, add this last value to the list
                if embed_value:
                    embed_value_list.append(embed_value)

                # Create embed field for every instance in the value list
                for i in embed_value_list:
                    embed.add_field(
                        name = '',
                        value = i,
                        inline = False
                    )

        await ctx.send(embed=embed)


def setup(client : commands.Bot):
    client.add_cog(ESEA(client))
import aiohttp

from rate_limit import rate_limiter, rate_monitor
from response_handler import check_response
from logging_config import logger


class FaceitData_v1:
    """The Data API for Faceit"""

    def __init__(self, session: aiohttp.ClientSession):
        """ Initialize the FaceitData_v1 class"""

        self.base_url = 'https://faceit.com/api'
        self.session = session
    
    async def all_leagues(self) -> dict | int:
        """ Retrieve all leagues from Faceit """
        
        URL = f'{self.base_url}/team-leagues/v2/leagues/a14b8616-45b9-4581-8637-4dfd0b5f6af8/seasons'
        
        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)
    
    async def league_season_stages(self, season_id: str) -> dict | int:
        """
        Retrieve all stages from a specific league season

        :param season_id: The ID of the season
        :return:
        """
        
        URL = "https://www.faceit.com/api/team-leagues/v1/get_filters"
        
        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)
    
    async def league_season_stage_teams(self, conference_id: str, starting_item_position: int=0, return_items: int=20) -> dict | int:
        """
        Retrieve all teams from a specific league season

        :param season_id: The ID of the season
        :return:
        """
        
        URL = "https://www.faceit.com/api/team-leagues/v2/conferences/{}/registrations?conferenceId={}&search=&offset={}&limit={}".format(
            conference_id, conference_id, int(starting_item_position), int(return_items)
        )
        
        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)
    
    async def league_team_details(self, team_id: str) -> dict | int:
        """
        Retrieve league details for a team

        :param team_id: The ID of the team
        :return:
        """
    
        URL = f'{self.base_url}/team-leagues/v1/teams/{team_id}/profile/leagues/summary'
        
        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)
    
    async def league_team_matches(self, team_id: str, league_id: str) -> dict | int:
        """
        Retrieve league matches for a team

        :param team_id: The ID of the team
        :param league_id: The ID of the league
        :return:
        """
        
        URL = f'{self.base_url}/championships/v1/matches?participantId={team_id}&participantType=TEAM&championshipId={league_id}&limit=20&offset=0&sort=ASC'

        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)
            
    async def league_team_details_batch(self, team_ids: list[str]) -> dict | int:
        """
        Retrieve league details for a batch of teams

        :param team_ids: The IDs of the teams
        :return:
        """
        
        URL = "https://www.faceit.com/api/teams/v3/teams/batch-get"

        body = {
            "ids": team_ids
        }

        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL, json=body) as response:
                return await check_response(response)
    
    async def league_team_players(self, team_id: str) -> dict | int:
        """
        Retrieve active league members from a team (so in the ESEA roster

        :param team_id: The ID of the team
        :return:
        """

        URL = f'{self.base_url}/team-leagues/v2/teams/{team_id}/members/active'

        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)

    async def player_friend_list(self, player_id: str, starting_item_position: int=0, return_items: int=20) -> dict | int:
        """
        Retrieve the friend list from a specific player
        
        : param player_id: The ID of the player
        :param starting_item_position: The starting item position (default is 0)
        :param return_items: The number of items to return (default is 20)
        :return:
        """

        URL = "{}/recommender/v1/friends/{}?offset={}&limit={}".format(
            self.base_url, player_id, starting_item_position, return_items
        )
        
        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)
    
    async def player_hubs(self, player_id: str, return_items: int=10, starting_item_position: int=0) -> dict | int:
        """
        Retrieve the hubs from a specific player
        
        :pram player_id: The ID of the player
        :param return_items: The number of items to return (default is 10)
        :param starting_item_position: The starting item position (default is 0)
        :return:
        """
        
        URL = "{}/hubs/v1/user/{}/membership?limit={}&offset={}&clubs=true".format(
            self.base_url, player_id, return_items, starting_item_position
        )
        
        async with rate_limiter:
            rate_monitor.register_request()
            rate_monitor.debug_log()
            
            async with self.session.get(URL) as response:
                return await check_response(response)


    
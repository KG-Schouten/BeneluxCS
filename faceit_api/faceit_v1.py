import aiohttp
import asyncio
from asynciolimiter import StrictLimiter

class RateLimitException(Exception):
    """ Raised when API rate limit is reached """
    pass

rate_limiter = StrictLimiter(400, 10)  # 1 request per 0.5 seconds


async def check_response(response: aiohttp.ClientResponse) -> dict | int:
    """ Checks the response from the API and returns the data or raises an exception """
    if response.status == 200:
        return await response.json()
    elif response.status == 429:
        print("Rate limit reached")
        raise RateLimitException("Rate limit reached")
    else:
        error_map = {
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            500: "Internal Server Error",
            502: "Bad Gateway",
            503: "Service Unavailable",
        }
        print(error_map.get(response.status, f"HTTP Unknown Error: {response.status}"))
        return response.status


class FaceitData_v1:
    """The Data API for Faceit"""

    def __init__(self, session: aiohttp.ClientSession):
        """ Initialize the FaceitData_v1 class"""

        self.base_url = 'https://faceit.com/api'
        self.session = session
    
    async def defall_leagues(self) -> dict | int:
        """ Retrieve all leagues from Faceit """
        
        URL = f'{self.base_url}/team-leagues/v2/leagues/a14b8616-45b9-4581-8637-4dfd0b5f6af8/seasons'
        
        async with rate_limiter:
            async with self.session.get(URL) as response:
                return await check_response(response)
    
    async def defleague_season_stages(self, season_id: str) -> dict | int:
        """
        Retrieve all stages from a specific league season

        :param season_id: The ID of the season
        :return:
        """
        
        URL = "https://www.faceit.com/api/team-leagues/v1/get_filters"
        
        body = {"seasonId":season_id}
        
        res = requests.post(URL, json=body)

        return check_response(res)
    
    async def defleague_season_stage_teams(self, conference_id: str, starting_item_position: int=0, return_items: int=20) -> dict | int:
        """
        Retrieve all teams from a specific league season

        :param season_id: The ID of the season
        :return:
        """
        
        URL = "https://www.faceit.com/api/team-leagues/v2/conferences/{}/registrations?conferenceId={}&search=&offset={}&limit={}".format(
            conference_id, conference_id, int(starting_item_position), int(return_items)
        )
        
        res = requests.get(URL)

        return check_response(res)
    
    async def defleague_team_details(self, team_id: str) -> dict | int:
        """
        Retrieve league details for a team

        :param team_id: The ID of the team
        :return:
        """
    
        URL = f'{self.base_url}/team-leagues/v1/teams/{team_id}/profile/leagues/summary'
        
        res = requests.get(URL)

        return check_response(res)
    
    async def defleague_team_matches(self, team_id: str, league_id: str) -> dict | int:
        """
        Retrieve league matches for a team

        :param team_id: The ID of the team
        :param league_id: The ID of the league
        :return:
        """
        
        URL = f'{self.base_url}/championships/v1/matches?participantId={team_id}&participantType=TEAM&championshipId={league_id}&limit=20&offset=0&sort=ASC'

        res = requests.get(URL)

        return check_response(res)
    
    async def defleague_team_details_batch(self, team_ids: list[str]) -> dict | int:
        """
        Retrieve league details for a batch of teams

        :param team_ids: The IDs of the teams
        :return:
        """
        
        URL = "https://www.faceit.com/api/teams/v3/teams/batch-get"

        body = {
            "ids": team_ids
        }

        res = requests.post(URL, json=body)

        return check_response(res)
    
    async def defleague_team_players(self, team_id: str) -> dict | int:
        """
        Retrieve active league members from a team (so in the ESEA roster

        :param team_id: The ID of the team
        :return:
        """

        URL = f'{self.base_url}/team-leagues/v2/teams/{team_id}/members/active'

        res = requests.get(URL)

        return check_response(res)

    async def defplayer_friend_list(self, player_id: str, starting_item_position: int=0, return_items: int=20) -> dict | int:
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
        
        res = requests.get(URL)
        
        return check_response(res)
    
    async def defplayer_hubs(self, player_id: str, return_items: int=10, starting_item_position: int=0) -> dict | int:
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
        
        res = requests.get(URL)
        
        return check_response(res)


    
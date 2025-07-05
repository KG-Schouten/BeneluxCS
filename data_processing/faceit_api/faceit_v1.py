# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import aiohttp

from data_processing.faceit_api.response_handler import check_response
from data_processing.faceit_api.sliding_window import RequestDispatcher

class FaceitData_v1:
    """The Data API for Faceit"""

    def __init__(self, dispatcher: RequestDispatcher):
        """ Initialize the FaceitData_v1 class"""

        self.base_url = 'https://faceit.com/api'
        self.session = None
        self.dispatcher = dispatcher
    
    async def __aenter__(self):
        """ Enter the asynchronous context manager """
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        """ Exit the asynchronous context manager """
        if self.session is not None:
            # Close the session if it was initialized
            await self.session.close()
        else:
            raise RuntimeError("Session was not initialized. Please use the context manager to initialize it.")
    
    async def _get(self, url:str) -> dict | int:
        """ Helper function to fetch data from a GET request """
        if self.session is None:
            raise RuntimeError("Session is not initialized. Please use the context manager to initialize it.")
        async with self.session.get(url) as response:
            return await check_response(response)
    
    async def _post(self, url:str, body:dict) -> dict | int:
        """ Helper function to fetch data from a POST request """
        if self.session is None:
            raise RuntimeError("Session is not initialized. Please use the context manager to initialize it.")
        async with self.session.post(url, json=body) as response:
            return await check_response(response)
    
    async def _get_with_params(self, url:str, params:dict) -> dict | int:
        """ Helper function to fetch data from a GET request with parameters """
        if self.session is None:
            raise RuntimeError("Session is not initialized. Please use the context manager to initialize it.")
        async with self.session.get(url, params=params) as response:
            return await check_response(response)
    
    
    async def league_details(self) -> dict | int:
        """ Retrieve league details from Faceit """
        
        URL = f'{self.base_url}/team-leagues/v2/leagues/a14b8616-45b9-4581-8637-4dfd0b5f6af8'
        
        return await self.dispatcher.run(self._get, URL)
    
    async def league_seasons(self) -> dict | int:
        """ Retrieve all leagues from Faceit """
        
        URL = f'{self.base_url}/team-leagues/v2/leagues/a14b8616-45b9-4581-8637-4dfd0b5f6af8/seasons'
        
        return await self.dispatcher.run(self._get, URL)
    
    async def league_season_stages(self, season_id: list[str]|str) -> dict | int:
        """
        Retrieve all stages from a specific league season

        :param season_id: The ID of the season (list of IDs or a single ID)
        :return:
        """
        
        URL = "https://www.faceit.com/api/team-leagues/v1/get_filters"

        body = {
            'seasonId': season_id
        }
        
        return await self.dispatcher.run(self._post, URL, body)
    
    async def league_season_stage_teams(self, conference_id: str, starting_item_position: int=0, return_items: int=20) -> dict | int:
        """
        Retrieve all teams from a specific league season

        :param season_id: The ID of the season
        :return:
        """
        
        URL = "https://www.faceit.com/api/team-leagues/v2/conferences/{}/registrations?conferenceId={}&search=&offset={}&limit={}".format(
            conference_id, conference_id, int(starting_item_position), int(return_items)
        )
        
        return await self.dispatcher.run(self._get, URL)
    
    async def league_team_details(self, team_id: str) -> dict | int:
        """
        Retrieve league details for a team

        :param team_id: The ID of the team
        :return:
        """
    
        URL = f'{self.base_url}/team-leagues/v1/teams/{team_id}/profile/leagues/summary'
        
        return await self.dispatcher.run(self._get, URL)
    
    async def league_team_matches(self, team_id: str, championship_id: list[str] | str) -> dict | int:
        """
        Retrieve league matches for a team

        :param team_id: The ID of the team
        :param championship_id: The ID of the championship (list of IDs or a single ID)
        :return:
        """

        URL = f'{self.base_url}/championships/v1/matches'
        
        params = {
            "participantId" : team_id,
            "participantType" : "TEAM",
            "championshipId" : championship_id,
            "limit" : "20",
            "offset" : "0",
            "sort" : "ASC"
        }
        
        return await self.dispatcher.run(self._get_with_params, URL, params)
    
    async def league_team_players(self, team_id: str) -> dict | int:
        """
        Retrieve active league members from a team (so in the ESEA roster

        :param team_id: The ID of the team
        :return:
        """

        URL = f'{self.base_url}/team-leagues/v2/teams/{team_id}/members/active'

        return await self.dispatcher.run(self._get, URL)

    async def player_details_batch(self, player_ids: list[str]) -> dict | int:
        """
        Retrieve player details for a batch of players

        :param player_ids: The IDs of the players
        :return:
        """
        
        URL = "https://www.faceit.com/api/user-summary/v2/list"

        body = {
            "ids": player_ids
        }

        return await self.dispatcher.run(self._post, URL, body)
    
    async def player_details_batch_v1(self, player_ids: list[str]) -> dict | int:
        """ Retrieve player details for a batch of players (v1 endpoint) """
        
        URL = "https://www.faceit.com/api/user-summary/v1/list"
        
        body = {
            "ids": player_ids
        }
        
        return await self.dispatcher.run(self._post, URL, body)
    
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
        
        return await self.dispatcher.run(self._get, URL)
    
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
        
        return await self.dispatcher.run(self._get, URL)

    async def championship_details(self, championship_id: str) -> dict | int:
        """
        Retrieve championship details from Faceit
        
        :param championship_id: The ID of the championship
        :return:
        """
        
        URL = f"https://www.faceit.com/api/championships/v1/championship/{championship_id}"
        return await self.dispatcher.run(self._get, URL)
    
    async def hub_details(self, hub_id: str) -> dict | int:
        """
        Retrieve club details from Faceit
        
        :param hub_id: The ID of the hub
        :return:
        """
        
        URL = f"https://www.faceit.com/api/hubs/v1/hub/{hub_id}"
        
        return await self.dispatcher.run(self._get, URL)
    
    async def hub_members(
        self, 
        hub_id: str, 
        offset: int=0, 
        limit: int=20, 
        userNickname: str = '',
        roles: str = ''
    ) -> dict | int:
        """ Retrieve club members """
        
        URL = f"https://www.faceit.com/api/hubs/v1/hub/{hub_id}/membership?offset={offset}&limit={limit}&userNickname={userNickname}&roles={roles}"

        return await self.dispatcher.run(self._get, URL)
        
    async def league_teams(self, conference_id: str, offset: int=0, limit: int=20) -> dict | int:
        """
        Retrieve all teams from a specific league conference

        :param conference_id: The ID of the conference
        :param offset: The offset for pagination (default is 0)
        :param limit: The number of items to return (default is 20)
        :return:
        """
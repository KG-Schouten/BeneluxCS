import json
import requests
import urllib.parse

def check_response(res: requests.Response) -> dict | int:
    if res.status_code == 200:
        return json.loads(res.content.decode('utf-8'))
    elif res.status_code ==  400:
        print("Bad request - The request was unacceptable, often due to missing a required parameter")
        return res.status_code
    elif res.status_code ==  401:
        print("Unauthorized - Invalid or missing credentials")
        return res.status_code
    elif res.status_code ==  403:
        print("Forbidden - The request was understood, but it has been refused or access is not allowed")
        return res.status_code
    elif res.status_code ==  404:
        print("Not Found - The URI requested is invalid or the resource requested, such as a user, does not exist")
        return res.status_code
    elif res.status_code ==  429:
        print("Too Many Requests - Rate limiting has been applied")
        return res.status_code
    elif res.status_code ==  503:
        print("Service Unavailable - The service is temporarily unavailable")
        return res.status_code


class FaceitData_v1:
    """The Data API for Faceit"""

    def __init__(self):

        self.base_url = 'https://faceit.com/api'

    def league_team_details(self, team_id: str) -> dict | int:
        """
        Retrieve league details for a team

        :param team_id: The ID of the team
        :return:
        """
    
        URL = f'{self.base_url}/team-leagues/v1/teams/{team_id}/profile/leagues/summary'
        
        res = requests.get(URL)

        return check_response(res)
    
    def league_team_matches(self, team_id: str, league_id: str) -> dict | int:
        """
        Retrieve league matches for a team

        :param team_id: The ID of the team
        :param league_id: The ID of the league
        :return:
        """
    
        URL = f'{self.base_url}/championships/v1/matches?participantId={team_id}&participantType=TEAM&championshipId={league_id}&limit=20&offset=0&sort=ASC'

        res = requests.get(URL)

        return check_response(res)
    
    def league_team_players(self, team_id: str) -> dict | int:
        """
        Retrieve active league members from a team (so in the ESEA roster

        :param team_id: The ID of the team
        :return:
        """

        URL = f'{self.base_url}/team-leagues/v2/teams/{team_id}/members/active'

        res = requests.get(URL)

        return check_response(res)




    
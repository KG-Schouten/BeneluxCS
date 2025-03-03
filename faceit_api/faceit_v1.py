import json
import requests
import urllib.parse


class FaceitData_v1:
    """The Data API for Faceit"""

    def __init__(self):

        self.base_url = 'https://faceit.com/api'

    def league_team_details(self, team_id):
        """
        Retrieve league details for a team

        :param team_id: The ID of the team
        :return:
        """
    
        URL = f'{self.base_url}/team-leagues/v1/teams/{team_id}/profile/leagues/summary'
        
        res = requests.get(URL)

        return json.loads(res.content.decode('utf-8'))
    
    def league_team_matches(self, team_id, league_id):
        """
        Retrieve league matches for a team

        :param team_id: The ID of the team
        :param league_id: The ID of the league
        :return:
        """
    
        URL = f'{self.base_url}/championships/v1/matches?participantId={team_id}&participantType=TEAM&championshipId={league_id}&limit=20&offset=0&sort=ASC'

        res = requests.get(URL)

        return json.loads(res.content.decode('utf-8'))
    
    def league_team_players(self, team_id):
        """
        Retrieve active league members from a team (so in the ESEA roster

        :param team_id: The ID of the team
        :return:
        """

        URL = f'{self.base_url}/team-leagues/v2/teams/{team_id}/members/active'

        res = requests.get(URL)

        return json.loads(res.content.decode('utf-8'))




    
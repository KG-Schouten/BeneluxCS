import json
import requests
import urllib.parse

def check_response(res):
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
    else:
        print("No idea what response we got")
        return


class FaceitData:
    """The Data API for Faceit"""

    def __init__(self, api_token):
        """
        Constructor Keyword arguments:

        :param api_token: The api token used for the Faceit API (either client or server API types)
        """

        self.api_token = api_token
        self.base_url = 'https://open.faceit.com/data/v4'

        self.headers = {
            'accept': 'application/json',
            'Authorization': 'Bearer {}'.format(self.api_token)
        }


    # Leagues (NEW!!!!)
    def leagues_details(self, league_id):
        """
        Retrieve league details

        :param league_id: The ID of the league
        :return:
        """

        api_url = "{}/leagues/{}".format(self.base_url, league_id)

        res = requests.get(api_url, headers=self.headers)

        return check_response(res)
        
    def leagues_season_details(self, league_id, season_id):
        """
        Retrieve league season details

        :param league_id: The ID of the league
        :param season_id: The ID of the season
        :return:
        """

        api_url = "{}/leagues/{}/seasons/{}".format(self.base_url, league_id, season_id)

        res = requests.get(api_url, headers=self.headers)

        return check_response(res)


    # Championships
    def championship_details(self, championship_id, expanded=None):
        """
        Retrieve championship details

        :param championship_id: The ID of the championship
        :param expanded: List of entity names to expand in request, either "organizer" or "game"
        :return:
        """

        api_url = "{}/championships/{}".format(self.base_url, championship_id)
        if expanded is not None:
            if expanded.lower() == 'game':
                api_url += '?expanded=game'
            elif expanded.lower() == 'organizer':
                api_url += '?expanded=organizer'

        res = requests.get(api_url, headers=self.headers)

        return check_response(res)

    def championship_matches(self, championship_id, type_of_match="all", starting_item_position=0, return_items=20):
        """
        Championship match details

        :param championship_id: The championship ID
        :param type_of_match: Kind of matches to return. Can be all(default), upcoming, ongoing or past
        :param starting_item_position: The starting item position (default 0)
        :param return_items: The number of items to return (default 20)
        :return:
        """

        api_url = "{}/championships/{}/matches?type={}&offset={}&limit={}".format(
            self.base_url, championship_id, type_of_match, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        
        return check_response(res)

    def championship_subscriptions(self, championship_id, starting_item_position=0, return_items=10):
        """
        Retrieve all subscriptions of a championship

        :param championship_id: The championship ID
        :param starting_item_position: The starting item position (default 0)
        :param return_items: The number of items to return (default 10)
        :return:
        """

        api_url = "{}/championships/{}/subscriptions?offset={}&limit={}".format(
            self.base_url, championship_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        
        return check_response(res)
    # Games
    def all_faceit_games(self, starting_item_position=0, return_items=20):
        """
        Retrieve details of all games on FACEIT

        :param starting_item_position: The starting item position (default 0)
        :param return_items: The number of items to return (default 20)
        :return:
        """

        api_url = "{}/games?offset={}&limit={}".format(self.base_url, starting_item_position, return_items)
        res = requests.get(api_url, headers=self.headers)
        
        return check_response(res)
    
    def game_details(self, game_id):
        """
        Retrieve game details

        :param game_id: The ID of the game
        :return:
        """

        api_url = "{}/games/{}".format(self.base_url, game_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def game_details_parent(self, game_id=None):
        """
        Retrieve the details of the parent game, if the game is region-specific.

        :param game_id: The ID of the game
        :return:
        """

        api_url = "{}/games/{}/parent".format(self.base_url, game_id)
        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Hubs
    def hub_details(self, hub_id, game=None, organizer=None):
        """
        Retrieve hub details

        :param hub_id: The ID of the hub
        :param game: An entity to expand in request (default is None, but can be True)
        :param organizer: An entity to expand in request (default is None, but can be True)
        :return:
        """

        api_url = "{}/hubs/{}".format(self.base_url, hub_id)

        if game is not None:
            if game is True:
                api_url += "?expanded=game"
        if organizer is not None:
            if game is None:
                if organizer:
                    api_url += "?expanded=organizer"

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def hub_matches(self, hub_id, type_of_match="all", starting_item_position=0, return_items=20):
        """
        Retrieve all matches of a hub

        :param hub_id: The ID of the hub (required)
        :param type_of_match: Kind of matches to return. Default is all, can be upcoming, ongoing, or past
        :param starting_item_position: The starting item position. Default is (return_items should be a multiple of this!!)
        :param return_items: The number of items to return. Default is 20 (Must be a multiple of starting_item_position)
        :return:
        """

        api_url = "{}/hubs/{}/matches?type={}&offset={}&limit={}".format(
            self.base_url, hub_id, type_of_match, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def hub_members(self, hub_id, starting_item_position=0, return_items=20):
        """
        Retrieve all members of a hub

        :param hub_id: The ID of the hub (required)
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/hubs/{}/members?offset={}&limit={}".format(
            self.base_url, hub_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def hub_roles(self, hub_id, starting_item_position=0, return_items=20):
        """
        Retrieve all roles members can have in a hub

        :param hub_id: The ID of the hub
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/hubs/{}/roles?offset={}&limit={}".format(
            self.base_url, hub_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def hub_statistics(self, hub_id, starting_item_position=0, return_items=20):
        """
        Retrieves statistics of a hub

        :param hub_id: The ID of the hub
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/hubs/{}/stats?offset={}&limit={}".format(
            self.base_url, hub_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Leaderboards
    def championship_leaderboards(self, championship_id, starting_item_position=0, return_items=20):
        """
        Retrieves all leaderboards of a championship

        :param championship_id: The ID of a championship
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/leaderboards/championships/{}?offset={}&limit={}".format(
            self.base_url, championship_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def championship_group_ranking(self, championship_id, group, starting_item_position=0, return_items=20):
        """
        Retrieve group ranking of a championship

        :param championship_id: The ID of a championship
        :param group: A group of the championship
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/leaderboards/championships/{}/groups/{}?offset={}&limit={}".format(
            self.base_url, championship_id, group, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def hub_leaderboards(self, hub_id, starting_item_position=0, return_items=20):
        """
        Retrieve all leaderboards of a hub

        :param hub_id: The ID of the hub
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/leaderboards/hubs/{}?offset={}&limit={}".format(
            self.base_url, hub_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def hub_ranking(self, hub_id, starting_item_position=0, return_items=20):
        """
        Retrieve all time ranking of a hub

        :param hub_id: The ID of the hub
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/leaderboards/hubs/{}/general?offset={}&limit={}".format(
            self.base_url, hub_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def hub_season_ranking(self, hub_id, season, starting_item_position=0, return_items=20):
        """
        Retrieve seasonal ranking of a hub

        :param hub_id: The ID of the hub
        :param season: A season of the hub
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/leaderboards/hubs/{}/seasons/{}?offset={}&limit={}".format(
            self.base_url, hub_id, season, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def leaderboard_ranking(self, leaderboard_id, starting_item_position=0, return_items=20):
        """
        Retrieve ranking from a leaderboard ID

        :param leaderboard_id: The ID of the leaderboard
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/leaderboards/{}?offset={}&limit={}".format(
            self.base_url, leaderboard_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Matches
    def match_details(self, match_id):
        """
        Retrieve match details

        :param match_id: The ID of the match
        :return:
        """

        api_url = "{}/matches/{}".format(self.base_url, match_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def match_stats(self, match_id):
        """
        Retrieve match details

        :param match_id: The ID of the match
        :return:
        """

        api_url = "{}/matches/{}/stats".format(self.base_url, match_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Organizers
    def organizer_details(self, name_of_organizer=None, organizer_id=None):
        """
        Retrieve organizer details

        :param name_of_organizer: The name of organizer (use either this, or the organizer_id)
        :param organizer_id: The ID of the organizer (use either this, or the name_of_organizer)
        :return:
        """

        if name_of_organizer is None:
            if organizer_id is None:
                raise ValueError('You cannot set name_of_organizer and organizer_id to None. Need to choose one.')
            else:
                api_url = "{}/organizers"

                if name_of_organizer is not None:
                    api_url += "?name={}".format(name_of_organizer)
                else:
                    if organizer_id is not None:
                        api_url += "/{}".format(organizer_id)
                res = requests.get(api_url, headers=self.headers)
                return check_response(res)

    def organizer_championships(self, organizer_id, starting_item_position=0, return_items=20):
        """
        Retrieve all championships of an organizer

        :param organizer_id: The ID of the organizer
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/organizers/{}/championships?offset={}&limit={}".format(
            self.base_url, organizer_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def organizer_games(self, organizer_id):
        """
        Retrieve all games an organizer is involved with.

        :param organizer_id: The ID of the organizer
        :return:
        """

        api_url = "{}/organizers/{}/games".format(
            self.base_url, organizer_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def organizer_hubs(self, organizer_id, starting_item_position=0, return_items=20):
        """
        Retrieve all hubs of an organizer

        :param organizer_id: The ID of the organizer
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/organizers/{}/hubs?offset={}&limit={}".format(
            self.base_url, organizer_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def organizer_tournaments(self, organizer_id, type_of_tournament="upcoming", starting_item_position=0,
                              return_items=20):
        """
        Retrieve all tournaments of an organizer

        :param organizer_id: The ID of the organizer
        :param type_of_tournament: Kind of tournament. Can be upcoming(default) or past
        :param starting_item_position: The starting item position. Default is 0
        :param return_items: The number of items to return. Default is 20
        :return:
        """

        api_url = "{}/organizers/{}/tournaments?type={}&offset={}&limit={}".format(
            self.base_url, organizer_id, type_of_tournament, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Players
    def player_details(self, nickname):
        """
        Retrieve player details

        :param nickname: The nickname of the player of Faceit
        :return:
        """

        api_url = "{}/players?nickname={}".format(self.base_url, nickname)  # game_player_id and game broken i think
        # if game_player_id is not None:
        #     if nickname is not None:
        #         api_url += "&game_player_id={}".format(game_player_id)
        #     else:
        #         api_url += "?game_player_id={}".format(game_player_id)
        # if game is not None:
        #     api_url += "&game={}".format(game)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def player_id_details(self, player_id):
        """
        Retrieve player details

        :param player_id: The ID of the player
        :return:
        """

        api_url = "{}/players/{}".format(self.base_url, player_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def player_matches(self, player_id, game, from_timestamp=None, to_timestamp=None,
                       starting_item_position=0, return_items=20):
        """
        Retrieve all matches of a player

        :param player_id: The ID of a player
        :param game: A game on Faceit
        :param from_timestamp: The timestamp (UNIX time) as a lower bound of the query. 1 month ago if not specified (round it up/down to whole days!!!)
        :param to_timestamp: The timestamp (UNIX time) as a higher bound of the query. Current timestamp if not specified (round it up/down to whole days!!!)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/players/{}/history".format(self.base_url, player_id)

        # if from_timestamp is None:
        #     if to_timestamp is None:
        #         api_url += "?game={}&offset={}&limit={}".format(
        #             game, starting_item_position, return_items)
        #     else:
        #         api_url += "?game={}&limit={}&to={}".format(
        #             game, return_items, to_timestamp)
        # else:
        #     api_url += "?from={}".format(from_timestamp)

        api_url += "?game={}&from={}&to={}&limit={}".format(game, from_timestamp, to_timestamp, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def player_hubs(self, player_id, starting_item_position=0, return_items=20):
        """
        Retrieve all hubs of a player

        :param player_id: The ID of a player
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/players/{}/hubs?offset={}&limit={}".format(
            self.base_url, player_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def player_stats(self, player_id, game_id):
        """
        Retrieve the statistics of a player

        :param player_id: The ID of a player
        :param game_id: A game on Faceit
        :return:
        """

        api_url = "{}/players/{}/stats/{}".format(self.base_url, player_id, game_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def player_tournaments(self, player_id, starting_item_position=0, return_items=20):
        """
        Retrieve all hubs of a player

        :param player_id: The ID of a player
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/players/{}/tournaments?offset={}&limit={}".format(
            self.base_url, player_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Rankings
    def game_global_ranking(self, game_id, region, country=None, starting_item_position=0, return_items=20):
        """
        Retrieve global ranking of a game

        :param game_id: The ID of a game (Required)
        :param region: A region of a game (Required)
        :param country: A country code (ISO 3166-1)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/rankings/games/{}/regions/{}".format(
            self.base_url, game_id, region)
        if country is not None:
            api_url += "?country={}&offset={}&limit={}".format(
                country, starting_item_position, return_items)
        else:
            api_url += "?offset={}&limit={}".format(
                starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def player_ranking_of_game(self, game_id, region, player_id, country=None, return_items=20):
        """
        Retrieve user position in the global ranking of a game

        :param game_id: The ID of a game (required)
        :param region: A region of a game (required)
        :param player_id: The ID of a player (required)
        :param country: A country code (ISO 3166-1)
        :param return_items: The number of items to return (default is 20)
        :return:
        """

        api_url = "{}/rankings/games/{}/regions/{}/players/{}".format(
            self.base_url, game_id, region, player_id)

        if country is not None:
            api_url += "?country={}&limit={}".format(
                country, return_items)
        else:
            api_url += "?limit={}".format(return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Search
    def search_championships(self, name_of_championship, game=None, region=None, type_of_competition="all",
                             starting_item_position=0, return_items=20):
        """
        Search for championships

        :param name_of_championship: The name of a championship on Faceit (required)
        :param game: A game on Faceit
        :param region: A region of the game
        :param type_of_competition: Kind of competitions to return (default is all, can be upcoming, ongoing, or past)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/search/championships?name={}&type={}&offset={}&limit={}".format(
            self.base_url, urllib.parse.quote_plus(name_of_championship), type_of_competition,
            starting_item_position, return_items)

        if game is not None:
            api_url += "&game={}".format(game)
        elif region is not None:
            api_url += "&region={}".format(region)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def search_hubs(self, name_of_hub, game=None, region=None, starting_item_position=0, return_items=20):
        """
        Search for hubs

        :param name_of_hub: The name of a hub on Faceit (required)
        :param game: A game on Faceit
        :param region: A region of the game
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/search/hubs?name={}&offset={}&limit={}".format(
            self.base_url, urllib.parse.quote_plus(name_of_hub), starting_item_position, return_items)

        if game is not None:
            api_url += "&game={}".format(game)
        elif region is not None:
            api_url += "&region={}".format(region)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def search_organizers(self, name_of_organizer, starting_item_position=0, return_items=20):
        """
        Search for organizers

        :param name_of_organizer: The name of an organizer on Faceit
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/search/organizers?name={}&offset={}&limit={}".format(
            self.base_url, urllib.parse.quote_plus(name_of_organizer), starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def search_players(self, nickname, game=None, country_code=None, starting_item_position=0, return_items=20):
        """
        Search for players

        :param nickname: The nickname of a player on Faceit (required)
        :param game: A game on Faceit
        :param country_code: A country code (ISO 3166-1)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/search/players?nickname={}&offset={}&limit={}".format(
            self.base_url, urllib.parse.quote_plus(nickname), starting_item_position, return_items)

        if game is not None:
            api_url += "&game={}".format(urllib.parse.quote_plus(game))
        elif country_code is not None:
            api_url += "&country={}".format(country_code)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def search_teams(self, nickname, game=None, starting_item_position=0, return_items=20):
        """
        Search for teams

        :param nickname: The nickname of a team on Faceit (required)
        :param game: A game on Faceit
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/search/teams?nickname={}&offset={}&limit={}".format(
            self.base_url, urllib.parse.quote_plus(nickname), starting_item_position, return_items)

        if game is not None:
            api_url += "&game={}".format(urllib.parse.quote_plus(game))

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def search_tournaments(self, name_of_tournament, game=None, region=None, type_of_competition="all",
                           starting_item_position=0, return_items=20):
        """
        Search for tournaments

        :param name_of_tournament: The name of a tournament on Faceit (required)
        :param game: A game on Faceit
        :param region: A region of the game
        :param type_of_competition: Kind of competitions to return (default is all, can be upcoming, ongoing, or past)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/search/tournaments?name={}&type={}&offset={}&limit={}".format(
            self.base_url, urllib.parse.quote_plus(name_of_tournament), type_of_competition,
            starting_item_position, return_items)

        if game is not None:
            api_url += "&game={}".format(urllib.parse.quote_plus(game))
        elif region is not None:
            api_url += "&region={}".format(region)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Teams
    def team_details(self, team_id):
        """
        Retrieve team details
        :param team_id: The ID of the team (required)
        :return:
        """

        api_url = "{}/teams/{}".format(self.base_url, team_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def team_stats(self, team_id, game_id):
        """
        Retrieve statistics of a team

        :param team_id: The ID of a team (required)
        :param game_id: A game on Faceit (required)
        :return:
        """

        api_url = "{}/teams/{}/stats/{}".format(self.base_url, team_id, urllib.parse.quote_plus(game_id))

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def team_tournaments(self, team_id, starting_item_position=0, return_items=20):
        """
        Retrieve tournaments of a team

        :param team_id: The ID of a team (required)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/teams/{}/tournaments?offset={}&limit={}".format(
            self.base_url, team_id, starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    # Tournaments (no longer used)
    def all_tournaments(self, game=None, region=None, type_of_tournament="upcoming"):
        """
        Retrieve all tournaments

        :param game: A game on Faceit
        :param region: A region of the game
        :param type_of_tournament: Kind of tournament. Can be upcoming(default) or past
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/tournaments?type={}".format(
            self.base_url, type_of_tournament)

        if game is not None:
            api_url += "&game={}".format(urllib.parse.quote_plus(game))
        elif region is not None:
            api_url += "&region={}".format(region)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def tournament_details(self, tournament_id, expanded=None):
        """
        Retrieve tournament details

        :param tournament_id: The ID of the tournament (required)
        :param expanded: List of entity names to expand in request, either "organizer" or "game"
        :return:
        """

        api_url = "{}/tournaments/{}".format(self.base_url, tournament_id)
        if expanded is not None:
            if expanded.lower() == "organizer":
                api_url += "?expanded=organizer"
            elif expanded.lower() == "game":
                api_url += "?expanded=game"

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def tournament_brackets(self, tournament_id):
        """
        Retrieve brackets of a tournament

        :param tournament_id: The ID of the tournament (required)
        :return:
        """

        api_url = "{}/tournaments/{}/brackets".format(self.base_url, tournament_id)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def tournament_matches(self, tournament_id, starting_item_position=0, return_items=20):
        """
        Retrieve all matches of a tournament

        :param tournament_id: The ID of a tournament (required)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/tournaments/{}/matches?offset={}&limit={}".format(self.base_url, tournament_id,
                                                                        starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

    def tournament_teams(self, tournament_id, starting_item_position=0, return_items=20):
        """
        Retrieve all teams of a tournament

        :param tournament_id: The ID of a tournament (required)
        :param starting_item_position: The starting item position (Default is 0)
        :param return_items: The number of items to return (Default is 20)
        :return:
        """

        api_url = "{}/tournaments/{}/teams?offset={}&limit={}".format(self.base_url, tournament_id,
                                                                      starting_item_position, return_items)

        res = requests.get(api_url, headers=self.headers)
        return check_response(res)

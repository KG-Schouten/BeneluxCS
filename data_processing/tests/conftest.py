import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
from unittest.mock import AsyncMock
from data_processing.faceit_api import sliding_window

# Allow async test functions
pytestmark = pytest.mark.asyncio

@pytest.fixture
def mock_faceit_data_v1():
    """Default working mock of FaceitData_v1 with structured payloads."""
    mock = AsyncMock()

    mock.league_seasons.return_value = {
        "payload": [
            {
                "id": "season1",
                "season_number": 1,
                "map_pool": [{"maps": [{"name": "de_dust2"}]}],
                "time_start": "2022-01-01T00:00:00Z",
                "time_end": "2022-02-01T00:00:00Z"
            }
        ]
    }

    mock.league_details.return_value = {
        "payload": {
            "description": "Test League Description",
            "organizer_details": {
                "avatar_url": "http://example.com/avatar.png",
                "cover_url": "http://example.com/banner.png",
                "id": "org1",
                "name": "TestOrg"
            }
        }
    }

    mock.league_season_stages.return_value = {
        "payload": {
            "regions": [
                {
                    "id": "region1",
                    "name": "Europe",
                    "divisions": [
                        {
                            "id": "div1",
                            "name": "Main",
                            "stages": [
                                {
                                    "id": "stage1",
                                    "name": "Stage 1",
                                    "conferences": [
                                        {"id": "conf1", "name": "Benelux", "championship_id": "event1"}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }

    mock.league_team_details.return_value = {
        'payload': [
            {
                'league_seasons_info': [
                    {
                        'season_number': '1',
                        'season_id': '2',
                        'league_team_id': '3',
                        'team_members': [
                            {
                                'user_id': '4',
                                'user_name': 'player1',
                                'game_role': 'player',
                                'avatar_img': 'example.com/avatar1.png'},
                            {
                                'user_id': '5',
                                'user_name': 'player2',
                                'game_role': 'substitute',
                                'avatar_img': 'example.com/avatar2.png'},
                            {
                                'user_id': '6',
                                'user_name': 'player3',
                                'game_role': 'coach',
                                'avatar_img': 'example.com/avatar3.png'},
                        ],
                        'season_standings': [
                            {
                                'championship_id': '7',
                                'placement': {'left': 0, 'right': 2},
                                'wins': '1',
                                'losses': '2',
                                'ties': '3'
                            }
                        ]
                    }
                ]
            }
        ]
    }

    mock.league_team_matches.return_value = {
        'payload': {
            'items': [
                {
                    'origin': {
                        'id': 'match1',
                        'schedule': "1000"
                    },
                    'factions': [
                        {
                            'id': 'team1',
                        }
                    ]
                }
            ]
        }
    }
    
    return mock

@pytest.fixture
def mock_faceit_data_v4():
    """Default working mock of FaceitData_v1 with structured payloads."""
    mock = AsyncMock()
    
    mock.team_details.return_value = {
        'team_id': 'team1',
        'team_name': 'Team One',
        'nickname': 'T1',
        'avatar': 'http://example.com/avatar.png',
    }
    
    mock.match_stats.return_value = {
        'rounds': [
            {
                'match_round': '1',
                'best_of': '3',
                'round_stats': {
                    'Score': '16-14',
                    'Map': 'de_dust2'
                },
                'teams': [
                    {
                        'team_id': 'team1',
                        'team_stats': {
                            'Team': 'Team One',
                            'Final Score': '16'
                        },
                        'players': [
                            {
                                'player_id': 'player1',
                                'nickname': 'Player One',
                                'player_stats': {
                                    'Kills': 20
                                }
                            }
                        ]  
                    }
                ]
            }
        ]
    }
    
    mock.match_details.return_value = {
        'competition_id': 'event1',
        'competition_type': 'league',
        'competition_name': 'Test League',
        'organizer_id': 'org1',
        'organizer_name': 'Test Organizer',
        'best_of': '3',
        'round': '1',
        'group': '3',
        'demo_url': 'http://example.com/demo',
        'status': 'FINISHED',
        'scheduled_at': '100',
        'configured_at': '50',
        'started_at': '150',
        'results': {
            'winner': 'team1'
        },
        'teams': {
            'team1': {
                'faction_id': 'team1',
                'name': 'Team One',
                'avatar': 'http://example.com/avatar.png',
            }        
        }
    }    
        
    return mock

@pytest.fixture
def make_mock_with_status():
    """Factory for mocks that return a raw HTTP status code (int) instead of a payload."""
    def _make(status_code: int):
        mock = AsyncMock()
        
        # v1 api
        mock.league_seasons.return_value = status_code
        mock.league_details.return_value = status_code
        mock.league_season_stages.return_value = status_code
        mock.league_team_details.return_value = status_code
        mock.league_team_matches.return_value = status_code
        mock.player_details_batch.return_value = status_code
        
        # v4 api
        mock.team_details.return_value = status_code
        mock.match_stats.return_value = status_code
        mock.match_details.return_value = status_code
        
        return mock
    return _make

@pytest.fixture
def rate_limit_mock():
    """Mock that simulates a rate limit error when calling league_seasons."""
    mock = AsyncMock()
    
    # v1 api
    mock.league_seasons.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    mock.league_details.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    mock.league_team_details.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    mock.league_team_matches.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    mock.player_details_batch.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    
    # v4 api
    mock.team_details.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    mock.match_stats.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    mock.match_details.side_effect = sliding_window.RateLimitException("Mock Rate limit reached")
    
    return mock

@pytest.fixture
def invalid_format_mock():
    """Mock that returns non-dictionary values to simulate corrupted API response."""
    mock = AsyncMock()
    
    # v1 api
    mock.league_seasons.return_value = "not-a-dict"
    mock.league_details.return_value = "not-a-dict"
    mock.league_team_details.return_value = "not-a-dict"
    mock.league_team_matches.return_value = "not-a-dict"
    mock.player_details_batch.return_value = "not-a-dict"
    
    # v4 api
    mock.team_details.return_value = "not-a-dict"
    mock.match_stats.return_value = "not-a-dict"
    mock.match_details.return_value = "not-a-dict"
    
    return mock

@pytest.fixture
def empty_response_mock():
    """Mock that simulates valid API response structure with empty payloads."""
    mock = AsyncMock()
    
    # v1 api
    mock.league_seasons.return_value = {'payload': []}
    mock.league_details.return_value = {'payload': {}}
    mock.league_season_stages.return_value = {'payload': {}}
    mock.league_team_details.return_value = {'payload': []}
    mock.league_team_matches.return_value = {'payload': {'items': []}}
    mock.player_details_batch.return_value = {}
    
    # v4 api
    mock.team_details.return_value = {}
    mock.match_stats.return_value = {'rounds': []}
    mock.match_details.return_value = {}
    
    return mock

@pytest.fixture
def none_response_mock():
    """Mock that returns None to simulate a totally missing API response."""
    mock = AsyncMock()
    
    # v1 api
    mock.league_seasons.return_value = None
    mock.league_details.return_value = None
    mock.league_season_stages.return_value = None
    mock.league_team_details.return_value = None
    mock.league_team_matches.return_value = None
    mock.player_details_batch.return_value = None
    
    # v4 api
    mock.team_details.return_value = None
    mock.match_stats.return_value = None
    mock.match_details.return_value = None
    
    return mock


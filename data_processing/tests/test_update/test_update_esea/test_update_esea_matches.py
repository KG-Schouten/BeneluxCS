# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import patch, AsyncMock

from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.sliding_window import RequestDispatcher

@pytest.mark.asyncio
@patch("update.gather_internal_event_ids")
@patch("update.process_matches")
@patch("update.gather_esea_matches")
@patch("update.gather_last_match_time_database")
@patch("update.gather_event_teams")
@patch("data_processing.faceit_api.faceit_v1.FaceitData_v1", autospec=True)
@patch("data_processing.faceit_api.faceit_v4.FaceitData", autospec=True)
@patch("data_processing.faceit_api.sliding_window.RequestDispatcher", autospec=True)
async def test_update_esea_matches(
    mock_dispatcher_cls,
    mock_faceitdata_cls,
    mock_faceitdata_v1_cls,
    mock_gather_event_teams,
    mock_gather_last_match_time_database,
    mock_gather_esea_matches,
    mock_process_matches,
    mock_gather_internal_event_ids,
):
    # Setup mock dispatcher and FaceitData classes
    mock_dispatcher_cls.return_value.__aenter__.return_value = AsyncMock()
    mock_faceitdata_cls.return_value.__aenter__.return_value = AsyncMock()
    mock_faceitdata_v1_cls.return_value.__aenter__.return_value = AsyncMock()
    
    # Setup return values
    mock_gather_event_teams.return_value = pd.DataFrame(
        {'team_id': ['team1', 'team2'], 'event_id': ['event1', 'event2']}
    )
    mock_gather_last_match_time_database.return_value = 10
    mock_gather_esea_matches.return_value = pd.DataFrame(
        {
            'match_id': ['match1', 'match2'],
            'event_id': ['event1', 'event2'],
        }
    )
    def mock_process_matches_func():
        df_matches = pd.DataFrame(
            {
                'match_id': ['match1', 'match2'],
                'event_id': ['event1', 'event2'],
            }
        )
        df_teams_matches = pd.DataFrame(
            {'match_id': ['match1', 'match2'], 'team_id': ['team1', 'team2']}
        )
        df_teams = pd.DataFrame({'team_id': ['team1', 'team2']})
        df_maps = pd.DataFrame({'map_id': ['map1', 'map2']})
        df_maps_teams = pd.DataFrame({'map_id': ['map1', 'map2'], 'team_id': ['team1', 'team2']})
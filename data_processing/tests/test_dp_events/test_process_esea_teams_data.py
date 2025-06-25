# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import AsyncMock, patch

from data_processing import dp_events
from data_processing.faceit_api.faceit_v1 import FaceitData_v1
from data_processing.faceit_api.faceit_v4 import FaceitData
from data_processing.faceit_api.sliding_window import RequestDispatcher

@pytest.mark.asyncio
@patch('data_processing.dp_events.process_matches')
@patch('data_processing.dp_events.gather_esea_matches')
@patch('data_processing.dp_events.process_teams_benelux_esea')
@patch('data_processing.dp_events.process_esea_season_data')
@patch("data_processing.faceit_api.faceit_v1.FaceitData_v1", autospec=True)
@patch("data_processing.faceit_api.faceit_v4.FaceitData", autospec=True)
@patch("data_processing.faceit_api.sliding_window.RequestDispatcher", autospec=True)
async def test_process_esea_teams_data(
    mock_dispatcher_cls,
    mock_faceitdata_cls,
    mock_faceitdata_v1_cls,
    mock_process_season,
    mock_process_teams, 
    mock_gather_matches,
    mock_process_matches, 
    ):

    # Setup mock dispatcher and FaceitData classes
    mock_dispatcher_cls.return_value.__aenter__.return_value = AsyncMock()
    mock_faceitdata_cls.return_value.__aenter__.return_value = AsyncMock()
    mock_faceitdata_v1_cls.return_value.__aenter__.return_value = AsyncMock()
    
    # Setup return values
    mock_process_season.return_value = (
        pd.DataFrame({'season_id': [1]}),
        pd.DataFrame({'event_id': ['e1'], 'stage_id': ['s1']})
    )
    mock_process_teams.return_value = pd.DataFrame(
        {'team_id': ['t1', 't2'], 'event_id': ['e1', 'e1'],}
    )
    mock_gather_matches.return_value = pd.DataFrame(
        {'match_id': ['m1', 'm2', 'm3', 'm4'], 'event_id': ['e1', 'e1', 'e2', 'e2']},
    )
    mock_process_matches.return_value = (
        pd.DataFrame(
            {'match_id': ['m1', 'm2'], 'event_id': ['e1', 'e2']},
        ),
        pd.DataFrame({'match_id': ['m1'], 'team_id': ['t1']}),
        pd.DataFrame({'team_id': ['t1']}),
        pd.DataFrame({'map_id': ['map1']}),
        pd.DataFrame({'map_id': ['map1'], 'team_id': ['t1']}),
        pd.DataFrame({'player_id': ['p1'], 'match_id': ['m1']}),
        pd.DataFrame({'player_id': ['p1']}),
    )
    
    result = await dp_events.process_esea_teams_data()
    assert isinstance(result, tuple)
    assert len(result) == 10
    for df in result:
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
   
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("df_esea_matches", "df_matches", "error_type", "error_message"),
    [
        (pd.DataFrame(), pd.DataFrame(), None, ""),
        (pd.DataFrame({'match_id': ['m1']}), pd.DataFrame(), ValueError, "does not contain the required columns"),
        (pd.DataFrame({'event_id': ['e1']}), pd.DataFrame(), ValueError, "does not contain the required columns"),
        (pd.DataFrame({'match_id': ['m1'], 'event_id': ['e1']}), pd.DataFrame(), None, ""),
        (pd.DataFrame({'match_id': ['m1'], 'event_id': ['e1']}), pd.DataFrame({'match_id': ['m2']}), ValueError, "does not contain the required columns"),
    ]
)
@patch("data_processing.dp_events.process_esea_season_data")
@patch("data_processing.dp_events.process_teams_benelux_esea")
@patch("data_processing.dp_events.gather_esea_matches")
@patch("data_processing.dp_events.process_matches")
@patch("data_processing.faceit_api.faceit_v1.FaceitData_v1", autospec=True)
@patch("data_processing.faceit_api.faceit_v4.FaceitData", autospec=True)
@patch("data_processing.faceit_api.sliding_window.RequestDispatcher", autospec=True)
async def test_process_esea_teams_data_empty_handling(
    mock_dispatcher_cls,
    mock_faceitdata_cls,
    mock_faceitdata_v1_cls,
    mock_process_matches,
    mock_gather_matches,
    mock_process_teams,
    mock_process_season,
    df_esea_matches,
    df_matches,
    error_type,
    error_message
):
    mock_dispatcher_cls.return_value.__aenter__.return_value = AsyncMock()
    mock_faceitdata_cls.return_value.__aenter__.return_value = AsyncMock()
    mock_faceitdata_v1_cls.return_value.__aenter__.return_value = AsyncMock()

    mock_process_season.return_value = (
        pd.DataFrame({'season_id': [1, 2, 3]}),
        pd.DataFrame({'event_id': ['e1'], 'stage_id': ['s1']})
    )
    mock_process_teams.return_value = pd.DataFrame({
        'team_id': ['t1', 't2'],
        'event_id': ['e1', 'e1'],
    })

    mock_gather_matches.return_value = df_esea_matches
    mock_process_matches.return_value = (
        df_matches,
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame()
    )

    if error_type:
        with pytest.raises(error_type, match=error_message):
            await dp_events.process_esea_teams_data()
    else:
        result = await dp_events.process_esea_teams_data()

        assert isinstance(result, tuple)
        assert len(result) == 10

        for df in result:
            assert isinstance(df, pd.DataFrame)
# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch

from data_processing import dp_events

@pytest.mark.asyncio
@patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x)
@patch('data_processing.dp_events.read_league_teams_json')
@patch('data_processing.dp_events.process_league_team_season_standings')
async def test_process_teams_benelux_esea(mock_standings, mock_read_json, mock_modify_keys, mock_faceit_data_v1):
    # Setup the mocks
    mock_read_json.return_value = pd.DataFrame(
        [
            {'team_id': 'team1', 'season_number': 1},
            {'team_id': 'team2', 'season_number': 1}
        ]
    )
    
    async def mock_standings_func(team_id, _):
        return [
            {"team_id": team_id, "season_number": 1, "wins": 10}
        ]
    mock_standings.side_effect = mock_standings_func
    
    df_results = await dp_events.process_teams_benelux_esea(mock_faceit_data_v1)
    
    assert isinstance(df_results, pd.DataFrame)
    assert not df_results.empty
    assert 'team_id' in df_results.columns
    assert 'wins' in df_results.columns
    assert 'season_number' not in df_results.columns

@pytest.mark.asyncio
@patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x)
@patch('data_processing.dp_events.read_league_teams_json')
@patch('data_processing.dp_events.process_league_team_season_standings')
async def test_process_teams_benelux_esea_empty_teams(mock_standings, mock_read_json, mock_modify_keys, mock_faceit_data_v1):
    mock_read_json.return_value = pd.DataFrame()

    with pytest.raises(ValueError, match="No teams found in the Benelux json"):
        await dp_events.process_teams_benelux_esea(mock_faceit_data_v1)

@pytest.mark.asyncio
@patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x)
@patch('data_processing.dp_events.read_league_teams_json')
@patch('data_processing.dp_events.process_league_team_season_standings')
async def test_process_teams_benelux_esea_invalid_json_output(mock_standings, mock_read_json, mock_modify_keys, mock_faceit_data_v1):
    mock_read_json.return_value = "not a dataframe"

    with pytest.raises(TypeError, match="df_teams_benelux is not a DataFrame"):
        await dp_events.process_teams_benelux_esea(mock_faceit_data_v1)

@pytest.mark.asyncio
@patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x)
@patch('data_processing.dp_events.read_league_teams_json')
@patch('data_processing.dp_events.process_league_team_season_standings')
async def test_process_teams_benelux_esea_invalid_standings_data(mock_standings, mock_read_json, mock_modify_keys, mock_faceit_data_v1):
    mock_read_json.return_value = pd.DataFrame([{"team_id": "team1", "season_number": 1}])
    async def mock_standings_func(team_id, _):
        return None
    mock_standings.side_effect = mock_standings_func

    with pytest.raises(ValueError, match="All league team season standings are None"):
        await dp_events.process_teams_benelux_esea(mock_faceit_data_v1)

@pytest.mark.asyncio
@patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x)
@patch('data_processing.dp_events.read_league_teams_json')
@patch('data_processing.dp_events.process_league_team_season_standings')
async def test_process_teams_benelux_esea_empty_standings(mock_standings, mock_read_json, mock_modify_keys, mock_faceit_data_v1):
    mock_read_json.return_value = pd.DataFrame([{"team_id": "team1", "season_number": 1}])
    async def mock_standings_func(team_id, _):
        return []
    mock_standings.side_effect = mock_standings_func

    with pytest.raises(ValueError, match="All league team season standings are empty lists"):
        await dp_events.process_teams_benelux_esea(mock_faceit_data_v1)

@pytest.mark.asyncio
@patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x)
@patch('data_processing.dp_events.read_league_teams_json')
@patch('data_processing.dp_events.process_league_team_season_standings')
async def test_process_teams_benelux_esea_merge_empty_result(mock_standings, mock_read_json, mock_modify_keys, mock_faceit_data_v1):
    mock_read_json.return_value = pd.DataFrame([{"team_id": "team1", "season_number": 1}])
    async def mock_standings_func(team_id, _):
        return [
            {"team_id": 'team2', "season_number": 2, "wins": 5}
        ]
    mock_standings.side_effect = mock_standings_func

    df_result = await dp_events.process_teams_benelux_esea(mock_faceit_data_v1)
    
    # Merge should be empty
    assert df_result.empty
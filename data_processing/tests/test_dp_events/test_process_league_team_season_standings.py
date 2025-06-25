# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest

from data_processing import dp_events


# --- process_league_team_season_standings tests ---
@pytest.mark.asyncio
async def test_process_league_team_season_standings(mock_faceit_data_v1):
    team_id = "team1"
    data_list = await dp_events.process_league_team_season_standings(team_id, mock_faceit_data_v1)

    assert isinstance(data_list, list)
    assert data_list  # Should not be empty

    expected_keys = {"team_id", "event_id", "season_number", "placement", "wins", "losses", "ties",
                     "players_main", "players_sub", "players_coach"}
    for item in data_list:
        assert isinstance(item, dict)
        assert expected_keys.issubset(item.keys()), f"Missing keys in item: {item}"

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_fixture_name",
    [
        "invalid_format_mock",
        "none_response_mock",
        "empty_response_mock",
        "rate_limit_mock",
        "make_mock_with_status"
    ]
)
async def test_process_league_team_season_standings_invalid_api_responses(
    mock_fixture_name, request
):
    team_id = "team1"
    
    # For 'make_mock_with_status', we need to pass the status code as an argument
    if mock_fixture_name == "make_mock_with_status":
        mock = request.getfixturevalue(mock_fixture_name)(404)
    else:
        mock = request.getfixturevalue(mock_fixture_name)

    result = await dp_events.process_league_team_season_standings(
        team_id=team_id, 
        faceit_data_v1=mock)
    
    assert result == []  # Expecting an empty list for invalid responses
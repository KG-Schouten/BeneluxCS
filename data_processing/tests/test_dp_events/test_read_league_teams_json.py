# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch, mock_open
import json

from data_processing import dp_events

# --- read_league_teams_json tests ---
sample_league_teams_json_data = {
    "seasons": [
        {
            "season_number": "1",
            "teams": {
                "Team Alpha": "team1",
                "Team Beta": "team2"
            }
        },
        {
            "season_number": "2",
            "teams": {
                "Team Gamma": "team3"
            }
        }
    ]
}

@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_returns_all_teams(mock_json_load, mock_open):
    mock_json_load.return_value = sample_league_teams_json_data

    df = dp_events.read_league_teams_json()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert set(df["team_id"]) == {"team1", "team2", "team3"}
    assert set(df["season_number"]) == {"1", "2"}

@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_filter_by_single_season(mock_json_load, mock_open):
    mock_json_load.return_value = sample_league_teams_json_data

    df =  dp_events.read_league_teams_json(season_number="1")

    assert len(df) == 2
    assert set(df["team_id"]) == {"team1", "team2"}
    assert all(df["season_number"] == "1")
    
@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_filter_by_list_of_seasons(mock_json_load, mock_open):
    mock_json_load.return_value = sample_league_teams_json_data

    df = dp_events.read_league_teams_json(season_number=["1", "2"])

    assert len(df) == 3

@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_filter_by_team_id(mock_json_load, mock_open):
    mock_json_load.return_value = sample_league_teams_json_data

    df = dp_events.read_league_teams_json(team_id="team3")

    assert len(df) == 1
    assert df.iloc[0]["team_id"] == "team3"
    assert df.iloc[0]["team_name"] == "Team Gamma"

@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_filter_by_multiple_team_ids(mock_json_load, mock_open):
    mock_json_load.return_value = sample_league_teams_json_data

    df = dp_events.read_league_teams_json(team_id=["team1", "team3"])

    assert len(df) == 2
    assert set(df["team_id"]) == {"team1", "team3"}

@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_invalid_team_id_raises(mock_json_load, mock_open):
    mock_json_load.return_value = sample_league_teams_json_data

    with pytest.raises(ValueError, match="Invalid team_id values: .*"):
        dp_events.read_league_teams_json(team_id="nonexistent_id")

@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_invalid_season_number_raises(mock_json_load, mock_open):
    mock_json_load.return_value = sample_league_teams_json_data

    with pytest.raises(ValueError, match="Invalid season_number values: .*"):
        dp_events.read_league_teams_json(season_number="999")

@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_json_decode_error(mock_json_load, mock_open):
    mock_json_load.side_effect = json.JSONDecodeError("Expecting value", "doc", 0)

    with pytest.raises(json.JSONDecodeError):
        dp_events.read_league_teams_json()

@patch("data_processing.dp_events.function_logger")
@patch("data_processing.dp_events.open", new_callable=mock_open)
@patch("data_processing.dp_events.json.load")
def test_logger_called_on_error(mock_json_load, mock_open, mock_logger):
    mock_json_load.side_effect = Exception("Something bad")

    with pytest.raises(Exception):
        dp_events.read_league_teams_json()

    assert mock_logger.critical.called
    call_args = mock_logger.critical.call_args[0][0]
    assert "Error gathering team IDs" in call_args
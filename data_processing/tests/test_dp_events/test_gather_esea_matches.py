# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch

from data_processing import dp_events

@pytest.mark.asyncio
@patch('data_processing.dp_events.filter_esea_matches')
@patch('data_processing.dp_events.gather_esea_matches_team')
async def test_gather_esea_matches(mock_gather_matches_team, mock_filter_matches, mock_faceit_data_v1):
    team_ids = ["team1", "team2"]
    event_ids = ["event1", ["event2", "event3"]]
    
    async def mock_gather_matches_team_func(team_id, event_id, mock_faceit_data_v1):
        return [
            {
                "match_id": f"{team_id}_{event_id}_match1",
                "team_id": team_id, 
                "event_id": event_id,
                "match_time": 10
            },
            {
                "match_id": f"{team_id}_{event_id}_match2",
                "team_id": team_id, 
                "event_id": event_id,
                "match_time": 20
            }
        ]
    mock_gather_matches_team.side_effect = mock_gather_matches_team_func
    
    # Output is equal to input
    mock_filter_matches.side_effect = lambda df, df_events, *args, **kwargs: df
    
    df_esea_matches = await dp_events.gather_esea_matches(
        team_ids=team_ids,
        event_ids=event_ids,
        faceit_data_v1=mock_faceit_data_v1)
    
    assert isinstance(df_esea_matches, pd.DataFrame)
    assert not df_esea_matches.empty
    assert "match_id" in df_esea_matches.columns
    assert "team_id" in df_esea_matches.columns
    assert "event_id" in df_esea_matches.columns
    assert "match_time" in df_esea_matches.columns

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "team_ids, event_ids", 
    [
        ([], ["event1"]), # empty team_ids
        (['team1'], []), # empty event_ids
        ([], []), # empty team_ids and event_ids
        ("team1", ["event1"]), # invalid type: single team_id as string
        (["team1"], "event1"), # invalid type: single event_id as string
        ([['team1']], ["event1"]), # invalid type: team_ids as nested list
        (["team1"], ["event1", ["event2", 10]]), # invalid type: event_ids with mixed types
    ],
)
@patch('data_processing.dp_events.filter_esea_matches')
@patch('data_processing.dp_events.gather_esea_matches_team')
async def test_gather_esea_matches_invalid_inputs(
    mock_gather_matches_team, mock_filter_matches, mock_faceit_data_v1, team_ids, event_ids
    ):
    
    df_esea_matches = await dp_events.gather_esea_matches(
        team_ids=team_ids,
        event_ids=event_ids,
        faceit_data_v1=mock_faceit_data_v1)
    
    assert isinstance(df_esea_matches, pd.DataFrame)
    assert df_esea_matches.empty

@pytest.mark.asyncio
@patch('data_processing.dp_events.filter_esea_matches')
@patch('data_processing.dp_events.gather_esea_matches_team')
async def test_gather_esea_matches_empty_gather_matches_team(
    mock_gather_matches_team, mock_filter_matches, mock_faceit_data_v1
    ):
    team_ids = ["team1"]
    event_ids = ["event1"]
    
    async def mock_gather_matches_team_func(team_id, event_id):
        return []
    mock_gather_matches_team.side_effect = mock_gather_matches_team_func

    df_teams_matches = await dp_events.gather_esea_matches(
        team_ids=team_ids,
        event_ids=event_ids,
        faceit_data_v1=mock_faceit_data_v1)
    
    assert isinstance(df_teams_matches, pd.DataFrame)
    assert df_teams_matches.empty
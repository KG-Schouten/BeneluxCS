# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch

from data_processing import dp_general

# --- test process_match_details_batch ---
@pytest.mark.asyncio
@patch("data_processing.dp_general.modify_keys", side_effect=lambda x: x)
@patch("data_processing.dp_general.process_match_details")
async def test_process_match_details_batch(mock_process_match_details, mock_modify_keys, mock_faceit_data_v4):
    async def mock_process_match_details_func(match_id, event_id, faceit_data=None):
        return (
            {'match_id': match_id, 'event_id': event_id},
            [{'team_id': 'team1', 'match_id': match_id, 'event_id': event_id}]
        )
    mock_process_match_details.side_effect = mock_process_match_details_func
    
    df_matches, df_teams_matches = await dp_general.process_match_details_batch(
        match_ids=["match1", "match2", "match3"],
        event_ids=["event1", "event2", "event3"],
        faceit_data=mock_faceit_data_v4)
    
    # df_matches asserts
    assert isinstance(df_matches, pd.DataFrame)
    assert not df_matches.empty
    assert set(df_matches.columns) == {"match_id", "event_id"}
    
    # df_teams_matches asserts
    assert isinstance(df_teams_matches, pd.DataFrame)
    assert not df_teams_matches.empty
    assert set(df_teams_matches.columns) == {"team_id", "match_id", "event_id"}
    
@pytest.mark.asyncio
@patch("data_processing.dp_general.modify_keys")
@patch("data_processing.dp_general.process_match_details")
async def test_process_match_details_batch_empty_data(mock_process_match_details, mock_modify_keys, mock_faceit_data_v4):
    async def mock_process_match_details_func(match_id, event_id, faceit_data=None):
        return (
            {},
            []
        )
    mock_process_match_details.side_effect = mock_process_match_details_func

    df_matches, df_teams_matches = await dp_general.process_match_details_batch(
        match_ids=["match1", "match2", "match3"],
        event_ids=["event1", "event2", "event3"],
        faceit_data=mock_faceit_data_v4)

    # df_matches asserts
    assert isinstance(df_matches, pd.DataFrame)
    assert df_matches.empty

# --- test process_match_details ---
@pytest.mark.asyncio
async def test_process_match_details(mock_faceit_data_v4):
    match_id = "match123"
    event_id = "event456"
    
    match_dict, match_team_list = await dp_general.process_match_details(
        match_id=match_id,
        event_id=event_id,
        faceit_data=mock_faceit_data_v4)
    
    # match_dict asserts
    assert isinstance(match_dict, dict)
    assert match_dict.get("match_id") == match_id
    assert match_dict.get("event_id") == event_id
    assert match_dict
    
    # match_team_list asserts
    assert isinstance(match_team_list, list)
    assert match_team_list  # Should not be empty

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mock_name"),
    [
        ("invalid_format_mock"),
        ("none_response_mock"),
        ("empty_response_mock"),
        ("rate_limit_mock"),
        ("make_mock_with_status")
    ]
)
async def test_process_match_details_invalid_api_responses(mock_name, request):
    if mock_name == "make_mock_with_status":
        mock = request.getfixturevalue(mock_name)(404)
    else:
        mock = request.getfixturevalue(mock_name)
    
    result = await dp_general.process_match_details(
        match_id="match123",
        event_id="event456",
        faceit_data=mock
    )
    
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert all(isinstance(item, (dict, list)) for item in result)
    assert all(not item for item in result)  # Both should be empty
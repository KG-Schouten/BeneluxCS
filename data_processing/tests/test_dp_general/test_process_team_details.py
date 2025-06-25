# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch

from data_processing import dp_general

# --- test process_team_details_batch ---
@pytest.mark.asyncio
@patch("data_processing.dp_general.modify_keys", side_effect=lambda x: x)
@patch("data_processing.dp_general.process_team_details")
async def test_process_team_details_batch(mock_process_team_details, mock_modify_keys, mock_faceit_data_v4):
    team_ids = ["team1", "team2", "team3"]

    async def mock_process_team_details_func(team_id, faceit_data=None):
        return {
            "team_id": team_id,
            "team_name": f"Team {team_id}",
            "nickname": f"T{team_id[-1]}",
            "avatar": f"avatar{team_id[-1]}.png",
        }

    mock_process_team_details.side_effect = mock_process_team_details_func

    result = await dp_general.process_team_details_batch(team_ids=team_ids, faceit_data=mock_faceit_data_v4)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert set(result.columns) == {"team_id", "team_name", "nickname", "avatar"}

@pytest.mark.asyncio
@patch("data_processing.dp_general.modify_keys")
@patch("data_processing.dp_general.process_team_details")
async def test_process_team_details_batch_empty_data(mock_process_team_details, mock_modify_keys, mock_faceit_data_v4):
    team_ids = ["team1", "team2"]
    
    async def mock_process_team_details_func(team_id):
        return {}
    mock_process_team_details.side_effect = mock_process_team_details_func

    result = await dp_general.process_team_details_batch(team_ids=team_ids, faceit_data=mock_faceit_data_v4)
    
    assert isinstance(result, pd.DataFrame)
    assert result.empty  # Should be empty since all details are empty

# --- test process_team_details ---
@pytest.mark.asyncio
async def test_process_team_details(mock_faceit_data_v4):
    team_id = "team1"
    
    result = await dp_general.process_team_details(team_id=team_id, faceit_data=mock_faceit_data_v4)

    assert isinstance(result, dict)
    assert 'team_id' in result
    assert result['team_id'] == team_id
    assert 'team_name' in result
    assert 'nickname' in result
    assert 'avatar' in result

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
async def test_process_team_details_invalid_api_responses(
    mock_name, request
):
    if mock_name == "make_mock_with_status":
        mock = request.getfixturevalue(mock_name)(404)
    else:
        mock = request.getfixturevalue(mock_name)

    result = await dp_general.process_team_details(team_id="team1", faceit_data=mock)
    
    assert isinstance(result, dict)
    assert not result  # Should be empty



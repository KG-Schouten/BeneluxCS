# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch

from data_processing import dp_general

# --- test process_player_details_batch ---
@pytest.mark.asyncio
@patch("data_processing.dp_general.modify_keys", side_effect=lambda x: x)
@patch("data_processing.dp_general.process_player_details")
async def test_process_player_details_batch(mock_process_player_details, mock_modify_keys, mock_faceit_data_v1):
    player_ids = ["player1", "player2", "player3"]

    async def mock_process_player_details_func(player_ids_batch, faceit_data_v1=None):
        return [
            {'player_id': player_id}
            for player_id in player_ids_batch
        ]

    mock_process_player_details.side_effect = mock_process_player_details_func

    result = await dp_general.process_player_details_batch(player_ids=player_ids, faceit_data_v1=mock_faceit_data_v1)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert set(result.columns) == {"player_id"}

@pytest.mark.asyncio
@patch("data_processing.dp_general.modify_keys")
@patch("data_processing.dp_general.process_player_details")
async def test_process_player_details_batch_empty_data(mock_process_player_details, mock_modify_keys, mock_faceit_data_v1):
    player_ids = ["player1", "player2", "player3"]
    
    async def mock_process_player_details_func(player_ids_batch, faceit_data_v1=None):
        return [
            {}
            for _ in player_ids_batch
        ]
    mock_process_player_details.side_effect = mock_process_player_details_func

    result = await dp_general.process_player_details_batch(player_ids=player_ids, faceit_data_v1=mock_faceit_data_v1)
    
    assert isinstance(result, pd.DataFrame)
    assert result.empty  # Should be empty since all details are empty
    
# --- test process_player_details ---
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "player_ids",
    [
        "player4",
        ["player1"],
        ["player2", "player3"]
    ]
)
async def test_process_player_details(mock_faceit_data_v1, player_ids):
    async def mock_player_details_func(ids):
        # Normalize to list for uniform handling
        ids = [ids] if isinstance(ids, str) else ids
        return {
            'payload': {
                player_id: {
                    'player_id': player_id,
                    'nickname': f'Player{player_id[-1]}',
                    'avatar': f'avatar{player_id[-1]}.png',
                    'country': 'CountryX'
                }
                for player_id in ids
            }
        }

    mock_faceit_data_v1.player_details_batch.side_effect = mock_player_details_func

    result = await dp_general.process_player_details(player_ids=player_ids, faceit_data_v1=mock_faceit_data_v1)

    assert isinstance(result, list)
    expected_len = 1 if isinstance(player_ids, str) else len(player_ids)
    assert len(result) == expected_len

    ids_list = [player_ids] if isinstance(player_ids, str) else player_ids
    for player in result:
        assert isinstance(player, dict)
        assert 'player_id' in player and player['player_id'] in ids_list

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "player_ids",
    [
        [],
        {},
        None
    ]
)
async def test_process_player_details_invalid_input(mock_faceit_data_v1, player_ids):
    
    result = await dp_general.process_player_details(player_ids=player_ids, faceit_data_v1=mock_faceit_data_v1)
    
    assert isinstance(result, list)
    assert not result

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
async def test_process_player_details_invalid_api_responses(mock_name, request):
    if mock_name == "make_mock_with_status":
        mock = request.getfixturevalue(mock_name)(404)
    else:
        mock = request.getfixturevalue(mock_name)
        
    result = await dp_general.process_player_details(player_ids=["player1"], faceit_data_v1=mock)
    
    assert isinstance(result, list)
    assert not result
    
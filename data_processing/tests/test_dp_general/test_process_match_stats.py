# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch

from data_processing import dp_general

# --- test process_match_stats_batch ---
@pytest.mark.asyncio
@patch("data_processing.dp_general.calculate_hltv", side_effect=lambda x, y: x)
@patch("data_processing.dp_general.modify_keys", side_effect=lambda x: x)
@patch("data_processing.dp_general.process_match_stats")
async def test_process_match_stats_batch(
    mock_process_match_stats, mock_modify_keys, mock_calculate_hltv, mock_faceit_data_v4):
    match_ids = ["match1", "match2", "match3"]
    
    async def mock_process_match_stats_func(match_id, faceit_data):
        return (
            [{'match_id': match_id, 'map': 'de_dust2', 'score': '16-14'}],
            [{'match_id': match_id, 'team_id': 'team1', 'final score': '16-14'}],
            [{'player_id': 'player1', 'kills': 20, 'deaths': 10}]
        )
    mock_process_match_stats.side_effect = mock_process_match_stats_func
    
    result = await dp_general.process_match_stats_batch(
        match_ids=match_ids,
        faceit_data=mock_faceit_data_v4
    )
    
    assert isinstance(result, tuple)
    assert len(result) == 3
    assert all(isinstance(item, pd.DataFrame) for item in result)
    assert all(not item.empty for item in result)  # All DataFrames should not be empty

@pytest.mark.asyncio
@patch("data_processing.dp_general.calculate_hltv", side_effect=lambda x, y: x)
@patch("data_processing.dp_general.modify_keys", side_effect=lambda x: x)
@patch("data_processing.dp_general.process_match_stats")
async def test_process_match_stats_batch_empty_data(
    mock_process_match_stats, mock_modify_keys, mock_calculate_hltv, mock_faceit_data_v4):
    match_ids = ["match1", "match2", "match3"]
    async def mock_process_match_stats_func(match_id, faceit_data):
        return (
            [],
            [],
            []
        )
    mock_process_match_stats.side_effect = mock_process_match_stats_func
    result = await dp_general.process_match_stats_batch(
        match_ids=match_ids,
        faceit_data=mock_faceit_data_v4)
    
    assert isinstance(result, tuple)
    assert len(result) == 3
    assert all(isinstance(item, pd.DataFrame) for item in result)
    assert all(item.empty for item in result)  # All DataFrames should be empty
    
# --- test process_match_stats ---
@pytest.mark.asyncio
@patch("data_processing.dp_general.string_to_number", side_effect=lambda x: x)
async def test_process_match_stats(mock_string_to_number, mock_faceit_data_v4):
    match_id =  "match123"
    map_list, team_map_list, player_stats_list = await dp_general.process_match_stats(
        match_id=match_id,
        faceit_data=mock_faceit_data_v4)
    
    # Map list asserts
    assert isinstance(map_list, list)
    assert len(map_list) == 1
    assert isinstance(map_list[0], dict)
    assert set(map_list[0].keys()) == {'match_id', 'match_round', 'best_of', 'map', 'score'}
    assert map_list[0]['match_id'] == match_id
    
    # Team map list asserts
    assert isinstance(team_map_list, list)
    assert len(team_map_list) == 1
    assert isinstance(team_map_list[0], dict)
    assert set(team_map_list[0].keys()) == {'match_id', 'match_round', 'team_id', 'team', 'final score'}
    assert team_map_list[0]['match_id'] == match_id
    
    # Player stats list asserts
    assert isinstance(player_stats_list, list)
    assert len(player_stats_list) == 1
    assert isinstance(player_stats_list[0], dict)
    assert set(player_stats_list[0].keys()) == {
        'player_id', 'player_name', 'team_id', 'match_id', 'match_round', 'kills'
    }
    
@pytest.mark.asyncio
@patch("data_processing.dp_general.string_to_number", side_effect=lambda x: x)
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
async def test_process_match_stats_invalid_api_responses(
    mock_string_to_number, mock_name, request):
    if mock_name == "make_mock_with_status":
        mock = request.getfixturevalue(mock_name)(404)
    else:
        mock = request.getfixturevalue(mock_name)
    
    result = await dp_general.process_match_stats(
        match_id="match123",
        faceit_data=mock)

    assert isinstance(result, tuple)
    assert len(result) == 3
    assert all(isinstance(item, list) for item in result)
    assert all(not item for item in result)  # All lists should be empty
    
# --- test calculate_hltv ---
def sample_hltv_data():
    df_players_stats = pd.DataFrame([
        {
            'match_id': 'm1',
            'match_round': 1,
            'player_id': 'p1',
            'kills': 10,
            'deaths': 5,
            'double_kills': 2,
            'triple_kills': 1,
            'quadro_kills': 0,
            'penta_kills': 0
        },
        {
            'match_id': 'm1',
            'match_round': 1,
            'player_id': 'p2',
            'kills': 0,
            'deaths': 10,
            'double_kills': 0,
            'triple_kills': 0,
            'quadro_kills': 0,
            'penta_kills': 0
        }
    ])

    df_maps = pd.DataFrame([
        {
            'match_id': 'm1',
            'match_round': 1,
            'rounds': 15
        }
    ])
    return df_players_stats, df_maps
def test_calculate_hltv():
    df_players_stats, df_maps = sample_hltv_data()
    result = dp_general.calculate_hltv(df_players_stats, df_maps)
    
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert 'hltv' in result.columns

def test_calculate_hltv_empty_df_players_stats():
    df_players_stats = pd.DataFrame()
    df_maps = pd.DataFrame({'match_id': ['m1'], 'match_round': [1], 'rounds': [15]})
    result = dp_general.calculate_hltv(df_players_stats, df_maps)
    pd.testing.assert_frame_equal(result, df_players_stats)

def test_calculate_hltv_empty_df_maps():
    df_players_stats = pd.DataFrame([{
        'match_id': 'm1',
        'match_round': 1,
        'player_id': 'p1',
        'kills': 5,
        'deaths': 2,
        'double_kills': 1,
        'triple_kills': 0,
        'quadro_kills': 0,
        'penta_kills': 0
    }])
    df_maps = pd.DataFrame()
    result = dp_general.calculate_hltv(df_players_stats, df_maps)
    pd.testing.assert_frame_equal(result, df_players_stats)

def test_calculate_hltv_missing_columns():
    df_players_stats = pd.DataFrame([{
        'match_id': 'm1',
        'match_round': 1,
        'player_id': 'p1',
        'kills': 5,
        'deaths': 2
    }])
    df_maps = pd.DataFrame([{
        'match_id': 'm1',
        'match_round': 1,
        'rounds': 15
    }])
    
    result = dp_general.calculate_hltv(df_players_stats, df_maps)
    
    assert  isinstance(result, pd.DataFrame)
    assert not result.empty
    pd.testing.assert_frame_equal(result, df_players_stats)

def test_calculate_hltv_no_rounds():
    df_players_stats = pd.DataFrame([{
        'match_id': 'm1',
        'match_round': 1,
        'player_id': 'p1',
        'kills': 5,
        'deaths': 2,
        'double_kills': 1,
        'triple_kills': 0,
        'quadro_kills': 0,
        'penta_kills': 0
    }])
    df_maps = pd.DataFrame([{
        'match_id': 'm1',
        'match_round': 1,
        'rounds': 0 # no rounds
    }])

    result = dp_general.calculate_hltv(df_players_stats, df_maps)

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert 'hltv' in result.columns
    assert np.isnan(result['hltv'].iloc[0]) or result['hltv'].iloc[0] is None

# --- test string_to_number ---
@pytest.mark.parametrize(
    ("value", "output"),
    [
       ("1", 1),
       ("1.5", 1.5),
       ("abc", "abc"),
       ("0", 0),
       ("-1", -1),
       ("-1.5", -1.5),
    ]
)
def test_string_to_number(value, output):
    assert dp_general.string_to_number(value) == output
    

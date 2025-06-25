# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch

from data_processing import dp_general

# --- test process_matches ---
def mock_process_match_details_func(match_ids, event_ids, faceit_data=None):
    df_matches = pd.DataFrame(
            {
                'match_id': match_id,
                'event_id': event_id,
                'status': 'FINISHED'
            }
            for match_id, event_id in zip(match_ids, event_ids)
        )
    df_teams_matches = pd.DataFrame(
            {
                'match_id': match_id,
                'team_id': f'team_{i}',
            }
            for i, match_id in enumerate(match_ids, start=1)
        )
    return  df_matches, df_teams_matches

def mock_process_team_details_func(team_ids, faceit_data=None):
    df_teams = pd.DataFrame(
        {
            'team_id': team_id,
            'team_name': f'team_{team_id}',
        }
        for team_id in team_ids
    )
    return df_teams

def mock_process_match_stats_func(match_ids, faceit_data=None):
    df_maps = pd.DataFrame(
        {
            'match_id': match_id,
            'match_round': 1
        }
        for match_id in match_ids
    )
    df_teams_maps = pd.DataFrame(
        {
            'match_id': match_id,
            'match_round': 1,
            'team_id': f'team_{i}',
        }
        for i, match_id in enumerate(match_ids, start=1)
    )
    df_players_stats = pd.DataFrame(
        {
            'player_id': f'player_{i}',
            'team_id': f'team_{i}',
            'match_id': match_id,
        }
        for i, match_id in enumerate(match_ids, start=1)
    )
    return df_maps, df_teams_maps, df_players_stats

def mock_process_player_details_func(player_ids, faceit_data_v1=None):
    df_players = pd.DataFrame(
        {
            'player_id': player_id,
            'player_name': f'Player{player_id[-1]}',
        }
        for player_id in player_ids
    )
    return df_players

@pytest.mark.asyncio
@patch("data_processing.dp_general.process_player_details_batch")
@patch("data_processing.dp_general.process_match_stats_batch")
@patch("data_processing.dp_general.process_team_details_batch")
@patch("data_processing.dp_general.process_match_details_batch")
async def test_process_matches(
    mock_process_match_details_batch,
    mock_process_team_details_batch,
    mock_process_match_stats_batch,
    mock_process_player_details_batch,
    mock_faceit_data_v1,
    mock_faceit_data_v4
):
    match_ids = ["match1", "match2", "match3"]
    event_ids = ["event1", "event2", "event3"]
    
    mock_process_match_details_batch.side_effect = mock_process_match_details_func
    mock_process_team_details_batch.side_effect = mock_process_team_details_func
    mock_process_match_stats_batch.side_effect = mock_process_match_stats_func
    mock_process_player_details_batch.side_effect = mock_process_player_details_func
    
    df_matches, df_teams_matches, df_teams, df_maps, df_teams_maps, df_players_stats, df_players = await dp_general.process_matches(
        match_ids=match_ids,
        event_ids=event_ids,
        faceit_data=mock_faceit_data_v4,
        faceit_data_v1=mock_faceit_data_v1)
    
    # df_matches assertions
    assert isinstance(df_matches, pd.DataFrame)
    assert not df_matches.empty
    assert set(df_matches.columns) == {"match_id", "event_id", "status"}
    assert set(df_matches['match_id']) == set(match_ids)
    assert set(df_matches['event_id']) == set(event_ids)
    
    # df_teams_matches assertions
    assert isinstance(df_teams_matches, pd.DataFrame)
    assert not df_teams_matches.empty
    assert set(df_teams_matches.columns) == {"match_id", "team_id"}
    assert set(df_teams_matches['match_id']) == set(match_ids)
    
    # df_teams assertions
    assert isinstance(df_teams, pd.DataFrame)
    assert not df_teams.empty
    assert set(df_teams.columns) == {"team_id", "team_name"}
    
    # df_maps assertions
    assert isinstance(df_maps, pd.DataFrame)
    assert not df_maps.empty
    assert set(df_maps.columns) == {"match_id", "match_round"}
    assert set(df_maps['match_id']) == set(match_ids)
    
    # df_teams_maps assertions
    assert isinstance(df_teams_maps, pd.DataFrame)
    assert not df_teams_maps.empty
    assert set(df_teams_maps.columns) == {"match_id", "match_round", "team_id"}
    assert set(df_teams_maps['match_id']) == set(match_ids)
    
    # df_players_stats assertions
    assert isinstance(df_players_stats, pd.DataFrame)
    assert not df_players_stats.empty
    assert set(df_players_stats.columns) == {"player_id", "team_id", "match_id"}
    assert set(df_players_stats['match_id']) == set(match_ids)
    
    # df_players assertions
    assert isinstance(df_players, pd.DataFrame)
    assert not df_players.empty
    assert set(df_players.columns) == {"player_id", "player_name"}
    
@pytest.mark.asyncio
async def test_process_matches_invalid_match_ids_type(mock_faceit_data_v1, mock_faceit_data_v4):
    with pytest.raises(TypeError):
        await dp_general.process_matches(
            match_ids="not_a_list", 
            event_ids=["event1"],
            faceit_data=mock_faceit_data_v4,
            faceit_data_v1=mock_faceit_data_v1,
        )

@pytest.mark.asyncio
async def test_process_matches_invalid_event_ids_type(mock_faceit_data_v1, mock_faceit_data_v4):
    with pytest.raises(TypeError):
        await dp_general.process_matches(
            match_ids=["match1"], 
            event_ids="not_a_list",
            faceit_data=mock_faceit_data_v4,
            faceit_data_v1=mock_faceit_data_v1,
        )

@pytest.mark.asyncio
async def test_process_matches_length_mismatch(mock_faceit_data_v1, mock_faceit_data_v4):
    with pytest.raises(ValueError):
        await dp_general.process_matches(
            match_ids=["match1", "match2"],
            event_ids=["event1"],
            faceit_data=mock_faceit_data_v4,
            faceit_data_v1=mock_faceit_data_v1,
        )

@patch("data_processing.dp_general.process_match_details_batch")
@pytest.mark.asyncio
async def test_process_matches_no_teams(mock_process_match_details_batch, mock_faceit_data_v1, mock_faceit_data_v4):
    # mock match details with no teams
    df_matches = pd.DataFrame({'match_id': ['match1'], 'event_id': ['event1'], 'status': 'FINISHED'})
    df_teams_matches = pd.DataFrame(columns=["match_id", "team_id"])
    mock_process_match_details_batch.return_value = (df_matches, df_teams_matches)

    # test
    results = await dp_general.process_matches(
        match_ids=["match1"],
        event_ids=["event1"],
        faceit_data=mock_faceit_data_v4,
        faceit_data_v1=mock_faceit_data_v1,
    )

    df_matches, df_teams_matches, df_teams, *_ = results
    assert df_teams.empty
    
@patch("data_processing.dp_general.process_match_details_batch")
@pytest.mark.asyncio
async def test_process_matches_no_finished_matches(mock_process_match_details_batch, mock_faceit_data_v1, mock_faceit_data_v4):
    df_matches = pd.DataFrame({'match_id': ['match1'], 'event_id': ['event1'], 'status': 'ONGOING'})
    df_teams_matches = pd.DataFrame({'match_id': ['match1'], 'team_id': ['team_1']})
    mock_process_match_details_batch.return_value = (df_matches, df_teams_matches)

    results = await dp_general.process_matches(
        match_ids=["match1"],
        event_ids=["event1"],
        faceit_data=mock_faceit_data_v4,
        faceit_data_v1=mock_faceit_data_v1,
    )

    *_, df_maps, df_teams_maps, df_players_stats, df_players = results
    assert df_maps.empty
    assert df_teams_maps.empty
    assert df_players_stats.empty
    assert df_players.empty

@patch("data_processing.dp_general.process_match_details_batch")
@patch("data_processing.dp_general.process_match_stats_batch")
@patch("data_processing.dp_general.process_team_details_batch")
@pytest.mark.asyncio
async def test_process_matches_no_players(
    mock_process_team_details_batch,
    mock_process_match_stats_batch,
    mock_process_match_details_batch,
    mock_faceit_data_v1,
    mock_faceit_data_v4
):
    df_matches = pd.DataFrame({'match_id': ['match1'], 'event_id': ['event1'], 'status': 'FINISHED'})
    df_teams_matches = pd.DataFrame({'match_id': ['match1'], 'team_id': ['team_1']})
    df_teams = pd.DataFrame({'team_id': ['team_1'], 'team_name': ['team_team_1']})
    df_maps = pd.DataFrame({'match_id': ['match1'], 'match_round': [1]})
    df_teams_maps = pd.DataFrame({'match_id': ['match1'], 'match_round': [1], 'team_id': ['team_1']})
    df_players_stats = pd.DataFrame(columns=['player_id', 'match_id', 'team_id'])  # No player_ids

    mock_process_match_details_batch.return_value = (df_matches, df_teams_matches)
    mock_process_team_details_batch.return_value = df_teams
    mock_process_match_stats_batch.return_value = (df_maps, df_teams_maps, df_players_stats)

    results = await dp_general.process_matches(
        match_ids=["match1"],
        event_ids=["event1"],
        faceit_data=mock_faceit_data_v4,
        faceit_data_v1=mock_faceit_data_v1,
    )

    *_, df_players = results
    assert df_players.empty

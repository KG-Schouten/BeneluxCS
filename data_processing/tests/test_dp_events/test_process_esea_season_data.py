# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from unittest.mock import patch

from data_processing import dp_events
from data_processing.faceit_api.sliding_window import RateLimitException

# --- process_esea_season_data tests ---
@pytest.mark.asyncio
@patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x)
async def test_process_esea_season_data(mock_modify_keys, mock_faceit_data_v1):
    df_seasons, df_events = await dp_events.process_esea_season_data(mock_faceit_data_v1)

    assert isinstance(df_seasons, pd.DataFrame)
    assert isinstance(df_events, pd.DataFrame)
    assert not df_seasons.empty
    assert not df_events.empty
    assert "season_id" in df_seasons.columns
    assert "event_id" in df_events.columns

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mock_name", "error_type", "error_message"),
    [
        ("invalid_format_mock", TypeError, "is not a dictionary"),
        ("none_response_mock", TypeError, "is not a dictionary"),
        ("empty_response_mock", ValueError, "No data found in"),
        ("rate_limit_mock", RateLimitException, "Rate limit reached"),
        ("make_mock_with_status", TypeError, "is not a dictionary")
    ]
)
async def test_process_esea_season_data_invalid_api_responses(
    mock_name, error_type, error_message, request
):
    with patch('data_processing.dp_general.modify_keys', side_effect=lambda x: x):
        if mock_name == "make_mock_with_status":
            mock = request.getfixturevalue(mock_name)(404)
        else:
            mock = request.getfixturevalue(mock_name)

        with pytest.raises(error_type, match=error_message):
            await dp_events.process_esea_season_data(mock)
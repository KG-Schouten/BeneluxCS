# Allow standalone execution
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytest
import pandas as pd
from datetime import datetime, timezone

from data_processing import dp_events

# --- filter_esea_matches  tests---

# Create reusable input data
now = int(datetime.now(timezone.utc).timestamp())
past_time = now - 10000
future_time = now + 10000
df_matches = pd.DataFrame([
    {"match_id": "m1", "event_id": "e1", "team_id": "t1", "match_time": past_time},
    {"match_id": "m2", "event_id": "e1", "team_id": "t2", "match_time": now},
    {"match_id": "m3", "event_id": "e2", "team_id": "t1", "match_time": future_time},
])
df_events = pd.DataFrame([
    {"event_id": "e1", "event_start": past_time, "event_end": now + 500},
    {"event_id": "e2", "event_start": future_time, "event_end": future_time + 500},
])
@pytest.mark.parametrize("match_amount, match_amount_type, from_timestamp, event_status", [
    # 1. No filters at all
    ("ALL", "ANY", 0, "ALL"),

    # 2. Filter by timestamp
    ("ALL", "ANY", now + 1, "ALL"),

    # 3. Filter by match amount ANY
    (2, "ANY", 0, "ALL"),

    # 4. Filter by match amount TEAM
    (1, "TEAM", 0, "ALL"),

    # 5. Filter by event status: PAST
    ("ALL", "ANY", 0, "PAST"),

    # 6. Filter by event status: ONGOING
    ("ALL", "ANY", 0, "ONGOING"),

    # 7. Filter by event status: UPCOMING
    ("ALL", "ANY", 0, "UPCOMING"),

    # 8. Filter by all at once
    (1, "TEAM", now, "ONGOING"),
])
def test_filter_esea_matches(match_amount, match_amount_type, from_timestamp, event_status):
    filtered = dp_events.filter_esea_matches(
        df=df_matches.copy(),
        df_events=df_events.copy(),
        match_amount=match_amount,
        match_amount_type=match_amount_type,
        from_timestamp=from_timestamp,
        event_status=event_status
    )
    
    assert isinstance(filtered, pd.DataFrame)
    if match_amount != "ALL":
        if match_amount_type == "ANY":
            assert len(filtered) <= match_amount
        elif match_amount_type == "TEAM":
            assert all(filtered.groupby("team_id").size() <= match_amount)
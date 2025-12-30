import pandas as pd
from pipeline.run_pipeline import build_stg_events, build_sessions, build_orders, build_attribution

def test_session_timeout_creates_new_session():
    raw = pd.DataFrame({
        "client_id": ["A","A","A"],
        "page_url": ["https://puffy.com/?gclid=1","https://puffy.com/","https://puffy.com/"],
        "referrer": [None,None,None],
        "timestamp": ["2025-01-01T00:00:00.000Z","2025-01-01T00:10:00.000Z","2025-01-01T01:00:01.000Z"],
        "event_name": ["page_viewed","page_viewed","page_viewed"],
        "event_data": [None,None,None],
        "user_agent": ["x","x","x"],
    })
    stg = build_stg_events(raw)
    stg2, sess = build_sessions(stg, timeout_min=30)
    # first 2 in same session, last in a new session
    sids = stg2.sort_values("ts")["session_id"].tolist()
    assert sids[0] == sids[1]
    assert sids[2] != sids[1]
    assert len(sess) == 2

def test_attribution_lookback_7_days():
    # one touchpoint at day -8 should NOT be counted; day -3 should be counted
    sessions = pd.DataFrame({
        "session_id": ["A-1","A-2"],
        "client_id": ["A","A"],
        "session_start": pd.to_datetime(["2025-01-01T00:00:00Z","2025-01-06T00:00:00Z"], utc=True),
        "session_end": pd.to_datetime(["2025-01-01T00:05:00Z","2025-01-06T00:05:00Z"], utc=True),
        "marketing_source": ["google_ads","meta_ads"],
        "utm_source": [None,None],
        "utm_medium": [None,None],
        "utm_campaign": [None,None],
        "ref_domain": [None,None],
    })
    orders = pd.DataFrame({
        "transaction_id": ["T1"],
        "client_id": ["A"],
        "order_ts": pd.to_datetime(["2025-01-09T00:00:00Z"], utc=True),
        "revenue": [100],
        "items_count": [1],
        "session_id": ["A-3"],
        "marketing_source": ["direct"],
        "utm_source": [None],
        "utm_medium": [None],
        "utm_campaign": [None],
        "ref_domain": [None],
        "device_type": ["desktop"],
    })
    attrib = build_attribution(orders, sessions, lookback_days=7)
    first = attrib[attrib["model"]=="first_click"].iloc[0]
    last = attrib[attrib["model"]=="last_click"].iloc[0]
    # session at 2025-01-01 is 8 days prior -> excluded, so first/last should be meta_ads (A-2)
    assert first["attributed_source"] == "meta_ads"
    assert last["attributed_source"] == "meta_ads"

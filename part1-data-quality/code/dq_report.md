# Data Quality Report

## Summary

- Partitions checked: **14**
- Total findings: **35** (ERROR: 11, WARN: 24)

## Key Metrics by Partition

|   partition |   rows |   purchases |   duplicate_transaction_ids |   null_rate.client_id_canonical |   null_rate.referrer |   bad_event_data_json |   invalid_event_name |
|------------:|-------:|------------:|----------------------------:|--------------------------------:|---------------------:|----------------------:|---------------------:|
|    20250223 |   4070 |          21 |                           0 |                      0.00540541 |             0.72973  |                     0 |                    0 |
|    20250224 |   3769 |          26 |                           0 |                      0.0145927  |             0.771557 |                     0 |                    0 |
|    20250225 |   3470 |          16 |                           0 |                      0.00979827 |             0.749568 |                     0 |                    0 |
|    20250226 |   3177 |          15 |                           2 |                      0.0144791  |             0.706956 |                     0 |                    0 |
|    20250227 |   3393 |          21 |                           2 |                      0.0100206  |             0.712938 |                     0 |                    0 |
|    20250228 |   3263 |          19 |                           0 |                      0.00827459 |             0.59669  |                     0 |                    0 |
|    20250301 |   3489 |          29 |                           2 |                      0.00659215 |             0.362855 |                     0 |                    0 |
|    20250302 |   4335 |          22 |                           0 |                      0.158016   |             0.46782  |                     0 |                    0 |
|    20250303 |   4042 |          24 |                           0 |                      0.0868382  |             0.451509 |                     0 |                    0 |
|    20250304 |   3538 |          20 |                           0 |                      0.0847937  |           nan        |                     0 |                    0 |
|    20250305 |   2881 |          20 |                           0 |                      0.0791392  |           nan        |                     0 |                    0 |
|    20250306 |   3442 |          23 |                           0 |                      0.0828007  |           nan        |                     0 |                    0 |
|    20250307 |   3251 |          16 |                           0 |                      0.0830514  |           nan        |                     0 |                    0 |
|    20250308 |   3843 |          22 |                           2 |                      0.142597   |           nan        |                     0 |                    0 |


## ERROR

- **purchase.duplicate_transaction_id** (partition 20250226): Found 2 purchase rows with duplicate transaction_id (will inflate revenue if summed).  
  _sample_: `{"examples": [{"timestamp": "2025-02-26T19:51:13.069Z", "source_file": "events_20250226.csv"}, {"timestamp": "2025-02-26T22:58:36.874Z", "source_file": "events_20250226.csv"}], "partition": "20250226"}...`
- **purchase.duplicate_transaction_id** (partition 20250227): Found 2 purchase rows with duplicate transaction_id (will inflate revenue if summed).  
  _sample_: `{"examples": [{"timestamp": "2025-02-27T01:36:32.342Z", "source_file": "events_20250227.csv"}, {"timestamp": "2025-02-27T22:11:28.480Z", "source_file": "events_20250227.csv"}], "partition": "20250227"}...`
- **purchase.duplicate_transaction_id** (partition 20250301): Found 2 purchase rows with duplicate transaction_id (will inflate revenue if summed).  
  _sample_: `{"examples": [{"timestamp": "2025-03-01T15:43:14.036Z", "source_file": "events_20250301.csv"}, {"timestamp": "2025-03-01T22:55:01.346Z", "source_file": "events_20250301.csv"}], "partition": "20250301"}...`
- **nulls.client_id_canonical** (partition 20250302): High null rate for client_id_canonical: 15.8%  
  _sample_: `{"partition": "20250302"}...`
- **nulls.client_id_canonical** (partition 20250303): High null rate for client_id_canonical: 8.7%  
  _sample_: `{"partition": "20250303"}...`
- **nulls.client_id_canonical** (partition 20250304): High null rate for client_id_canonical: 8.5%  
  _sample_: `{"partition": "20250304"}...`
- **nulls.client_id_canonical** (partition 20250305): High null rate for client_id_canonical: 7.9%  
  _sample_: `{"partition": "20250305"}...`
- **nulls.client_id_canonical** (partition 20250306): High null rate for client_id_canonical: 8.3%  
  _sample_: `{"partition": "20250306"}...`
- **nulls.client_id_canonical** (partition 20250307): High null rate for client_id_canonical: 8.3%  
  _sample_: `{"partition": "20250307"}...`
- **nulls.client_id_canonical** (partition 20250308): High null rate for client_id_canonical: 14.3%  
  _sample_: `{"partition": "20250308"}...`
- **purchase.duplicate_transaction_id** (partition 20250308): Found 2 purchase rows with duplicate transaction_id (will inflate revenue if summed).  
  _sample_: `{"examples": [{"timestamp": "2025-03-08T03:58:02.192Z", "source_file": "events_20250308.csv"}, {"timestamp": "2025-03-08T16:34:14.414Z", "source_file": "events_20250308.csv"}], "partition": "20250308"}...`


## WARN

- **purchase.revenue_non_positive** (partition 20250224): 1 purchase rows have revenue <= 0.  
  _sample_: `{"transaction_ids": ["ORD-20250224-227"], "partition": "20250224"}...`
- **purchase.revenue_non_integer** (partition 20250224): 1 purchase rows have non-integer revenue values (possible cents/dollars mismatch).  
  _sample_: `{"transaction_ids": ["ORD-20250224-382"], "partition": "20250224"}...`
- **purchase.revenue_non_positive** (partition 20250225): 1 purchase rows have revenue <= 0.  
  _sample_: `{"transaction_ids": ["ORD-20250225-246"], "partition": "20250225"}...`
- **schema.drift.client_id_alias** (partition 20250227): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250227"}...`
- **purchase.revenue_non_positive** (partition 20250227): 1 purchase rows have revenue <= 0.  
  _sample_: `{"transaction_ids": ["ORD-20250227-314"], "partition": "20250227"}...`
- **schema.drift.client_id_alias** (partition 20250228): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250228"}...`
- **purchase.revenue_non_positive** (partition 20250228): 1 purchase rows have revenue <= 0.  
  _sample_: `{"transaction_ids": ["ORD-20250228-318"], "partition": "20250228"}...`
- **schema.drift.client_id_alias** (partition 20250301): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250301"}...`
- **schema.drift.client_id_alias** (partition 20250302): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250302"}...`
- **schema.drift.client_id_alias** (partition 20250303): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250303"}...`
- **purchase.revenue_non_positive** (partition 20250303): 2 purchase rows have revenue <= 0.  
  _sample_: `{"transaction_ids": ["ORD-20250303-390", "ORD-20250303-169"], "partition": "20250303"}...`
- **purchase.revenue_non_integer** (partition 20250303): 1 purchase rows have non-integer revenue values (possible cents/dollars mismatch).  
  _sample_: `{"transaction_ids": ["ORD-20250303-171"], "partition": "20250303"}...`
- **schema.drift.client_id_alias** (partition 20250304): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250304"}...`
- **schema.missing_referrer** (partition 20250304): referrer column missing (will break/limit attribution).  
  _sample_: `{"partition": "20250304"}...`
- **purchase.revenue_non_positive** (partition 20250304): 2 purchase rows have revenue <= 0.  
  _sample_: `{"transaction_ids": ["ORD-20250304-605", "ORD-20250304-556"], "partition": "20250304"}...`
- **schema.drift.client_id_alias** (partition 20250305): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250305"}...`
- **schema.missing_referrer** (partition 20250305): referrer column missing (will break/limit attribution).  
  _sample_: `{"partition": "20250305"}...`
- **schema.drift.client_id_alias** (partition 20250306): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250306"}...`
- **schema.missing_referrer** (partition 20250306): referrer column missing (will break/limit attribution).  
  _sample_: `{"partition": "20250306"}...`
- **purchase.revenue_non_positive** (partition 20250306): 2 purchase rows have revenue <= 0.  
  _sample_: `{"transaction_ids": ["ORD-20250306-263", "ORD-20250306-685"], "partition": "20250306"}...`
- **schema.drift.client_id_alias** (partition 20250307): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250307"}...`
- **schema.missing_referrer** (partition 20250307): referrer column missing (will break/limit attribution).  
  _sample_: `{"partition": "20250307"}...`
- **schema.drift.client_id_alias** (partition 20250308): Client ID column drift detected. Present: ['clientId']. Canonical expected: client_id.  
  _sample_: `{"partition": "20250308"}...`
- **schema.missing_referrer** (partition 20250308): referrer column missing (will break/limit attribution).  
  _sample_: `{"partition": "20250308"}...`


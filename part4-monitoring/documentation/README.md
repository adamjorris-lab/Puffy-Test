
# Puffy Skills Test — Part 4: Production Monitoring (Enhanced)

## Goal
Daily monitoring that prevents incorrect data from reaching dashboards and marketing decisions.

## Run
```bash
pip install -r monitoring/requirements.txt
python monitoring/run_monitoring.py \
  --warehouse_dir warehouse \
  --rules config/monitoring_rules.yml \
  --output_dir reports \
  --fail_on_error
```

### Optional: set expected partition date explicitly
```bash
python monitoring/run_monitoring.py --warehouse_dir warehouse --expected_run_date 2025-03-08
```

## Outputs
- `reports/monitoring_report.md`
- `reports/monitoring_report.json`
- `reports/quarantine_path.txt` (when quarantined)
- `quarantine/warehouse_quarantine_<timestamp>/` (snapshot for rollback)

## Notes
Alert routing is implemented as a practical stub (routing plan + console output). In production, wire it to Slack/PagerDuty.

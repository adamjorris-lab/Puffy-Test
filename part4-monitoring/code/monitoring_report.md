# Daily Monitoring Report
- Generated (UTC): 2025-12-30
- Expected partition date: **2025-03-08**
- Warehouse: `/mnt/data/puffy_part4_production_monitoring_v2/warehouse`

## Summary
- ERROR: **2** | WARN: **12** | INFO: **0**

## Findings
- **ERROR** `anomaly:conversion_rate` (2025-03-01): conversion_rate anomalous vs baseline mean=0.01 std=0.00: curr=0.01 z=3.12 pct_change=43.49% (threshold=3.0) value=0.00966010733452594
- **ERROR** `anomaly:revenue` (2025-03-01): revenue anomalous vs baseline mean=18351.10 std=3863.39: curr=30585.00 z=3.17 pct_change=66.67% (threshold=3.0) value=30585.0
- **WARN** `anomaly:aov` (2025-03-02): aov anomalous vs baseline mean=972.05 std=98.21: curr=1282.77 z=3.16 pct_change=31.97% (threshold=3.0) value=1282.7727272727273
- **WARN** `anomaly:channel_share` (2025-02-28): Channel 'direct' share shifted: curr=65.5% vs baseline=47.3% (threshold=0.15) value=0.6551502490760084
- **WARN** `anomaly:channel_share` (2025-02-28): Channel 'google_ads' share shifted: curr=13.1% vs baseline=28.7% (threshold=0.15) value=0.13107290160158552
- **WARN** `anomaly:channel_share` (2025-03-02): Channel 'direct' share shifted: curr=27.2% vs baseline=49.8% (threshold=0.15) value=0.2719960313241912
- **WARN** `anomaly:channel_share` (2025-03-03): Channel 'direct' share shifted: curr=22.5% vs baseline=46.4% (threshold=0.15) value=0.2251180211462682
- **WARN** `anomaly:channel_share` (2025-03-03): Channel 'microsoft_ads' share shifted: curr=36.0% vs baseline=6.2% (threshold=0.15) value=0.35957434356833523
- **WARN** `anomaly:channel_share` (2025-03-03): Channel 'other_referrer' share shifted: curr=26.8% vs baseline=7.8% (threshold=0.15) value=0.26798603253872266
- **WARN** `anomaly:channel_share` (2025-03-04): Channel 'direct' share shifted: curr=69.5% vs baseline=39.8% (threshold=0.15) value=0.6951268626661297
- **WARN** `anomaly:channel_share` (2025-03-07): Channel 'direct' share shifted: curr=67.3% vs baseline=47.4% (threshold=0.15) value=0.672576509737603
- **WARN** `anomaly:channel_share` (2025-03-07): Channel 'google_ads' share shifted: curr=1.7% vs baseline=20.7% (threshold=0.15) value=0.01674758605640718
- **WARN** `anomaly:channel_share` (2025-03-08): Channel 'utm_tagged' share shifted: curr=24.0% vs baseline=8.6% (threshold=0.15) value=0.24041948874808827
- **WARN** `orders:zero_revenue_orders`: Zero revenue orders: 10 (threshold=5.0) value=10.0
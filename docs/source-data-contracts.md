# Retail Source Data Contracts

These contracts define the first two-day integration fixture. All values are synthetic. Money uses decimal INR with two fractional digits. Timestamps use ISO 8601 UTC. Dates in folder names are delivery batch dates. The finance reporting date is derived from the transaction posting timestamp in UTC for this fixture.

Each scenario is a complete independent input. Use either clean or defects for a run. Paths follow feed/batch_date=YYYY-MM-DD/feed.csv beneath the scenario root. Use UTF-8 CSV with a header, commas, and standard CSV quoting. A header-only file is a valid empty batch. An absent required file is a missing batch.

## Feed definitions

- **orders** — Incremental order headers keyed by order_id. Fields are order_id, order_ts_utc, and city.
- **order_items** — Incremental order lines keyed by order_id and line_id. Fields are order_id, line_id, product_id, seller_id, quantity, and unit_price_inr. Line IDs and quantities are positive integers. Each line references its parent order, product, and seller.
- **products** — Full daily product snapshot keyed by product_id. Fields are product_id and product_name.
- **sellers** — Full daily seller snapshot keyed by seller_id. Fields are seller_id and seller_name.
- **payments** — Incremental transactions keyed by payment_id. Fields are payment_id, order_id, posted_ts_utc, amount_inr, and status. Amounts are positive. CAPTURED transactions contribute to gross sales. Replay at payment grain, independently of item counts.
- **refunds** — Incremental transactions keyed by refund_id. Fields are refund_id, payment_id, posted_ts_utc, amount_inr, and status. Amounts are positive. SUCCEEDED refunds subtract from sales on their posting date. Day one contains a header-only file. Day two references the day-one payment PAY1.
- **shipments** — Incremental shipment headers keyed by shipment_id. Fields are shipment_id, order_id, and promised_ts_utc.
- **shipment_events** — Incremental events keyed by event_id. Fields are event_id, shipment_id, event_ts_utc, source_sequence, and status. Sequence is a positive integer ordered within a shipment. Accept DISPATCHED, IN_TRANSIT, and DELIVERED. Retain unique late events in history. Update current status only when the accepted event sequence is newer. Quarantine malformed timestamps.
- **carrier_snapshot** — Independent daily reconciliation feed with one row per shipment_id and batch date. Fields are shipment_id, source_sequence, and status.

All listed fields are required in valid data. Identifiers are case-sensitive strings. Reference lookups can span earlier batches. These contracts define the fixture's valid statuses and schema version 1. Establish explicit schema changes before adding new fields or statuses.

## Generate and verify

Run the following from the retail-lakehouse directory with Python 3.

```powershell
python src/generate_fixtures.py --output data/fixtures
```

Process 2026-11-20 before 2026-11-21. In the defect scenario, day two contains EV3 before EV2, an identical duplicate of EV3, and EV_BAD with an invalid timestamp. SH1 remains DELIVERED at sequence 3. SH2 remains DISPATCHED. The duplicate contributes no extra history record and EV_BAD goes to quarantine. The clean and defect scenarios reconcile to gross INR 2,000.00, refunds INR 250.00, and net INR 1,750.00.

Use data/fixtures/expected-results.json for row counts, daily finance totals, and final shipment status. To test a missing file, omit one required feed file while staging a copy for ADF. The checked-in fixture can then be reused for normal runs.

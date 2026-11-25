"""Generate two deterministic retail batches for integration testing."""

import argparse
import csv
import json
from pathlib import Path


SCHEMAS = {
    "orders": ("order_id", "order_ts_utc", "city"),
    "order_items": ("order_id", "line_id", "product_id", "seller_id", "quantity", "unit_price_inr"),
    "products": ("product_id", "product_name"),
    "sellers": ("seller_id", "seller_name"),
    "payments": ("payment_id", "order_id", "posted_ts_utc", "amount_inr", "status"),
    "refunds": ("refund_id", "payment_id", "posted_ts_utc", "amount_inr", "status"),
    "shipments": ("shipment_id", "order_id", "promised_ts_utc"),
    "shipment_events": ("event_id", "shipment_id", "event_ts_utc", "source_sequence", "status"),
    "carrier_snapshot": ("shipment_id", "source_sequence", "status"),
}

DAY_ONE = "2026-11-20"
DAY_TWO = "2026-11-21"
DAY_ONE_BATCH = {
    "orders": [("O100", DAY_ONE + "T09:00:00Z", "Hyderabad")],
    "order_items": [("O100", 1, "P1", "S1", 2, "500.00"),
                    ("O100", 2, "P2", "S2", 1, "500.00")],
    "products": [("P1", "Notebook"), ("P2", "Desk lamp")],
    "sellers": [("S1", "Paper Goods"), ("S2", "Home Lighting")],
    "payments": [("PAY1", "O100", DAY_ONE + "T09:01:00Z", "1000.00", "CAPTURED"),
                 ("PAY2", "O100", DAY_ONE + "T09:02:00Z", "500.00", "CAPTURED")],
    "shipments": [("SH1", "O100", DAY_TWO + "T15:00:00Z")],
    "shipment_events": [("EV1", "SH1", DAY_ONE + "T12:00:00Z", 1, "DISPATCHED")],
    "carrier_snapshot": [("SH1", 1, "DISPATCHED")],
}
DAY_TWO_BATCH = {
    "orders": [("O101", DAY_TWO + "T08:00:00Z", "Bengaluru")],
    "order_items": [("O101", 1, "P1", "S1", 1, "500.00")],
    "products": DAY_ONE_BATCH["products"],
    "sellers": DAY_ONE_BATCH["sellers"],
    "payments": [("PAY3", "O101", DAY_TWO + "T08:01:00Z", "500.00", "CAPTURED")],
    "refunds": [("REF1", "PAY1", DAY_TWO + "T17:00:00Z", "250.00", "SUCCEEDED")],
    "shipments": [("SH2", "O101", DAY_TWO + "T16:00:00Z")],
    "shipment_events": [("EV2", "SH1", DAY_TWO + "T10:00:00Z", 2, "IN_TRANSIT"),
                        ("EV3", "SH1", DAY_TWO + "T14:00:00Z", 3, "DELIVERED"),
                        ("EV4", "SH2", DAY_TWO + "T11:00:00Z", 1, "DISPATCHED")],
    "carrier_snapshot": [("SH1", 3, "DELIVERED"), ("SH2", 1, "DISPATCHED")],
}


def write_batch(output, date, batch):
    for feed, columns in SCHEMAS.items():
        target = output / feed / f"batch_date={date}" / f"{feed}.csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(columns)
            writer.writerows(batch.get(feed, ()))


def generate(output):
    write_batch(output / "clean", DAY_ONE, DAY_ONE_BATCH)
    write_batch(output / "clean", DAY_TWO, DAY_TWO_BATCH)

    in_transit, delivered, dispatched = DAY_TWO_BATCH["shipment_events"]
    bad_batch = {
        **DAY_TWO_BATCH,
        "shipment_events": [
            delivered, in_transit, delivered, dispatched,
            ("EV_BAD", "SH2", "invalid-timestamp", 2, "DELIVERED"),
        ],
    }
    write_batch(output / "defects", DAY_ONE, DAY_ONE_BATCH)
    write_batch(output / "defects", DAY_TWO, bad_batch)

    expected = {
        "finance_by_posting_date": {
            DAY_ONE: {"gross_inr": "1500.00", "refund_inr": "0.00", "net_inr": "1500.00"},
            DAY_TWO: {"gross_inr": "500.00", "refund_inr": "250.00", "net_inr": "250.00"},
        },
        "totals": {"orders": 2, "order_items": 3, "payments": 3, "refunds": 1,
                   "gross_inr": "2000.00", "refund_inr": "250.00", "net_inr": "1750.00"},
        "shipment_status_after_day_two": {"SH1": "DELIVERED", "SH2": "DISPATCHED"},
        "shipment_events": {
            "clean": {"raw": 4, "accepted_unique": 4, "duplicates": 0, "quarantined": 0},
            "defects": {"raw": 6, "accepted_unique": 4, "duplicates": 1, "quarantined": 1},
        },
    }
    (output / "expected-results.json").write_text(
        json.dumps(expected, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    generate(args.output)
    print(f"Generated clean and defect scenarios in {args.output.resolve()}")

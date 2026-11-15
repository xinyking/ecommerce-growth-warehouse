"""Validate the hand-calculated acceptance sample with the standard library."""

import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path


def main():
    fixture = Path(__file__).resolve().parents[1] / "tests/fixtures/events.jsonl"
    rows = [json.loads(line) for line in fixture.read_text(encoding="utf-8").splitlines()]
    unique = {}
    for row in rows:
        previous = unique.setdefault(row["event_id"], row)
        assert previous == row, f"Conflicting event ID: {row['event_id']}"

    sessions = defaultdict(list)
    for row in unique.values():
        if row.get("user_id") and row.get("session_id"):
            sessions[row["user_id"], row["session_id"]].append(row)

    counts = [0, 0, 0]
    stages = ("view_item", "add_to_cart", "purchase")
    for events in sessions.values():
        visited = 0
        previous_time = None
        for event in sorted(events, key=lambda row: row["event_time"]):
            timestamp = datetime.fromisoformat(event["event_time"])
            if event["event_name"] != stages[visited]:
                continue
            if previous_time is not None and timestamp <= previous_time:
                continue
            counts[visited] += 1
            previous_time = timestamp
            visited += 1
            if visited == len(stages):
                break

    purchases = [row for row in unique.values() if row["event_name"] == "purchase"]
    amount = sum(Decimal(row["amount_usd"]) for row in purchases if row.get("amount_usd"))
    missing = sum(not row.get("amount_usd") for row in purchases)
    actual = (len(rows), len(unique), len(sessions), counts, len(purchases), amount, missing)
    expected = (9, 8, 3, [3, 1, 1], 3, Decimal("50.00"), 1)
    if actual != expected:
        raise AssertionError(f"Expected {expected}, got {actual}")
    print("PASS: rows=9/8, sessions=3, funnel=3/1/1, purchases=3, USD=50.00, missing=1")


if __name__ == "__main__":
    main()

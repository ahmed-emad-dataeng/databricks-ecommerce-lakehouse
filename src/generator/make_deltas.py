"""Generate the CDC change files and order-event stream from the Olist base.

Why generate rather than use Olist as-is: Olist is a static historical export
with no `updated_at`, no deletes and no change feed, so it cannot demonstrate
incremental loading or SCD2 on its own. This produces the missing change
traffic on top of the real data.

Deliberately dirty, because clean input proves nothing:
  * duplicate keys within a single file  -> exercises dedupe ordering
  * out-of-order rows                    -> file order must not decide the winner
  * late-arriving events                 -> exercises watermarks (extension E1)
  * deletes                              -> exercises SCD2 tombstones

DETERMINISTIC: seeded RNG and a fixed base timestamp, so regenerating produces
byte-identical files. The `idempotency_check` job task depends on that -- if the
generator drifted between runs, a re-run would legitimately differ and the check
would be meaningless.

Runs on Databricks (`%pip install faker`) or locally:

    python -m src.generator.make_deltas \
        --customers ./data/olist/olist_customers_dataset.csv \
        --cdc-out ./data/cdc
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

BASE_TS = datetime(2018, 10, 1, 0, 0, 0)

CHANGE_HEADER = [
    "op",
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "updated_at",
]

# Plausible relocation targets, so a city change reads as a move, not noise.
BR_CITIES = [
    ("sao paulo", "SP"),
    ("rio de janeiro", "RJ"),
    ("belo horizonte", "MG"),
    ("curitiba", "PR"),
    ("porto alegre", "RS"),
    ("salvador", "BA"),
    ("brasilia", "DF"),
    ("fortaleza", "CE"),
    ("recife", "PE"),
    ("manaus", "AM"),
]

EVENT_SEQUENCE = ["created", "approved", "shipped", "delivered"]


def _ts(offset_hours: float) -> str:
    return (BASE_TS + timedelta(hours=offset_hours)).strftime("%Y-%m-%d %H:%M:%S")


def read_head(path: str, limit: int) -> list[dict]:
    """Read a deterministic head slice of a real Olist CSV."""
    with open(path, newline="", encoding="utf-8") as fh:
        return [row for _, row in zip(range(limit), csv.DictReader(fh))]


def build_change_day(
    day: int,
    customers: list[dict],
    rng: random.Random,
    n_updates: int,
    n_deletes: int,
) -> list[list]:
    """One day of CDC traffic: updates, deletes and brand-new customers."""
    rows: list[list] = []
    hour = day * 24

    # Disjoint slices per day, so day 2 does not silently undo day 1.
    start = (day - 1) * (n_updates + n_deletes)
    pool = customers[start : start + n_updates + n_deletes]
    updates, deletes = pool[:n_updates], pool[n_updates:]

    for i, cust in enumerate(updates):
        city, state = rng.choice(BR_CITIES)
        rows.append(
            [
                "U",
                cust["customer_id"],
                cust["customer_unique_id"],
                city,
                state,
                _ts(hour + i * 0.1),
            ]
        )

    for i, cust in enumerate(deletes):
        rows.append(
            [
                "D",
                cust["customer_id"],
                cust["customer_unique_id"],
                cust["customer_city"],
                cust["customer_state"],
                _ts(hour + 12 + i * 0.1),
            ]
        )

    # Brand-new customers that do not exist in the base load at all.
    for i in range(3):
        city, state = rng.choice(BR_CITIES)
        synthetic = (f"gen{day:02d}{i:03d}" + "0" * 24)[:32]
        rows.append(["I", synthetic, "u" + synthetic[:31], city, state, _ts(hour + 18 + i)])

    _inject_dirt(rows, day, rng)
    return rows


def _inject_dirt(rows: list[list], day: int, rng: random.Random) -> None:
    """Add a duplicate key and shuffle, in place.

    The duplicate carries a LATER updated_at but can land anywhere in the file
    after the shuffle, so a pipeline that keeps the first row seen -- or the last
    row seen -- gets the wrong answer. Only ordering by updated_at is correct.
    """
    if not rows:
        return

    victim = list(rows[0])
    victim[3] = "correct-city-wins"
    victim[5] = _ts(day * 24 + 23)
    rows.insert(0, victim)
    rng.shuffle(rows)


def build_events(orders: list[dict], rng: random.Random) -> list[dict]:
    """Order lifecycle events, with duplicates and late arrivals."""
    events: list[dict] = []

    for idx, order in enumerate(orders):
        n_stages = rng.randint(1, len(EVENT_SEQUENCE))
        for stage in range(n_stages):
            events.append(
                {
                    "event_id": f"{order['order_id']}-{EVENT_SEQUENCE[stage]}",
                    "order_id": order["order_id"],
                    "customer_id": order["customer_id"],
                    "event_type": EVENT_SEQUENCE[stage],
                    "event_ts": _ts(idx * 0.5 + stage * 6),
                }
            )

    # At-least-once delivery: every tenth event arrives twice, identically.
    duplicates = [dict(e) for e in events[::10]]

    # Late arrivals: real timestamp far in the past, so a watermark has to
    # decide whether to still accept them.
    late = [
        {**dict(e), "event_id": e["event_id"] + "-late", "event_ts": _ts(-72)}
        for e in events[::25]
    ]

    combined = events + duplicates + late
    rng.shuffle(combined)
    return combined


def write_csv(path: str, header: list[str], rows: list[list]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def write_jsonl(path: str, records: list[dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate CDC change files and order events.")
    ap.add_argument("--customers", required=True, help="olist_customers_dataset.csv")
    ap.add_argument("--orders", help="olist_orders_dataset.csv (for the event stream)")
    ap.add_argument("--cdc-out", required=True, help="dir for customers_changes_day*.csv")
    ap.add_argument("--events-out", help="dir for order_events_*.jsonl")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--updates-per-day", type=int, default=40)
    ap.add_argument("--deletes-per-day", type=int, default=5)
    ap.add_argument("--event-orders", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=42, help="fixed so output is reproducible")
    args = ap.parse_args()

    needed = args.days * (args.updates_per_day + args.deletes_per_day)
    customers = read_head(args.customers, needed)
    if len(customers) < needed:
        raise SystemExit(
            f"need {needed} customer rows for {args.days} days, got {len(customers)}"
        )

    for day in range(1, args.days + 1):
        # Re-seed per day: day N's output must not depend on whether day N-1 ran.
        rng = random.Random(args.seed + day)
        rows = build_change_day(day, customers, rng, args.updates_per_day, args.deletes_per_day)
        out = os.path.join(args.cdc_out, f"customers_changes_day{day}.csv")
        write_csv(out, CHANGE_HEADER, rows)
        print(f"wrote {len(rows):>6} change rows -> {out}")

    if args.orders and args.events_out:
        orders = read_head(args.orders, args.event_orders)
        events = build_events(orders, random.Random(args.seed))
        out = os.path.join(args.events_out, "order_events_001.jsonl")
        write_jsonl(out, events)
        print(f"wrote {len(events):>6} events      -> {out}")


if __name__ == "__main__":
    main()

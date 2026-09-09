# Databricks E-Commerce Lakehouse

A dimensional analytics platform built on Databricks Free Edition over the Olist
Brazilian e-commerce dataset — eight related operational tables turned into a
Kimball star schema that answers defined business questions.

The point of the project is not the medallion layers. It is this:

```
eight operational entities
   → incremental ingestion (COPY INTO, file-idempotent)
   → data quality + de-duplication (rules, quarantine, audit)
   → CDC (generated change feed: inserts, updates, deletes)
   → SCD Type 2 (point-in-time correct customer history)
   → dimensional model (3 fact grains, 6 conformed dimensions)
   → business analysis (AI/BI dashboard + Genie)
```

Everything runs on **Databricks Free Edition at $0**. The platform constraints
that shaped the design are documented rather than hidden — see
[Free Edition limitations](#free-edition-limitations-and-what-they-changed).

> **Status — verified against a live Free Edition workspace:**
> `bronze_ingest`, `silver_clean` and `gold_dims` all run green. 550,759 rows
> loaded across 8 bronze tables, 8 silver tables, 7 quarantine tables and 6 gold
> dimensions. Numbers below marked measured are from that run.
>
> **Not yet run:** the three fact tables, the CDC/SCD2 path, the analytical
> views, the AI/BI dashboard, the Genie space, and the end-to-end idempotency
> check. Those remain `TBD` and are not claimed.

---

## Architecture

```mermaid
flowchart TB
    subgraph src["Source"]
        A["Olist CSVs<br/>8 files loaded, ~550K rows<br/>(geolocation excluded: unused)"]
        B["Generated change feed<br/>CDC + order events"]
    end
    subgraph lake["Unity Catalog: ecommerce"]
        C["landing.raw<br/>UC Volume"]
        D["bronze<br/>8 Delta tables, all-string + provenance"]
        E["silver<br/>typed · cleaned · deduped · validated"]
        Q["silver.quarantine_*<br/>rows failing a reject rule"]
        F["gold<br/>dim_* / fact_* / v_*"]
        O["ops<br/>pipeline_runs · dq_results · gold_fingerprints"]
    end
    subgraph out["Consumption"]
        G["AI/BI dashboard"]
        H["Genie space"]
    end

    A --> C
    B --> C
    C -->|"COPY INTO<br/>file-idempotent"| D
    D -->|PySpark| E
    E -->|rejected| Q
    E -->|"MERGE · SCD2"| F
    F --> G
    F --> H
    D -.-> O
    E -.-> O
    F -.-> O
```

Orchestrated as one linear Lakeflow Job:

```
generate_deltas → bronze_ingest → silver_clean → gold_dims
                → cdc_apply → gold_facts → analytics_views → idempotency_check
```

Linear because Free Edition caps concurrent job tasks at five — not for style.

---

## The data model

Three facts, three grains, stated in every table's `COMMENT`:

| Fact | Grain | Key measures |
|---|---|---|
| `fact_order` | one **order** | `order_total`, `delivery_days`, `delivery_delay_days`, `is_late`, `approval_lag_hours` |
| `fact_order_item` | one **product line within an order** | `item_price`, `freight_value`, `item_revenue` |
| `fact_payment` | one **payment record for an order** | `payment_value`, `payment_installments` |

| Dimension | Type | Natural key |
|---|---|---|
| `dim_date` | generated, conformed | `date` |
| `dim_customer` | **SCD Type 2** | `customer_unique_id` |
| `dim_product` | SCD Type 1 | `product_id` |
| `dim_seller` | SCD Type 1 | `seller_id` |
| `dim_order_status` | conformed lookup | `order_status` |
| `dim_payment_type` | conformed lookup | `payment_type` |

Informational PK/FK constraints are declared so Catalog Explorer renders the
star schema as an ERD.

> **ERD screenshot:** TBD (Catalog Explorer, after first run)

### Two traps this model exists to avoid

Both are demonstrated by a query in `sql/views/`, not just described. Full
reasoning in [docs/decisions.md](docs/decisions.md).

**1. The customer key.** Olist issues a **new `customer_id` for every order**;
only `customer_unique_id` is stable across orders. Key `dim_customer` on
`customer_id` and every customer looks brand new — repeat-purchase rate collapses
to ~0%, LTV equals AOV, and SCD2 never fires. Nothing errors; every number is
just wrong. `gold.v_repeat_rate_key_comparison` computes the metric both ways.

Measured on the loaded data:

| | correct key (`customer_unique_id`) | naive key (`customer_id`) |
|---|---|---|
| distinct customers | **96,096** | 99,441 |
| phantom customers introduced | — | **+3,345** |
| repeat-purchase rate | TBD (needs `fact_order`) | TBD |

3,345 people are counted twice or more by the naive key. Every one of them is a
repeat customer that a `customer_id`-keyed model reports as brand new.

**2. Payment fan-out.** An Olist order carries N payment rows (installments,
voucher + card splits). Measured: **103,886 payment rows across 99,440 orders
= 1.0447 payments per order.** Joining payments onto order grain therefore
inflates revenue by **~4.5%** — more dangerous than a factor of ten, because a
number that is 4.5% wrong looks plausible and ships. `gold.v_fanout_demo` shows
correct and inflated revenue side by side.

### The payoff query

> *What city and segment was this customer in **at the time** order X was placed?*

`gold.v_order_point_in_time` answers it by joining `fact_order.customer_sk` to
the customer **version** current at purchase time, and puts the present-day
values in adjacent columns so the difference is visible. This is the query that
proves SCD Type 2 was implemented rather than named.

---

## Business questions

| # | Question | View |
|---|---|---|
| 1 | How does revenue change over time? | `v_revenue_monthly` |
| 2 | Which categories generate the most revenue? | `v_revenue_by_category` |
| 3 | Which sellers perform best, by month? | `v_seller_monthly_performance` |
| 4 | What is the repeat-purchase rate? | `v_repeat_purchase_rate` |
| 5 | How long do shipments take, and how often are they late? | `v_delivery_performance` |
| 6 | Which categories are cancelled most? | `v_cancellation_by_category` |
| 7 | How do payment methods affect order completion? | `v_payment_method_completion` |

The dashboard reads these views, never the fact tables directly, so a model
change does not break the dashboard.

> **Dashboard screenshot:** TBD
> **Genie space:** TBD — the curated questions and whether each resolved correctly

---

## Idempotency

"What happens if the job runs twice?" is answered by a task, not a claim.
Three distinct guarantees:

1. **File-level** — `COPY INTO` tracks consumed files; nothing is ingested
   twice. A re-run reports **0 inserted rows**, which is the guarantee showing
   up as a measurement rather than a claim.
2. **Write-level** — silver and gold are deterministic rebuilds of the layer
   below (`CREATE OR REPLACE`, `MERGE` on business keys). SCD2 is the exception
   and handles it by classifying replayed rows as `unchanged`.
3. **End-to-end** — `idempotency_check` fingerprints every gold table (row
   counts + additive measure sums) into `ops.gold_fingerprints` and **fails the
   job** if a re-run of the same `run_date` differs.

Deterministic surrogate keys (hashes, not sequences) are what make (2) possible:
rebuilding a dimension cannot renumber keys that facts already point at.

> **Re-run proof:** TBD — `idempotency_check` output across two runs of the same `run_date`

---

## Data quality

24 rules across 7 tables, defined as data in `src/silver/dq.py`. One definition
drives the clean/quarantine split, the per-run counts in `ops.dq_results`, and
this table.

| Severity | Behaviour | Example |
|---|---|---|
| `reject` | row diverted to `silver.quarantine_<table>` | `order_items.price_non_negative` |
| `warn` | row loads, failure still counted | `products.category_present` — **measured: 610 of 32,951 products (1.85%)** have no category but carry real revenue; dropping them would understate totals |

Two details that matter more than the rule count:

- **A NULL predicate result counts as a failure.** `price >= 0` is NULL when
  `price` is NULL, and a naive filter passes that row. Tested.
- **`warn` exists because Olist is real data.** Deliveries recorded before
  approval do occur; that is worth surfacing, not worth dropping the order over.

**No `reject` rule fires on real Olist data** — it is clean enough that every
`quarantine_*` table is empty after a normal load. An empty quarantine table
looks identical whether routing works or is silently broken, so
[`notebooks/98_dq_probe.py`](notebooks/98_dq_probe.py) injects a known-bad batch,
runs the real `clean_all()` path, asserts all five reject rules fired (including
a NULL-price row, which a naive filter would pass), then restores bronze via
Delta time travel and asserts the lakehouse matches its pre-probe baseline.

**Probe result — measured, and independently re-verified from outside the probe:**

| rule | rows failed | expected |
|---|---|---|
| `order_items.price_non_negative` | **3** | 3 (negative, **NULL**, and the two-rule row) |
| `order_items.freight_non_negative` | **2** | 2 |
| `order_items.order_id_not_null` | **1** | 1 |
| `order_items.product_id_not_null` | **1** | 1 |
| `order_items.item_sequence_positive` | **1** | 1 |

8 rule-failures across **7 quarantined rows** (one row breaks two rules), out of
`rows_checked = 112,658` — the 112,650 real rows plus 8 probe rows. The clean
control row reached silver. `price_non_negative` counting **3** rather than 2 is
the important number: it includes the NULL-price row, proving a NULL predicate
result is treated as a failure rather than passing a naive filter.

Restoration verified independently of the probe's own assertions:
`bronze.order_items` 112,650, `silver.order_items` 112,650,
`quarantine_order_items` 0 — all back to baseline, zero synthetic rows anywhere,
and `DESCRIBE HISTORY` shows `version 3 | RESTORE`, so the whole exercise is
auditable after the fact.

> **`ops.dq_results` screenshot:** TBD

---

## Observability

| Table | Grain | Contents |
|---|---|---|
| `ops.pipeline_runs` | (run_id, task_name) | status, duration, rows read/written/quarantined, error |
| `ops.dq_results` | (run_id, table, rule_id) | rows checked, rows failed, failure rate |
| `ops.gold_fingerprints` | (run_id, table) | row counts + measure sums per run |

Failures are logged **and re-raised** — a task that logs "succeeded" while
failing silently is worse than no logging.

---

## Repository layout

```
src/
  config.py              source-table registry driving every layer's loop
  generator/             deterministic CDC + event generator (pure Python)
  bronze/ingest.py       COPY INTO (active); Auto Loader kept, does not work here
  silver/
    transforms.py        PURE DataFrame -> DataFrame functions (all unit-tested)
    dq.py                rule registry
    clean.py             I/O driver composing the above
  gold/
    scd2.py              change classification (pure) + Delta MERGE driver
    dims.py  facts.py    dimensions and the three fact grains
  ops/
    run_log.py           pipeline_runs + dq_results
    idempotency.py       fingerprint + cross-run comparison
sql/views/               13 documented analytical + reconciliation views
notebooks/               thin job entrypoints (logic lives in src/)
tests/                   19 tests; 3 pin the SCD2 transitions
resources/               Lakeflow Job as YAML
databricks.yml           Declarative Automation Bundle
docs/decisions.md        every non-obvious choice, with reasoning
```

The split between `transforms.py` (pure) and `clean.py` (I/O) exists for one
reason: it makes the logic that carries correctness risk testable without a
metastore.

---

## Running it

Full setup, including the Free Edition prerequisites, is in
[docs/SETUP.md](docs/SETUP.md).

```bash
databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run ecommerce_pipeline --target dev
```

Re-run the same logical date to exercise the idempotency check:

```bash
databricks bundle run ecommerce_pipeline --target dev
```

Apply a day of CDC changes:

```bash
databricks bundle run ecommerce_pipeline --target dev -- --cdc_day 1
```

### Tests

Run them **in the workspace** via `notebooks/99_run_tests.py`. Local PySpark on
Windows needs a JDK plus `winutils.exe`/`HADOOP_HOME`, which is a time sink with
no payoff here. `tests/conftest.py` reuses the Databricks session when present
and builds a local one otherwise, so both paths work if you do have a JDK.

> **pytest output screenshot:** TBD

---

## Free Edition limitations, and what they changed

Stated plainly because these are platform limits, not design preferences — and
because knowing where a platform stops is part of knowing the platform.

| Limitation | Consequence for this project |
|---|---|
| Serverless only, no custom compute | No cluster-tuning story. Not attempted. |
| Spark UI unavailable (query profile only) | Performance analysis reads query profiles and `EXPLAIN`, not stage timelines. |
| `cache()` / `persist()` blocked | `apply_scd2` materialises to a staging table instead of caching a thrice-scanned DataFrame. |
| Only 6 Spark configs settable | `autoBroadcastJoinThreshold` and AQE cannot be changed, so a broadcast-vs-sort-merge benchmark is not a controlled experiment. Dropped rather than faked. |
| `Trigger.AvailableNow()` only | Streaming here is **micro-batch incremental ingestion**, not real-time. Named that way throughout. |
| **Auto Loader cannot start a streaming query at all** | Hit in practice, not predicted: `SPARK_CONNECT_ILLEGAL_STATE.…OPERATION_STATUS_MISMATCH` on the first table. `_schemas/customers` *was* written to the volume first, so volume writes and schema inference work — the fault is Spark Connect's streaming-query lifecycle, and serverless is Spark Connect only. Switched to `COPY INTO`, which was already coded as the fallback and gives identical file-level idempotency with no checkpoint. |
| Max 5 concurrent job tasks | Linear DAG. |
| One active pipeline per type | Both Lakeflow extensions share one pipeline. |
| One workspace / metastore / user | Governance shown via PK/FK constraints, comments and lineage — not `GRANT`s to invented groups. |
| Restricted outbound internet | Olist CSVs are uploaded from a laptop, not downloaded in a notebook. PyPI is reachable, so the generator runs in-workspace. |
| Quota breach halts compute for the day | No development cron; warehouse stopped manually; data held at ~550K rows. |

---

## Planned extensions

Not part of the core deliverable; each earns its way in.

1. **`AUTO CDC` comparison** — reproduce the SCD2 dimension with
   `AUTO CDC INTO ... STORED AS SCD TYPE 2` and write up hand-rolled vs
   declarative.
2. **Streaming path** — order events with watermarks,
   `dropDuplicatesWithinWatermark`, and windowed aggregation over the generated
   late/out-of-order events.
3. **CI** — GitHub Actions running `ruff` + `databricks bundle validate`.
4. **Data contracts** — source schemas as YAML, breaking changes fail the job.
5. **Performance notebook** — query profiles, `EXPLAIN FORMATTED`, Delta file
   skipping, liquid clustering, `OPTIMIZE`/`VACUUM`. Last on the list because
   serverless exposes the least here.

---

## Data attribution

Brazilian E-Commerce Public Dataset by Olist, published on Kaggle under
**CC BY-NC-SA 4.0**. Used here for non-commercial portfolio purposes.
The CSVs are **not** committed to this repository — see `docs/SETUP.md` for how
to obtain and upload them.

The CDC change feed and order-event stream are **generated**
(`src/generator/make_deltas.py`), because Olist is a static export with no
`updated_at`, no deletes and no change feed, and therefore cannot demonstrate
incremental loading or SCD2 on its own.

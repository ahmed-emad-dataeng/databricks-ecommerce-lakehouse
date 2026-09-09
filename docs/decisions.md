# Design decisions and trade-offs

Every non-obvious choice in this project, with the reasoning. Where a decision
was driven by a Databricks Free Edition constraint, that is stated rather than
dressed up as preference.

---

## 1. The customer key: `customer_unique_id`, not `customer_id`

**The trap.** Olist issues a **new `customer_id` for every order**. Only
`customer_unique_id` is stable for a person across orders:

| rows in `olist_customers_dataset.csv` | ~99,441 |
| distinct `customer_id`                | ~99,441 |
| distinct `customer_unique_id`         | ~96,096 |

So `customer_id` is not a customer identifier at all — it is an order-scoped
identifier that happens to live in a table called "customers".

**Why it matters.** Key `dim_customer` on `customer_id` and every customer has
exactly one order, so:

- repeat-purchase rate collapses to ~0%
- customer lifetime value equals average order value
- "new vs returning" reports that everyone is new
- SCD Type 2 never fires, because no key is ever seen twice

None of that throws an error. The pipeline runs green and every number is
wrong — which is what makes it worth documenting. It is the most common bug in
Olist portfolio projects.

**The decision.** `dim_customer` has natural key `customer_unique_id`.
`customer_id` is treated as what it is: a degenerate order-level attribute,
carried on `fact_order` for lineage back to the source row.

**The evidence.** `gold.v_repeat_rate_key_comparison` computes the metric both
ways in one query, so the gap is demonstrable rather than asserted.

**Consequence.** The customers table must be collapsed from `customer_id` grain
to `customer_unique_id` grain before it becomes a dimension. That needs a rule
for which address wins when a person ordered from several: the most recent one,
by order timestamp (`dims.customer_attributes`).

---

## 2. Payment fan-out: `fact_payment` is its own fact

**The trap.** An Olist order can have several payment rows — installments, or a
voucher combined with a card. Roughly 1.04 payment records per order on average,
but with a long tail.

**Why it matters.** Fold payments onto an order-grain fact, or join
`fact_payment` to `fact_order` and sum `order_total`, and revenue is multiplied
by the payment count. The inflation is only a few percent, which is worse than a
factor of ten — a number that is 4% wrong looks plausible and ships.

**The decision.** Three facts at three grains, never mixed:

| fact | grain |
|---|---|
| `fact_order` | one order |
| `fact_order_item` | one product line within an order |
| `fact_payment` | one payment record for an order |

Order revenue is aggregated **up** from `order_items`, never **across** from
payments. "Revenue by payment method" is answered from `fact_payment` at payment
grain, with orders counted via `count(DISTINCT order_id)`.

**The evidence.** `gold.v_fanout_demo` shows correct revenue next to
fan-out-inflated revenue in the same row.

**The general rule this illustrates.** Two measures at different grains do not
belong in one table. The convenience of a single wide table is exactly the bug.

---

## 3. SCD Type 2 on `dim_customer` only

`dim_customer` is SCD2. `dim_product` and `dim_seller` are SCD1.

**Why customer.** There is a real historical question:

> What city and segment was this customer in *at the time* order X was placed?

Answering it requires versioned customer rows and a point-in-time join
(`gold.v_order_point_in_time`).

**Why the change driver is `customer_segment`, not a city edit.** A generated
city change is contrived — nothing in Olist says customers move. But
`customer_segment` (an RFM-ish new / returning / lapsed / high_value bucket) is
**recomputed every run** and changes genuinely as behaviour ages: a customer who
has not ordered in 180 days becomes `lapsed` whether or not any source row
changed. That makes SCD2 load-bearing rather than decorative. The generated city
changes are layered on top to exercise the CDC path.

**Why product is deliberately SCD1.** Product category history would be
technically similar and add nothing to the analytical story — customer SCD2
already demonstrates point-in-time modelling. Making both SCD2 would be feature
coverage, not business-driven design. Stated explicitly because "why isn't this
one SCD2 as well?" is a fair interview question and the answer is a choice, not
an oversight.

**Deletes become tombstones, not just closed rows.** A CDC `D` closes the
current version *and* appends a version with `is_deleted = true`. Simply closing
the last row would leave the key with no current version, breaking point-in-time
joins for orders placed before the deletion. So:

- point-in-time joins → `customer_sk`
- live population → `is_current AND NOT is_deleted`

---

## 4. Hand-rolled `MERGE` before `AUTO CDC`

Free Edition supports `AUTO CDC INTO ... STORED AS SCD TYPE 2`, which does this
declaratively in about ten lines. The MVP still implements SCD2 by hand.

**Why.** Using the declarative API first would mean never demonstrating that the
mechanics are understood — closing the old version, opening the new one,
detecting change by hash, and above all making an unchanged row a **no-op**. The
comparison is the more useful artifact, and it only exists in this order:
implement it manually, then reproduce it with `AUTO CDC` and write up the
trade-offs (extension E2).

**The no-op is the load-bearing part.** If a replayed batch classifies as
`changed`, every job run appends a spurious version and idempotency is gone. The
row hash coalesces NULLs to a sentinel before hashing precisely because
`xxhash64(NULL)` is NULL, hashes never compare equal, and every row with a NULL
attribute would otherwise look changed on every single run. Three tests in
`tests/test_scd2.py` pin this down.

---

## 5. Surrogate keys are deterministic hashes, not sequences

`xxhash64` over the natural key (plus `effective_from` for SCD2 versions), not
`monotonically_increasing_id()` or an identity column.

**Trade-off accepted:** hashes are unreadable in a debugging session, and
collisions are theoretically possible (negligible at 10^5 rows).

**What it buys:** rebuilding a dimension cannot renumber existing keys. With
sequence-generated keys, a `CREATE OR REPLACE` of a dimension silently
invalidates every already-written fact row pointing at it — and would need extra
bookkeeping to stay stable across replays. Deterministic keys make the whole gold
layer a pure function of silver, which is what the idempotency check verifies.

---

## 6. Idempotency is three guarantees, not one

Easy to conflate, so stated separately:

1. **File-level** — Auto Loader (or `COPY INTO`) tracks which files a target has
   already consumed. Re-running ingests nothing twice.
2. **Write-level** — silver and gold are rebuilt deterministically from the layer
   below (`CREATE OR REPLACE` / `MERGE` on business keys), so re-running
   overwrites rather than appends. SCD2 is the exception and handles it by
   classifying replays as `unchanged`.
3. **End-to-end** — `ops/idempotency.py` fingerprints every gold table (row
   counts plus additive measure sums) and **fails the job** if a re-run of the
   same `run_date` differs.

(1) alone is not pipeline idempotency, which is the mistake worth avoiding: file
tracking says nothing about whether a downstream `MERGE` double-counts.

The fingerprint is a regression guard, not a correctness proof — two runs that
are wrong identically will pass. Correctness is covered by
`gold.v_reconciliation` and `gold.v_scd2_integrity`.

---

## 7. Free Edition constraints that shaped the architecture

Not preferences. These are platform limits, verified against current docs.

| Constraint | What it forced |
|---|---|
| Serverless only; no custom compute | No cluster-tuning story exists here. Not attempted. |
| Spark UI unavailable (query profile only) | Performance work reads query profiles and `EXPLAIN`, never a stage timeline. |
| `df.cache()` / `persist()` / `CACHE TABLE` blocked | `scd2.apply_scd2` materialises to a **staging table** instead of caching the classified batch, which is scanned three times. |
| Only 6 Spark configs settable | Cannot set `autoBroadcastJoinThreshold` or disable AQE, so a broadcast-vs-sort-merge benchmark is not a controlled experiment. Dropped rather than faked. |
| `Trigger.AvailableNow()` only | The streaming extension is **micro-batch incremental ingestion**, not real-time. Named honestly throughout. |
| Max 5 concurrent job tasks | The DAG is linear. Not a stylistic choice. |
| One active pipeline per pipeline type | Both Lakeflow extensions (E1 streaming, E2 `AUTO CDC`) must share one pipeline. |
| One workspace, one metastore, one user | Unity Catalog governance is demonstrated through PK/FK constraints, comments and lineage — not `GRANT` statements to invented groups, which would be theatre. |
| Outbound internet restricted | The Olist CSVs cannot be downloaded from a notebook; they are uploaded from a laptop. PyPI *is* reachable, so the generator runs in-workspace. |
| Quota breach shuts compute down for the day | No development cron schedule; the SQL warehouse is stopped manually; data stays at ~550K rows. |

---

## 8. Bronze stays all-string

Bronze columns are ingested as strings with `inferColumnTypes` off, and typing
happens once in silver.

**Why.** A cast that fails on row 40,000 of a CSV should be a visible silver
failure with a quarantine row, not a schema-inference surprise that changes
column types between runs depending on what the sample contained.
`rescuedDataColumn` catches anything that stops matching the schema instead of
dropping it silently.

---

## 9. Data quality: rules as data, with severities

Rules live in `src/silver/dq.py` as a list, driving three things from one
definition: the clean/quarantine split, the per-run counts in `ops.dq_results`,
and the documentation table in the README.

**`reject` vs `warn` is a real distinction.** Two examples from Olist:

- `orders.delivery_after_purchase` is **warn**. Olist genuinely contains
  deliveries recorded before approval. It is worth surfacing and not worth
  dropping the order over.
- `products.category_present` is **warn**. About 600 products have no category.
  They carry real revenue; dropping them would understate totals. They are
  bucketed as `unknown` instead.

Quarantining everything questionable would be easier and would produce wrong
totals.

**A NULL predicate result counts as a failure.** `price >= 0` evaluates to NULL
when `price` is NULL, and a naive filter would pass that row. `tag_rule_failures`
coalesces to `False` first. Tested in `tests/test_transforms.py`.

---

## 10. What was deliberately left out

| Not used | Why |
|---|---|
| AWS / S3 | Project 1 covers it; Free Edition cannot attach custom storage anyway. |
| Kafka | A generated file-based change feed demonstrates the same CDC concepts at zero cost. |
| Airflow | Lakeflow Jobs is the native orchestrator and is what this project is meant to show. |
| dbt | Project 1 covers it. Adding it here would dilute the PySpark evidence this project exists to provide. |
| Terraform | Declarative Automation Bundles is the native IaC path and needs no separate tool. As of CLI v1.15.0 bundles default to the `direct` deployment engine, so Terraform is not even an implementation detail underneath any more. |
| Power BI | Project 1 covers it. AI/BI + Genie keeps this project Databricks-native. |
| A large dataset | The objective is engineering evidence, not benchmarking. ~550K rows on a quota-limited account is the right size. |
| Olist `geolocation` | ~1M rows — two thirds of the raw dataset — and no dimension, fact or view joins to it. `dim_customer` carries the zip prefix directly. Ingesting it would spend most of the quota on a table nothing reads, so it is opt-in via `OPTIONAL_TABLES`. |

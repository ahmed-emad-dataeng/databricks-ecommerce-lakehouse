"""SCD Type 2 for dim_customer, hand-rolled on Delta MERGE.

Split deliberately into two layers:

  classify_changes()  -- pure, no I/O, fully unit-tested. All the correctness
                         risk lives here: deciding which incoming rows are new,
                         changed, unchanged or deleted.
  apply_scd2()        -- thin Delta MERGE + append driver. Hard to unit-test
                         without a metastore, so it is kept as dumb as possible.

The unchanged -> no-op path is what makes the pipeline re-runnable: replaying the
same batch must produce zero new versions. tests/test_scd2.py asserts that.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

ACTION = "_scd_action"
ROW_HASH = "_row_hash"

NEW = "new"
CHANGED = "changed"
UNCHANGED = "unchanged"
DELETED = "deleted"


def conform_to_table(spark, df: DataFrame, table: str) -> DataFrame:
    """Project `df` onto `table`'s columns, cast to its declared types.

    Selecting by name alone is not enough: computed columns routinely carry
    wider types than the DDL declares (a sum of DECIMAL(10,2) widens well past
    DECIMAL(12,2); CSV-derived values arrive as strings), and Delta's
    append-time schema enforcement rejects the mismatch. Casting explicitly
    makes every write match the contract in the DDL.
    """
    fields = spark.table(table).schema.fields
    missing = {f.name for f in fields} - set(df.columns)
    if missing:
        raise ValueError(f"{table}: source frame is missing {sorted(missing)}")
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in fields])


def add_row_hash(df: DataFrame, tracked_cols: tuple[str, ...]) -> DataFrame:
    """Hash the tracked attributes so change detection is one comparison.

    NULLs are coalesced to a sentinel before hashing: without it, xxhash64 over
    a NULL yields NULL, the hashes never compare equal, and every unchanged row
    looks changed -- which would produce a new dimension version on every run
    and quietly destroy idempotency.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in tracked_cols]
    return df.withColumn(ROW_HASH, F.xxhash64(F.concat_ws("||", *parts)))


def resolve_against_current(
    incoming: DataFrame,
    current: DataFrame,
    natural_key: str,
    attribute_cols: tuple[str, ...],
) -> DataFrame:
    """Overlay a CDC change row onto the customer's current dimension row.

    A change row carries only the columns the source system actually sent, but a
    new SCD2 version has to be a COMPLETE dimension row. So per attribute:

      * carried by the change  -> take the change; NULL means "no change" and
                                  inherits (the IGNORE NULL UPDATES semantics
                                  that AUTO CDC spells out explicitly)
      * omitted by the change  -> inherit from the version being superseded
      * brand-new key          -> NULL, which is honest: nothing is known yet

    Without this, a city change would blank out the customer's lifetime value
    and order count, and tracking an attribute the feed never sends (here,
    the derived customer_segment) would fail outright on a missing column.
    """
    cur = current.select(
        F.col(natural_key).alias("_nk"),
        *[F.col(c).alias(f"_cur_{c}") for c in attribute_cols],
    )
    joined = incoming.join(cur, incoming[natural_key] == cur["_nk"], "left")

    resolved = [
        (
            F.coalesce(F.col(c), F.col(f"_cur_{c}")).alias(c)
            if c in incoming.columns
            else F.col(f"_cur_{c}").alias(c)
        )
        for c in attribute_cols
    ]
    passthrough = [F.col(c) for c in incoming.columns if c not in attribute_cols]
    return joined.select(*passthrough, *resolved)


def classify_changes(
    current: DataFrame,
    incoming: DataFrame,
    natural_key: str,
    tracked_cols: tuple[str, ...],
    op_col: str | None = None,
) -> DataFrame:
    """Label each incoming row against the current dimension state.

    Args:
        current: the *current* rows of the dimension (is_current = true only),
            carrying `natural_key` and `_row_hash`.
        incoming: one row per natural key, already de-duplicated.
        natural_key: e.g. "customer_unique_id".
        tracked_cols: attributes whose change opens a new version.
        op_col: optional CDC operation column; a value of "D" marks a delete.

    Returns:
        `incoming` plus `_scd_action` in {new, changed, unchanged, deleted}.
    """
    incoming_hashed = add_row_hash(incoming, tracked_cols)

    already_deleted = (
        F.col("is_deleted") if "is_deleted" in current.columns else F.lit(False)
    )
    current_state = current.select(
        F.col(natural_key).alias("_cur_key"),
        F.col(ROW_HASH).alias("_cur_hash"),
        F.coalesce(already_deleted, F.lit(False)).alias("_cur_deleted"),
    )

    joined = incoming_hashed.join(
        current_state,
        incoming_hashed[natural_key] == current_state["_cur_key"],
        "left",
    )

    is_delete: Column
    if op_col and op_col in incoming.columns:
        is_delete = F.upper(F.col(op_col)) == "D"
    else:
        is_delete = F.lit(False)

    action = (
        # Re-deleting an already-deleted key is a no-op, not a second tombstone.
        # Without this, replaying a change file appends one tombstone per run,
        # dim_customer grows on every execution, and the idempotency check fails
        # -- which is precisely the class of bug that check exists to catch.
        F.when(
            is_delete
            & F.col("_cur_key").isNotNull()
            & F.col("_cur_deleted"),
            F.lit(UNCHANGED),
        )
        # A delete for a key we've never seen is also a no-op, not a tombstone.
        .when(is_delete & F.col("_cur_key").isNotNull(), F.lit(DELETED))
        .when(is_delete, F.lit(UNCHANGED))
        .when(F.col("_cur_key").isNull(), F.lit(NEW))
        .when(F.col(ROW_HASH) != F.col("_cur_hash"), F.lit(CHANGED))
        .otherwise(F.lit(UNCHANGED))
    )

    return joined.withColumn(ACTION, action).drop(
        "_cur_key", "_cur_hash", "_cur_deleted"
    )


def build_new_versions(
    classified: DataFrame,
    natural_key: str,
    attribute_cols: tuple[str, ...],
    effective_from: Column,
    end_of_time: str = "9999-12-31 00:00:00",
) -> DataFrame:
    """Rows to append: a version for every new/changed key, plus tombstones.

    A delete becomes a tombstone version (is_current = true, is_deleted = true)
    rather than simply closing the previous row. That keeps point-in-time joins
    working -- an order placed before the deletion still resolves to the
    attributes the customer had at the time -- while `is_current AND NOT
    is_deleted` gives the live population.
    """
    to_insert = classified.filter(F.col(ACTION).isin(NEW, CHANGED, DELETED))

    return to_insert.select(
        F.col(natural_key),
        *[F.col(c) for c in attribute_cols],
        F.col(ROW_HASH),
        effective_from.alias("effective_from"),
        F.lit(end_of_time).cast("timestamp").alias("effective_to"),
        F.lit(True).alias("is_current"),
        (F.col(ACTION) == DELETED).alias("is_deleted"),
        F.col(ACTION).alias("_change_reason"),
    )


def keys_to_close(classified: DataFrame, natural_key: str) -> DataFrame:
    """Natural keys whose current version must be closed out.

    Only changed and deleted keys. `new` has nothing to close; `unchanged` must
    be left strictly alone -- that is the no-op guarantee.

    `effective_from` is carried through as `closed_at` when present, because the
    MERGE needs it to stamp effective_to on the row it closes. Projecting it away
    here and re-referencing it afterwards is exactly what broke this: the frame
    no longer had the column, and the failure surfaced as UNRESOLVED_COLUMN from
    inside a MERGE rather than anywhere near this function.
    """
    cols = [F.col(natural_key)]
    if "effective_from" in classified.columns:
        cols.append(F.col("effective_from").alias("closed_at"))
    return (
        classified.filter(F.col(ACTION).isin(CHANGED, DELETED))
        .select(*cols)
        .distinct()
    )


def apply_scd2(
    spark,
    target_table: str,
    classified: DataFrame,
    natural_key: str,
    attribute_cols: tuple[str, ...],
    effective_from: Column,
    staging_table: str,
    surrogate_key: str = "customer_sk",
) -> dict[str, int]:
    """Close superseded versions, then append new ones.

    Two passes rather than one MERGE on purpose: a single MERGE cannot both
    update an existing target row and insert a second row for the same key --
    Delta raises on multiple source matches. Closing first, appending second is
    the standard resolution and stays idempotent because `classified` marks
    replayed rows as `unchanged`, so both passes see an empty set.

    `classified` is materialised to `staging_table` before use. It gets scanned
    three times (counts, closing, appending) and serverless compute blocks
    df.cache()/persist(), so a staging table is the available way to avoid
    recomputing the join three times.
    """
    classified.withColumn("effective_from", effective_from).write.mode(
        "overwrite"
    ).option("overwriteSchema", "true").saveAsTable(staging_table)
    staged = spark.table(staging_table)

    counts = dict.fromkeys((NEW, CHANGED, UNCHANGED, DELETED), 0)
    for row in staged.groupBy(ACTION).count().collect():
        counts[row[ACTION]] = row["count"]

    if counts[CHANGED] or counts[DELETED]:
        keys_to_close(staged, natural_key).createOrReplaceTempView("_scd_closing")
        spark.sql(f"""
            MERGE INTO {target_table} AS tgt
            USING _scd_closing AS src
              ON tgt.{natural_key} = src.{natural_key}
             AND tgt.is_current = true
            WHEN MATCHED THEN UPDATE SET
              tgt.is_current   = false,
              tgt.effective_to = src.closed_at
        """)

    if counts[NEW] or counts[CHANGED] or counts[DELETED]:
        new_versions = build_new_versions(
            staged, natural_key, attribute_cols, F.col("effective_from")
        )
        # Surrogate key is per *version*, so it hashes key + effective_from.
        with_sk = new_versions.withColumn(
            surrogate_key,
            F.xxhash64(
                F.concat_ws(
                    "||", F.col(natural_key), F.col("effective_from").cast("string")
                )
            ),
        )
        conform_to_table(spark, with_sk, target_table).write.mode(
            "append"
        ).saveAsTable(target_table)

    return counts

"""Tests for SCD Type 2 change classification.

The three transitions below are the ones that matter:

  new       -> a version is opened
  changed   -> the old version is closed and a new one opened
  unchanged -> NOTHING happens

The third is the one that makes the pipeline re-runnable. If a replayed batch
classifies as `changed`, every job run appends a spurious dimension version and
the idempotency guarantee is gone -- so it gets its own test, plus a NULL-handling
test for the hash bug that causes it.
"""

from pyspark.sql import functions as F

from src.gold.scd2 import (
    ACTION,
    CHANGED,
    DELETED,
    NEW,
    ROW_HASH,
    UNCHANGED,
    add_row_hash,
    build_new_versions,
    classify_changes,
    keys_to_close,
)

TRACKED = ("city", "segment")
NK = "customer_unique_id"

INCOMING_SCHEMA = f"{NK} string, city string, segment string, op string"


def _current(spark, rows):
    """Build a `current` dimension slice with hashes already computed."""
    df = spark.createDataFrame(rows, f"{NK} string, city string, segment string")
    return add_row_hash(df, TRACKED)


def _classify(spark, current_rows, incoming_rows):
    current = _current(spark, current_rows)
    incoming = spark.createDataFrame(incoming_rows, INCOMING_SCHEMA)
    out = classify_changes(current, incoming, NK, TRACKED, op_col="op")
    return {r[NK]: r[ACTION] for r in out.collect()}


# --- The three transitions ------------------------------------------------
def test_unseen_key_is_new(spark):
    actions = _classify(spark, [], [("c1", "cairo", "new", "I")])
    assert actions == {"c1": NEW}


def test_changed_tracked_attribute_is_changed(spark):
    actions = _classify(
        spark,
        [("c1", "cairo", "new")],
        [("c1", "alexandria", "new", "U")],
    )
    assert actions == {"c1": CHANGED}


def test_identical_row_is_unchanged_so_a_replay_is_a_no_op(spark):
    """Replaying the same batch must produce no new version. This is the
    guarantee the `idempotency_check` job task depends on."""
    actions = _classify(
        spark,
        [("c1", "cairo", "new")],
        [("c1", "cairo", "new", "U")],
    )
    assert actions == {"c1": UNCHANGED}


# --- The traps that break the no-op guarantee ----------------------------
def test_null_attributes_hash_equal_so_null_rows_are_not_falsely_changed(spark):
    """xxhash64(NULL) is NULL, so an un-coalesced hash never compares equal and
    every row with a NULL attribute looks changed on every run."""
    actions = _classify(
        spark,
        [("c1", None, "new")],
        [("c1", None, "new", "U")],
    )
    assert actions == {"c1": UNCHANGED}


def test_untracked_column_change_does_not_open_a_version(spark):
    """`op` differs between the two rows but is not a tracked attribute."""
    actions = _classify(
        spark,
        [("c1", "cairo", "new")],
        [("c1", "cairo", "new", "I")],
    )
    assert actions == {"c1": UNCHANGED}


def test_row_hash_is_order_sensitive_across_columns(spark):
    """Guards against a concat_ws collision: ('a','bc') must not hash like
    ('ab','c')."""
    df = spark.createDataFrame(
        [("k1", "a", "bc"), ("k2", "ab", "c")],
        f"{NK} string, city string, segment string",
    )
    hashes = [r[ROW_HASH] for r in add_row_hash(df, TRACKED).collect()]
    assert hashes[0] != hashes[1]


# --- Deletes --------------------------------------------------------------
def test_delete_of_known_key_is_deleted(spark):
    actions = _classify(
        spark, [("c1", "cairo", "new")], [("c1", "cairo", "new", "D")]
    )
    assert actions == {"c1": DELETED}


def test_delete_of_unknown_key_is_a_no_op_not_a_tombstone(spark):
    actions = _classify(spark, [], [("ghost", "cairo", "new", "D")])
    assert actions == {"ghost": UNCHANGED}


# --- What actually gets written ------------------------------------------
def test_only_changed_and_deleted_keys_are_closed(spark):
    current = _current(spark, [("c1", "cairo", "new"), ("c2", "giza", "new")])
    incoming = spark.createDataFrame(
        [
            ("c1", "alexandria", "new", "U"),  # changed  -> close
            ("c2", "giza", "new", "U"),        # unchanged -> leave alone
            ("c3", "luxor", "new", "I"),       # new       -> nothing to close
            ("c4", "aswan", "new", "D"),       # unknown delete -> no-op
        ],
        INCOMING_SCHEMA,
    )
    classified = classify_changes(current, incoming, NK, TRACKED, op_col="op")

    closing = {r[NK] for r in keys_to_close(classified, NK).collect()}
    assert closing == {"c1"}


def test_new_versions_are_open_ended_and_flag_tombstones(spark):
    current = _current(spark, [("c1", "cairo", "new")])
    incoming = spark.createDataFrame(
        [
            ("c1", "cairo", "new", "D"),   # tombstone
            ("c2", "luxor", "new", "I"),   # brand new
            ("c3", "giza", "new", "U"),    # new key arriving as an update
        ],
        INCOMING_SCHEMA,
    )
    classified = classify_changes(current, incoming, NK, TRACKED, op_col="op")
    versions = build_new_versions(
        classified, NK, TRACKED, F.lit("2017-06-01 00:00:00").cast("timestamp")
    )
    rows = {r[NK]: r for r in versions.collect()}

    assert set(rows) == {"c1", "c2", "c3"}
    # Every appended version is the current one and open-ended.
    assert all(r["is_current"] for r in rows.values())
    assert all(r["effective_to"].year == 9999 for r in rows.values())
    # A deleted customer keeps its attributes but is flagged, so point-in-time
    # joins still resolve while `is_current AND NOT is_deleted` excludes it.
    assert rows["c1"]["is_deleted"] is True
    assert rows["c1"]["city"] == "cairo"
    assert rows["c2"]["is_deleted"] is False


def test_unchanged_keys_produce_no_versions_at_all(spark):
    current = _current(spark, [("c1", "cairo", "new")])
    incoming = spark.createDataFrame([("c1", "cairo", "new", "U")], INCOMING_SCHEMA)
    classified = classify_changes(current, incoming, NK, TRACKED, op_col="op")

    assert keys_to_close(classified, NK).count() == 0
    assert build_new_versions(
        classified, NK, TRACKED, F.current_timestamp()
    ).count() == 0

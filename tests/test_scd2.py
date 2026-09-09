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
    resolve_against_current,
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


# --- Overlaying a partial CDC row onto the current version -----------------
# A change feed sends only what the source system changed. A new SCD2 version
# must still be a complete dimension row, so omitted attributes inherit.
ATTRS = ("city", "segment", "lifetime_value")


def _current_full(spark):
    return spark.createDataFrame(
        [("c1", "cairo", "high_value", 900.0)],
        f"{NK} string, city string, segment string, lifetime_value double",
    )


def test_omitted_attribute_is_inherited_not_blanked(spark):
    """The feed carries city but not segment or lifetime_value. Those must come
    from the version being superseded -- a city change must not wipe out a
    customer's lifetime value."""
    incoming = spark.createDataFrame(
        [("c1", "alexandria", "U")], f"{NK} string, city string, op string"
    )
    got = resolve_against_current(
        incoming, _current_full(spark), NK, ATTRS
    ).collect()[0]

    assert got["city"] == "alexandria"        # changed
    assert got["segment"] == "high_value"     # inherited
    assert got["lifetime_value"] == 900.0     # inherited, not NULL
    assert got["op"] == "U"                   # passthrough preserved


def test_null_in_the_feed_means_no_change_and_inherits(spark):
    """IGNORE NULL UPDATES: a NULL in a carried column is "unchanged", not
    "set to NULL"."""
    incoming = spark.createDataFrame(
        [("c1", None, "U")], f"{NK} string, city string, op string"
    )
    got = resolve_against_current(
        incoming, _current_full(spark), NK, ATTRS
    ).collect()[0]

    assert got["city"] == "cairo"  # inherited, NOT blanked


def test_brand_new_key_gets_nulls_rather_than_failing(spark):
    """Nothing is known about a key with no current row. NULL is the honest
    answer; effective_from does not depend on these, so no orphan risk."""
    incoming = spark.createDataFrame(
        [("brand_new", "luxor", "I")], f"{NK} string, city string, op string"
    )
    got = resolve_against_current(
        incoming, _current_full(spark), NK, ATTRS
    ).collect()[0]

    assert got["city"] == "luxor"
    assert got["segment"] is None
    assert got["lifetime_value"] is None


def test_inheriting_a_carry_forward_value_is_not_a_change(spark):
    """The whole point of separating tracked from carry-forward: resolving an
    omitted attribute must not look like a change and must not open a version."""
    current = _current_full(spark)
    incoming = spark.createDataFrame(
        [("c1", "cairo", "U")], f"{NK} string, city string, op string"
    )
    resolved = resolve_against_current(incoming, current, NK, ATTRS)

    # Track only city/segment; lifetime_value is carry-forward.
    classified = classify_changes(
        add_row_hash(current, ("city", "segment")),
        resolved,
        NK,
        ("city", "segment"),
        op_col="op",
    )
    assert {r[NK]: r[ACTION] for r in classified.collect()} == {"c1": UNCHANGED}


def test_keys_to_close_carries_effective_from_as_closed_at(spark):
    """Regression: keys_to_close projected effective_from away, and apply_scd2
    then referenced it to stamp effective_to on the closed row. The failure
    surfaced as UNRESOLVED_COLUMN from inside a Delta MERGE, nowhere near the
    projection that caused it."""
    current = _current(spark, [("c1", "cairo", "new")])
    incoming = spark.createDataFrame(
        [("c1", "alexandria", "new", "U")], INCOMING_SCHEMA
    )
    classified = classify_changes(current, incoming, NK, TRACKED, op_col="op")
    staged = classified.withColumn(
        "effective_from", F.lit("2018-10-01 00:00:00").cast("timestamp")
    )

    closing = keys_to_close(staged, NK)

    assert "closed_at" in closing.columns
    row = closing.collect()[0]
    assert row[NK] == "c1"
    assert row["closed_at"].year == 2018


def test_keys_to_close_works_without_effective_from(spark):
    """The column is optional: the pure-classification tests call this without
    an effective_from and must keep working."""
    current = _current(spark, [("c1", "cairo", "new")])
    incoming = spark.createDataFrame(
        [("c1", "alexandria", "new", "U")], INCOMING_SCHEMA
    )
    closing = keys_to_close(
        classify_changes(current, incoming, NK, TRACKED, op_col="op"), NK
    )

    assert "closed_at" not in closing.columns
    assert [r[NK] for r in closing.collect()] == ["c1"]

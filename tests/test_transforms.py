"""Tests for the pure silver transforms."""

from pyspark.sql import functions as F

from src.silver.transforms import (
    DQRule,
    aggregate_geolocation,
    count_rule_failures,
    dedupe_by_key,
    normalise_strings,
    parse_timestamps,
    split_by_rules,
)


def test_normalise_strings_collapses_casing_padding_and_blanks(spark):
    df = spark.createDataFrame(
        [("sao paulo",), ("Sao Paulo ",), ("SAO  PAULO",), ("   ",), (None,)],
        "city string",
    )
    out = normalise_strings(df, ("city",))
    values = [r["city"] for r in out.collect()]

    # The three spellings must collapse to one group-by key...
    assert values[:3] == ["sao paulo"] * 3
    # ...and whitespace-only must become NULL, not ''.
    assert values[3] is None
    assert values[4] is None


def test_parse_timestamps_nulls_unparseable_instead_of_raising(spark):
    df = spark.createDataFrame(
        [("2017-05-16 15:05:35",), ("not-a-date",), (None,)], "ts string"
    )
    out = parse_timestamps(df, ("ts",)).collect()

    assert out[0]["ts"].year == 2017
    assert out[1]["ts"] is None
    assert out[2]["ts"] is None


def test_dedupe_by_key_keeps_latest_by_sequence_column(spark):
    df = spark.createDataFrame(
        [
            ("o1", "created", "2017-01-01 00:00:00"),
            ("o1", "delivered", "2017-03-01 00:00:00"),
            ("o1", "shipped", "2017-02-01 00:00:00"),
            ("o2", "created", "2017-01-01 00:00:00"),
        ],
        "order_id string, status string, updated_at string",
    )
    out = dedupe_by_key(df, ("order_id",), order_by="updated_at")
    got = {r["order_id"]: r["status"] for r in out.collect()}

    assert out.count() == 2
    assert got["o1"] == "delivered"  # latest wins, not first-seen


def test_dedupe_by_key_prefers_rows_with_a_timestamp_over_nulls(spark):
    df = spark.createDataFrame(
        [("o1", "real", "2017-01-01 00:00:00"), ("o1", "missing", None)],
        "order_id string, status string, updated_at string",
    )
    out = dedupe_by_key(df, ("order_id",), order_by="updated_at").collect()

    # desc_nulls_last: a row with an actual timestamp must not lose to a NULL.
    assert len(out) == 1
    assert out[0]["status"] == "real"


def test_split_by_rules_routes_failures_and_records_which_rule_failed(spark):
    df = spark.createDataFrame(
        [("o1", 100.0), ("o2", -5.0), (None, 50.0)],
        "order_id string, price double",
    )
    rules = [
        DQRule("orders.order_id_not_null", "orders", "", F.col("order_id").isNotNull()),
        DQRule("orders.price_non_negative", "orders", "", F.col("price") >= 0),
    ]
    clean, quarantined = split_by_rules(df, rules)

    assert [r["order_id"] for r in clean.collect()] == ["o1"]
    failed = {
        (r["order_id"], tuple(r["_dq_failed_rules"])) for r in quarantined.collect()
    }
    assert ("o2", ("orders.price_non_negative",)) in failed
    assert (None, ("orders.order_id_not_null",)) in failed


def test_split_by_rules_treats_null_predicate_result_as_a_failure(spark):
    """`price >= 0` is NULL when price is NULL. That must quarantine, not pass.

    This is the single most common silent bug in a hand-rolled DQ layer.
    """
    df = spark.createDataFrame([("o1", None)], "order_id string, price double")
    rules = [DQRule("orders.price_non_negative", "orders", "", F.col("price") >= 0)]

    clean, quarantined = split_by_rules(df, rules)

    assert clean.count() == 0
    assert quarantined.count() == 1


def test_split_by_rules_keeps_warn_severity_rows_in_the_clean_set(spark):
    df = spark.createDataFrame([("o1", -5.0)], "order_id string, price double")
    rules = [
        DQRule("orders.price_non_negative", "orders", "", F.col("price") >= 0, "warn")
    ]
    clean, quarantined = split_by_rules(df, rules)

    assert clean.count() == 1
    assert quarantined.count() == 0


def test_aggregate_geolocation_collapses_to_one_row_per_zip_prefix(spark):
    df = spark.createDataFrame(
        [
            (1001, -23.5, -46.6, "sao paulo", "SP"),
            (1001, -23.7, -46.8, "sao paulo", "SP"),
            (2002, -22.9, -43.2, "rio de janeiro", "RJ"),
        ],
        "geolocation_zip_code_prefix int, geolocation_lat double, "
        "geolocation_lng double, geolocation_city string, geolocation_state string",
    )
    out = {r["geolocation_zip_code_prefix"]: r for r in aggregate_geolocation(df).collect()}

    assert len(out) == 2
    assert out[1001]["reading_count"] == 2
    assert out[1001]["latitude"] == -23.6  # centroid of the two readings
    assert out[1001]["city"] == "sao paulo"

def test_count_rule_failures_counts_warn_rules_too(spark):
    """Regression: counting failures from the quarantine table reported every
    `warn` rule as clean, because warn rows never enter quarantine. Counts must
    come from the full input instead."""
    df = spark.createDataFrame(
        [("o1", 100.0), ("o2", -5.0), ("o3", -7.0)],
        "order_id string, price double",
    )
    rules = [
        DQRule("orders.price_warn", "orders", "", F.col("price") >= 0, "warn"),
        DQRule("orders.id_reject", "orders", "", F.col("order_id").isNotNull()),
    ]

    counts = {r["rule_id"]: r["rows_failed"] for r in count_rule_failures(df, rules).collect()}

    # Two rows violate the warn rule and must be counted despite loading fine.
    assert counts["orders.price_warn"] == 2
    clean, quarantined = split_by_rules(df, rules)
    assert clean.count() == 3
    assert quarantined.count() == 0


def test_count_rule_failures_with_no_rules_returns_empty(spark):
    df = spark.createDataFrame([("o1",)], "order_id string")
    assert count_rule_failures(df, []).count() == 0

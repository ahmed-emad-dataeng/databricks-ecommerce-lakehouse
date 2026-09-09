"""Tests for catalog resolution.

Regression cover for a real bug: the catalog was a module-level constant, so a
job that passed `ecommerce_dev` as a parameter had it ignored and wrote to
`ecommerce` instead -- no error, just the wrong target. These tests pin the
call-time resolution that fixes it.

No Spark needed.
"""

import os

import pytest

from src import config


@pytest.fixture(autouse=True)
def _clean_env():
    """Isolate each test from the ambient catalog setting."""
    saved = os.environ.pop(config.CATALOG_ENV_VAR, None)
    yield
    os.environ.pop(config.CATALOG_ENV_VAR, None)
    if saved is not None:
        os.environ[config.CATALOG_ENV_VAR] = saved


def test_falls_back_to_default_when_unset():
    assert config.catalog() == config.DEFAULT_CATALOG
    assert config.fqn("gold", "fact_order") == f"{config.DEFAULT_CATALOG}.gold.fact_order"


def test_set_catalog_changes_every_derived_name():
    config.set_catalog("ecommerce_dev")

    assert config.catalog() == "ecommerce_dev"
    assert config.fqn("gold", "fact_order") == "ecommerce_dev.gold.fact_order"
    assert config.volume_root() == "/Volumes/ecommerce_dev/landing/raw"
    assert config.path_olist() == "/Volumes/ecommerce_dev/landing/raw/olist"
    assert config.path_cdc().endswith("/ecommerce_dev/landing/raw/cdc")
    assert config.path_checkpoints().endswith("/_checkpoints")
    assert config.path_schemas().endswith("/_schemas")


def test_source_glob_follows_the_live_catalog():
    """The dataclass is frozen, so source_glob must be a property, not a field."""
    table = config.SOURCE_TABLES[0]

    config.set_catalog("cat_a")
    first = table.source_glob
    config.set_catalog("cat_b")
    second = table.source_glob

    assert "/cat_a/" in first
    assert "/cat_b/" in second
    assert first != second


def test_empty_env_var_falls_back_rather_than_yielding_empty_catalog():
    """An unset job parameter arrives as "" -- that must not produce `.gold.x`."""
    os.environ[config.CATALOG_ENV_VAR] = ""

    assert config.catalog() == config.DEFAULT_CATALOG
    assert not config.fqn("gold", "x").startswith(".")


def test_geolocation_is_not_in_the_default_load():
    """~1M rows, two thirds of the dataset, and nothing in the model reads it."""
    assert "geolocation" not in {t.name for t in config.SOURCE_TABLES}
    assert "geolocation" in {t.name for t in config.OPTIONAL_TABLES}
    assert len(config.SOURCE_TABLES) == 8

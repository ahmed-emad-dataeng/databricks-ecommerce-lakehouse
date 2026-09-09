"""Tests for splitting the sql/views/ files.

These run locally -- no Spark -- and they cover a bug that would otherwise only
show up as a Spark parse error inside a job task.
"""

from src.ops.sql_files import (
    load_view_statements,
    split_statements,
    strip_line_comments,
    views_dir,
)


def test_does_not_split_on_a_semicolon_in_a_comment():
    """The actual bug: two files in sql/views/ have a prose semicolon in a
    comment. Naive split(";") turns the rest of that sentence into bare SQL."""
    body = """-- Olist issues a new customer_id per order; only unique_id is stable
CREATE OR REPLACE VIEW v AS SELECT 1;"""

    got = split_statements(body)

    assert len(got) == 1
    assert got[0].startswith("CREATE OR REPLACE VIEW")
    assert "only unique_id is stable" not in got[0]


def test_splits_multiple_statements_in_order():
    body = "CREATE VIEW a AS SELECT 1;\nCREATE VIEW b AS SELECT 2;\n"
    assert split_statements(body) == ["CREATE VIEW a AS SELECT 1", "CREATE VIEW b AS SELECT 2"]


def test_drops_trailing_and_comment_only_fragments():
    body = "-- just a header\n\nCREATE VIEW a AS SELECT 1;\n\n-- trailing note\n"
    assert split_statements(body) == ["CREATE VIEW a AS SELECT 1"]


def test_strips_inline_trailing_comments():
    body = "SELECT 1 FROM t HAVING count(*) >= 30;  -- suppress tiny groups\n"
    got = split_statements(body)
    assert len(got) == 1
    assert "suppress" not in got[0]


def test_views_dir_resolves_without_relying_on_cwd():
    d = views_dir()
    assert d.is_dir(), f"{d} should exist regardless of the process cwd"
    assert list(d.glob("*.sql")), "no view files found"


def test_every_view_file_yields_parseable_looking_statements():
    """Guards the real files, not just synthetic input: every statement must
    start with a SQL keyword. A mis-split leaves prose at the front."""
    statements = load_view_statements("ecommerce_test")

    assert len(statements) >= 13, f"expected >=13 statements, got {len(statements)}"
    for name, sql in statements:
        first = sql.split(None, 1)[0].upper()
        assert first in {"CREATE", "SELECT", "WITH", "ALTER", "DROP"}, (
            f"{name}: statement starts with {first!r}, which means the split "
            f"left non-SQL at the front: {sql[:80]!r}"
        )


def test_catalog_placeholder_is_substituted_everywhere():
    statements = load_view_statements("my_cat")
    joined = " ".join(sql for _, sql in statements)
    assert "${catalog}" not in joined
    assert "my_cat.gold" in joined

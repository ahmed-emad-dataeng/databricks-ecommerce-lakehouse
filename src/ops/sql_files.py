"""Load and split the .sql files in sql/views/.

Small on purpose, and a separate module rather than inline notebook code so the
splitting can be unit-tested. Naive `body.split(";")` is wrong here and fails
loudly only if you are lucky:

  -- Olist issues a NEW customer_id for every order; only customer_unique_id is
  -- stable across orders.
  CREATE OR REPLACE VIEW ...;

Splitting that on ";" yields a fragment starting with the bare prose
" only customer_unique_id is" followed by the CREATE -- a parse error, from a
semicolon in an English sentence. Two files in sql/views/ contain one.
"""

from __future__ import annotations

import pathlib
import re

LINE_COMMENT = re.compile(r"--.*$")


def strip_line_comments(body: str) -> str:
    """Remove `--` line comments.

    Applied BEFORE splitting so a prose semicolon inside a comment cannot split
    a statement. The comments are documentation for whoever reads the repo, not
    for the engine -- each view carries its own COMMENT clause.

    Caveat: a literal `--` inside a string literal would also be stripped. No
    file in sql/views/ contains one, and `test_does_not_split_on_a_semicolon_in
    _a_comment` would start failing loudly if that changed.
    """
    return "\n".join(LINE_COMMENT.sub("", line) for line in body.splitlines())


def split_statements(body: str) -> list[str]:
    """Split one .sql file into executable statements, in order."""
    return [s.strip() for s in strip_line_comments(body).split(";") if s.strip()]


def views_dir() -> pathlib.Path:
    """Locate sql/views/ from this module, not from the process cwd.

    Deriving it from `os.getcwd()` assumes the notebook's working directory,
    which is true in a job task and not true in a REPL or a test. This is
    stable either way.
    """
    return pathlib.Path(__file__).resolve().parent.parent.parent / "sql" / "views"


def load_view_statements(catalog: str) -> list[tuple[str, str]]:
    """Return (filename, statement) pairs for every view file, in filename order.

    `${catalog}` is substituted so the views follow the run's target catalog the
    same way the Python layer does.
    """
    out: list[tuple[str, str]] = []
    for path in sorted(views_dir().glob("*.sql")):
        body = path.read_text(encoding="utf-8").replace("${catalog}", catalog)
        for statement in split_statements(body):
            out.append((path.name, statement))
    return out

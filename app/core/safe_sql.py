"""
Safe SQL identifier utilities.

Validates and quotes SQL identifiers (table/column names) to prevent
SQL injection via f-string interpolation. Use ``qi()`` (quote identifier)
everywhere a table or column name is dynamically inserted into a SQL string.
"""

import re

# Only alphanumeric + underscore; must start with letter or underscore
_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Allowed comparison operators in DQ rules
ALLOWED_OPERATORS = frozenset({"<", ">", "<=", ">=", "=", "!=", "<>"})


def safe_identifier(name: str) -> str:
    """Validate and double-quote a SQL identifier.

    Raises ValueError if the name contains dangerous characters.
    Returns the name wrapped in double-quotes for PostgreSQL.
    """
    if not name or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return f'"{name}"'


# Short alias for concise usage across the codebase
qi = safe_identifier


def safe_identifiers(*names: str) -> list[str]:
    """Validate and quote multiple SQL identifiers."""
    return [safe_identifier(n) for n in names]


def safe_operator(op: str) -> str:
    """Validate a SQL comparison operator against an allowlist.

    Raises ValueError if the operator is not in ALLOWED_OPERATORS.
    """
    if op not in ALLOWED_OPERATORS:
        raise ValueError(f"Invalid SQL operator: {op!r}")
    return op


def safe_int(value, label: str = "value") -> int:
    """Coerce and validate an integer for use in SQL (e.g., timeouts, intervals).

    Raises ValueError if the value cannot be safely converted.
    """
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid integer for {label}: {value!r}")
    return result

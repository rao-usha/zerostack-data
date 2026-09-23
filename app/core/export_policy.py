"""
What the generic table export may serve (SPEC_127).

`/export` used to serve every table in the public schema, including
`users.password_hash`, plaintext password-reset tokens, API-key hashes and
lead PII. This module is the single gate: a table is exportable only if it is
not on the deny list, its name does not look like auth/lead/secret storage,
and none of its columns look like a credential. It errs on the side of
refusing — an ops table that trips a pattern can be exported with SQL by an
admin, a leaked credential cannot be un-leaked.
"""

import re
from typing import Iterable

DENIED_TABLES = frozenset(
    {
        # auth / sessions
        "users",
        "password_reset_tokens",
        "refresh_tokens",
        "login_codes",
        "sessions",
        # API keys and their metering
        "api_keys",
        "api_usage",
        "api_key_usage",
        "rate_limit_buckets",
        # stored third-party credentials
        "source_api_keys",
        # leads, customers, visitor telemetry (PII / IPs)
        "leads",
        "playground_leads",
        "playground_runs",
        "playground_quota_buckets",
        "diligence_orders",
        "atlas_events",
        "atlas_queries",
        "atlas_card_feedback",
        # invitations carry bearer tokens; webhooks carry secrets/targets
        "workspace_invitations",
        "workspace_members",
        "webhooks",
        "webhook_deliveries",
        # schema bookkeeping
        "alembic_version",
    }
)

# Whole-word-ish fragments of a table name. `(^|_)` / `($|_)` keep
# `leadership_changes` exportable while `crm_leads` is not.
_DENIED_TABLE_PATTERN = re.compile(
    r"(^|_)("
    r"password\w*|secrets?|tokens?|credentials?|sessions?|leads?|api_keys?|apikeys?"
    r")($|_)",
    re.IGNORECASE,
)

# Column names that mean "this table stores something you log in with".
# Generic content hashes (`content_hash`, `row_hash`) are deliberately allowed.
SENSITIVE_COLUMN_PATTERN = re.compile(
    r"^("
    r"password\w*|passwd|\w*_password"
    r"|\w*secret\w*"
    r"|(\w+_)?tokens?(_\w+)?"
    r"|\w*api_?key\w*"
    r"|key_hash|code_hash|token_hash"
    r"|credentials?\w*|\w*_credentials?"
    r"|private_key|otp\w*"
    r")$",
    re.IGNORECASE,
)


def is_sensitive_column(column: str) -> bool:
    return bool(SENSITIVE_COLUMN_PATTERN.match(column or ""))


def is_denied_table_name(table: str) -> bool:
    name = (table or "").lower()
    if not name or name.startswith("_") or name in DENIED_TABLES:
        return True
    return bool(_DENIED_TABLE_PATTERN.search(name))


def is_exportable(table: str, columns: Iterable[str]) -> bool:
    """True when `table` (with these column names) may be exported/previewed."""
    if is_denied_table_name(table):
        return False
    return not any(is_sensitive_column(c) for c in columns)

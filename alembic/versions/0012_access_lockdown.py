"""SPEC_127: access lockdown -- user roles, hashed reset tokens, refresh audience.

The auth tables are created lazily by `AuthService._ensure_tables`, not by
`create_all`, so on a fresh database they do not exist yet when migrations run
at startup. Every statement is therefore guarded by `to_regclass(...)`; the
lazy `CREATE TABLE` already has the new shape.

- `users.role` ('admin' | 'user', default 'user'). Nobody is promoted here:
  admins come from `scripts/create_user.py --admin` or `ADMIN_EMAILS`.
- `password_reset_tokens.token` held the reset secret in plaintext and was
  readable through `/export/tables/password_reset_tokens/preview`. Every
  existing row is deleted (a leaked token must not stay valid), the column is
  dropped, and `token_hash` (SHA-256) replaces it.
- `refresh_tokens.audience` so a playground session can never be refreshed
  into a platform session. Existing rows are revoked: their `token_hash` was
  random and never matched a presented token, so none of them could work.
- Data-profile snapshots store the top values of string columns, and the
  profile endpoints used to profile any table on request (anonymously, before
  this spec). Snapshots of tables the export policy denies (users, reset
  tokens, API keys, ...) are deleted; the service now refuses to create them.
"""

from alembic import op
from sqlalchemy import text

from app.core.export_policy import is_exportable

revision = "0012_access_lockdown"
down_revision = "0011_pe_people_sec"
branch_labels = None
depends_on = None


UPGRADE_SQL = [
    # --- users.role --------------------------------------------------------
    """
    DO $$
    BEGIN
        IF to_regclass('public.users') IS NOT NULL THEN
            ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(20) NOT NULL DEFAULT 'user';
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'ck_users_role'
            ) THEN
                ALTER TABLE users ADD CONSTRAINT ck_users_role
                    CHECK (role IN ('admin', 'user'));
            END IF;
        END IF;
    END $$;
    """,
    # --- password reset tokens: plaintext -> sha256 ------------------------
    """
    DO $$
    BEGIN
        IF to_regclass('public.password_reset_tokens') IS NOT NULL THEN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'password_reset_tokens'
                  AND column_name = 'token'
            ) THEN
                DELETE FROM password_reset_tokens;
                ALTER TABLE password_reset_tokens DROP COLUMN token;
            END IF;
            ALTER TABLE password_reset_tokens
                ADD COLUMN IF NOT EXISTS token_hash VARCHAR(64);
            DELETE FROM password_reset_tokens WHERE token_hash IS NULL;
            ALTER TABLE password_reset_tokens ALTER COLUMN token_hash SET NOT NULL;
            DROP INDEX IF EXISTS idx_reset_tokens_token;
            CREATE UNIQUE INDEX IF NOT EXISTS uq_password_reset_tokens_token_hash
                ON password_reset_tokens (token_hash);
        END IF;
    END $$;
    """,
    # --- refresh tokens: audience + revoke the unusable legacy rows --------
    """
    DO $$
    BEGIN
        IF to_regclass('public.refresh_tokens') IS NOT NULL THEN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'refresh_tokens'
                  AND column_name = 'audience'
            ) THEN
                ALTER TABLE refresh_tokens
                    ADD COLUMN audience VARCHAR(20) NOT NULL DEFAULT 'nexdata-app';
                UPDATE refresh_tokens SET revoked_at = CURRENT_TIMESTAMP
                WHERE revoked_at IS NULL;
            END IF;
        END IF;
    END $$;
    """,
]

DOWNGRADE_SQL = [
    """
    DO $$
    BEGIN
        IF to_regclass('public.refresh_tokens') IS NOT NULL THEN
            ALTER TABLE refresh_tokens DROP COLUMN IF EXISTS audience;
        END IF;
        IF to_regclass('public.password_reset_tokens') IS NOT NULL THEN
            -- hashed tokens cannot be turned back into plaintext: invalidate
            DELETE FROM password_reset_tokens;
            DROP INDEX IF EXISTS uq_password_reset_tokens_token_hash;
            ALTER TABLE password_reset_tokens DROP COLUMN IF EXISTS token_hash;
            ALTER TABLE password_reset_tokens
                ADD COLUMN IF NOT EXISTS token VARCHAR(64) NOT NULL UNIQUE;
        END IF;
        IF to_regclass('public.users') IS NOT NULL THEN
            ALTER TABLE users DROP CONSTRAINT IF EXISTS ck_users_role;
            ALTER TABLE users DROP COLUMN IF EXISTS role;
        END IF;
    END $$;
    """,
]


def purge_denied_profiles(conn) -> int:
    """Delete profile snapshots (and their column stats) of denied tables.

    Returns the number of snapshots removed. No-op when the profiling tables
    do not exist yet.
    """
    if conn.execute(text("SELECT to_regclass('public.data_profile_snapshots')")).scalar() is None:
        return 0
    tables = [r[0] for r in conn.execute(
        text("SELECT DISTINCT table_name FROM data_profile_snapshots")
    )]
    denied = []
    for table in tables:
        columns = [r[0] for r in conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = :t"
            ),
            {"t": table},
        )]
        if not is_exportable(table, columns):
            denied.append(table)
    if not denied:
        return 0
    has_columns = conn.execute(
        text("SELECT to_regclass('public.data_profile_columns')")
    ).scalar() is not None
    removed = 0
    for table in denied:
        if has_columns:
            conn.execute(
                text(
                    "DELETE FROM data_profile_columns WHERE snapshot_id IN "
                    "(SELECT id FROM data_profile_snapshots WHERE table_name = :t)"
                ),
                {"t": table},
            )
        removed += conn.execute(
            text("DELETE FROM data_profile_snapshots WHERE table_name = :t"), {"t": table}
        ).rowcount or 0
    return removed


def upgrade() -> None:
    for stmt in UPGRADE_SQL:
        op.execute(stmt)
    purge_denied_profiles(op.get_bind())


def downgrade() -> None:
    for stmt in DOWNGRADE_SQL:
        op.execute(stmt)

"""
Create or update a Nexdata platform user (SPEC_127).

Self-registration is closed by default (ALLOW_SIGNUP=false), so this is how
accounts -- including the first admin -- come into existence.

    # inside the api container (DATABASE_URL + JWT settings come from its env)
    docker-compose exec api python scripts/create_user.py you@example.com --admin

    # password from the environment instead of a prompt (CI / automation)
    NEXDATA_NEW_PASSWORD=... python scripts/create_user.py you@example.com --admin

An existing account (including a passwordless playground account) gets the
new password and is marked verified. --admin promotes; omitting it never
demotes. The password is never echoed or logged.
"""

import argparse
import getpass
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _read_password(args) -> str:
    if args.password_stdin:
        return sys.stdin.readline().rstrip("\n")
    env_pw = os.environ.get("NEXDATA_NEW_PASSWORD")
    if env_pw:
        return env_pw
    first = getpass.getpass("Password (min 8 chars): ")
    second = getpass.getpass("Repeat password: ")
    if first != second:
        sys.exit("Passwords do not match.")
    return first


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Create or update a Nexdata user.")
    parser.add_argument("email")
    parser.add_argument("--name", default=None)
    parser.add_argument("--admin", action="store_true", help="grant role=admin")
    parser.add_argument(
        "--password-stdin",
        action="store_true",
        help="read the password from the first line of stdin",
    )
    args = parser.parse_args(argv)

    password = _read_password(args)
    if len(password) < 8:
        print("Password must be at least 8 characters.", file=sys.stderr)
        return 2

    from app.core.database import get_session_factory
    from app.users.auth import AuthService

    db = get_session_factory()()
    try:
        result = AuthService(db).create_user(
            args.email, password, name=args.name, admin=args.admin
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    finally:
        db.close()

    verb = "Created" if result["created"] else "Updated"
    print(f"{verb} user {result['email']} (id={result['id']}, role={result['role']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

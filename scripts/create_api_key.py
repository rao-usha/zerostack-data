"""
Mint an API key for operator tooling (SPEC_127).

Every internal route needs an admin principal. Browser sessions use a
60-minute JWT; skills, curl, scripts/nexdata_client.py and eval runs use an
admin-scope API key in the X-API-Key header instead:

    # inside the api container (DATABASE_URL comes from its env)
    docker-compose exec api python scripts/create_api_key.py you@example.com

    # then, on the host (e.g. in ~/.bashrc), for the tooling to pick up:
    export NEXDATA_API_KEY=nxd_...

The raw key is printed once and never stored (only its SHA-256). Revoke it
with DELETE /api/v1/api-keys/{id}. `--scope read` mints a /public-only key.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Create a Nexdata API key.")
    parser.add_argument("owner_email")
    parser.add_argument("--name", default="operator-cli")
    parser.add_argument(
        "--scope",
        default="admin",
        choices=("read", "write", "admin"),
        help="admin = internal routers (default); read/write = /api/v1/public only",
    )
    parser.add_argument("--expires-in-days", type=int, default=None)
    args = parser.parse_args(argv)

    from app.auth.api_keys import APIKeyCreate, APIKeyService
    from app.core.database import get_session_factory

    db = get_session_factory()()
    try:
        created = APIKeyService(db).create_key(
            APIKeyCreate(
                name=args.name,
                owner_email=args.owner_email,
                scope=args.scope,
                expires_in_days=args.expires_in_days,
            )
        )
    finally:
        db.close()

    print(f"Created API key id={created.id} scope={created.scope} name={created.name}")
    print("Store it now; it is not shown again:")
    print(created.key)
    return 0


if __name__ == "__main__":
    sys.exit(main())

---
name: seed
description: Run seed/demo data scripts to populate the database for a specific domain. Use when the user wants to load seed data, demo data, or initialize a domain.
allowed-tools:
  - Bash
argument-hint: "[domain: demo|pe|companies|3pl]"
---

Run the appropriate seed script to populate the database for the requested domain.

## Available seed scripts

| Domain | Script | What it does |
|--------|--------|-------------|
| `demo` | `scripts/populate_demo_data.py` | Load 20 sample data sources across all domains |
| `pe` | `scripts/seed_pe_firms.py` | Seed PE/VC firm records |
| `companies` | `scripts/seed_industrial_companies.py` | Seed industrial company records |
| `3pl` | Look for 3PL seed in scripts/ | Seed 3PL logistics companies |
| `full` | `scripts/seed_demo_data.py` | Full demo data setup |

## Behavior

1. If `$ARGUMENTS` specifies a domain, run the matching script. If not specified, ask which domain.

2. Run the script inside the API container:
   ```bash
   docker exec nexdata-api-1 python scripts/SCRIPT_NAME.py
   ```
   Or if the script uses the API endpoints, run it from the host with the right port.

3. After the script completes, show:
   - How many records were created/updated
   - Any errors encountered
   - A quick count of the affected tables (use the db-status approach)

4. If the script doesn't exist or fails, check:
   - `ls scripts/` for available scripts
   - Whether it needs to run inside the container vs from host
   - Whether the API needs to be running first

## Important
- Some scripts use `asyncio.run()` and need to run inside the container where the database is accessible
- Scripts that call API endpoints should use `http://localhost:8000` inside the container or `http://localhost:8001` from host
- Always show the script output to the user so they can verify

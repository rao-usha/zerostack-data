# Nexdata — Environment Setup Guide

## Environments

| Environment | Database | Port | Use case |
|---|---|---|---|
| **local** | Docker Postgres container | 5434 | Default dev — fully local |
| **cloud** | GCP Cloud SQL via proxy | 5435 | Shared access with collaborators |
| **test** | Docker Postgres (isolated) | 5436 | Automated testing |

---

## Quick Switch

```bash
# Switch to local (default)
source switch-env.sh local
docker-compose up -d

# Switch to cloud
source switch-env.sh cloud
# Start proxy first (see Cloud Setup below), then:
docker-compose --env-file .env.active up -d api worker frontend

# Switch to test
source switch-env.sh test
docker-compose --env-file .env.active up -d
```

Or manually: copy the env file you want to `.env` and restart.

---

## Local Setup (default)

Nothing extra needed. Just run:
```bash
docker-compose up -d
```
This starts Postgres + API + Worker + Frontend. Database on `localhost:5434`.

---

## Cloud Setup (GCP Cloud SQL)

Shared PostgreSQL on GCP. Access restricted to authorized Google accounts via IAM.

**Instance:** `nexdata-cloud:us-central1:nexdata-pg`
**Database:** `nexdata`
**User:** `nexdata` / Password: `Nex2026`
**Cost:** ~$7/mo

### First-time setup (~5 minutes)

#### 1. Install Google Cloud CLI

**Windows:** Download and run: https://cloud.google.com/sdk/docs/install

**Mac:**
```bash
brew install google-cloud-sdk
```

#### 2. Authenticate
```bash
gcloud auth login
gcloud auth application-default login
```
Both commands open your browser — sign in with the Google account that was granted access.

#### 3. Download Cloud SQL Auth Proxy

**Windows:**
```bash
curl -o cloud-sql-proxy.exe https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.x64.exe
```

**Mac (Apple Silicon):**
```bash
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.darwin.arm64
chmod +x cloud-sql-proxy
```

**Mac (Intel):**
```bash
curl -o cloud-sql-proxy https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.14.3/cloud-sql-proxy.darwin.amd64
chmod +x cloud-sql-proxy
```

### Connecting

#### Step 1: Start the proxy (keep this terminal open)
```bash
# Windows
./cloud-sql-proxy.exe nexdata-cloud:us-central1:nexdata-pg --port=5435

# Mac/Linux
./cloud-sql-proxy nexdata-cloud:us-central1:nexdata-pg --port=5435
```

#### Step 2: Connect

**With the Nexdata app:**
```bash
source switch-env.sh cloud
docker-compose --env-file .env.active up -d api worker frontend
```
Note: Don't start the `postgres` service — you're using the cloud DB instead.

**Direct with psql:**
```bash
psql -h localhost -p 5435 -U nexdata -d nexdata
# Password: Nex2026
```

**DBeaver / pgAdmin / DataGrip:**
- Host: `localhost`
- Port: `5435`
- Database: `nexdata`
- User: `nexdata`
- Password: `Nex2026`

**Python:**
```python
import psycopg2
conn = psycopg2.connect(
    host="localhost", port=5435,
    dbname="nexdata", user="nexdata", password="Nex2026"
)
```

### Security
- No public IP exposure — all traffic goes through Google's encrypted tunnel
- Only authorized Google accounts (via IAM) can start the proxy
- The proxy authenticates with your Google credentials, then the DB password is a second factor
- If the proxy won't start, re-run `gcloud auth application-default login`

### Authorized Users
- alexiusmichael@gmail.com (owner)
- usharao13@gmail.com (client)

To add a new user:
```bash
gcloud projects add-iam-policy-binding nexdata-cloud \
  --member="user:their-email@gmail.com" \
  --role="roles/cloudsql.client"
```

---

## Disaster Recovery

### What's protected
| Feature | Status | Details |
|---|---|---|
| Automated backups | ✅ Enabled | Daily at 4am UTC, 7 backups retained |
| Point-in-time recovery | ✅ Enabled | Restore to any second within the last 7 days |
| Deletion protection | ✅ Enabled | Instance cannot be deleted without disabling first |
| Zone availability | ⚠️ Zonal | Single zone — fine for dev/experimental |

### Restore from backup (full restore)

```bash
# 1. List available backups
gcloud sql backups list --instance=nexdata-pg --project=nexdata-cloud

# 2. Restore from a specific backup (OVERWRITES current data)
gcloud sql backups restore BACKUP_ID \
  --restore-instance=nexdata-pg \
  --project=nexdata-cloud
```

### Restore to a point in time (PITR)

Use this to recover from accidental data deletion or corruption:

```bash
# Restore to a specific timestamp (within last 7 days)
# This creates a NEW instance — safe, doesn't overwrite current data
gcloud sql instances clone nexdata-pg nexdata-pg-restored \
  --point-in-time="2026-03-17T12:00:00Z" \
  --project=nexdata-cloud

# Verify the restored instance, then if good — swap over:
# 1. Update .env.cloud to point to nexdata-pg-restored
# 2. Delete old instance once confirmed
```

### Emergency: instance deleted or unrecoverable

If `nexdata-pg` is somehow gone:

```bash
# 1. Create a new instance (same config)
gcloud sql instances create nexdata-pg-new \
  --database-version=POSTGRES_14 \
  --tier=db-f1-micro \
  --storage-size=10GB \
  --region=us-central1 \
  --project=nexdata-cloud

# 2. Restore latest backup into it
gcloud sql backups restore BACKUP_ID \
  --restore-instance=nexdata-pg-new \
  --project=nexdata-cloud

# 3. Update .env.cloud connection string
# Change nexdata-pg → nexdata-pg-new
```

### Manual backup (before risky changes)

```bash
gcloud sql backups create \
  --instance=nexdata-pg \
  --project=nexdata-cloud \
  --description="pre-migration backup"
```

---

## Adding a Developer

```bash
# Grant access (run once per developer)
gcloud projects add-iam-policy-binding nexdata-cloud \
  --member="user:their-email@gmail.com" \
  --role="roles/cloudsql.client"
```

Then send them this file. They follow **Cloud Setup → First-time setup** above.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Proxy won't start | Run `gcloud auth application-default login` |
| "password authentication failed" | Check you're on the right port (5435 for cloud) |
| Can't connect to local DB | Run `docker-compose up -d postgres` and wait 10s |
| API won't start | Check `docker-compose logs api --tail 20` |

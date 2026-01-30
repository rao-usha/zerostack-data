# People & Org Chart Platform - Deployment Guide

Instructions for deploying the People & Org Chart Intelligence Platform.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development](#local-development)
3. [Docker Deployment](#docker-deployment)
4. [Production Deployment](#production-deployment)
5. [Database Setup](#database-setup)
6. [Configuration](#configuration)
7. [Monitoring](#monitoring)

---

## Prerequisites

### Required Software

- Python 3.10+
- PostgreSQL 14+
- Docker & Docker Compose (for containerized deployment)
- Git

### API Keys (Optional)

- OpenAI API Key (for GPT-based extraction)
- Anthropic API Key (for Claude-based extraction)

---

## Local Development

### 1. Clone the Repository

```bash
git clone https://github.com/rao-usha/zerostack-data.git
cd zerostack-data
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

Create `.env` file:

```bash
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/nexdata

# LLM (optional)
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...

# App Settings
LOG_LEVEL=INFO
MAX_CONCURRENCY=5
```

### 5. Initialize Database

```bash
# Run database migrations
python -c "from app.core.database import engine; from app.core.models import Base; from app.core.people_models import *; Base.metadata.create_all(bind=engine)"
```

### 6. Seed Sample Data (Optional)

```bash
python scripts/seed_industrial_companies.py
```

### 7. Run the Server

```bash
uvicorn app.main:app --reload --port 8001
```

### 8. Verify Installation

```bash
curl http://localhost:8001/api/v1/health
```

Open Swagger UI: http://localhost:8001/docs

---

## Docker Deployment

### 1. Build and Run

```bash
docker-compose up --build -d
```

### 2. View Logs

```bash
docker-compose logs -f api
```

### 3. Stop Services

```bash
docker-compose down
```

### Docker Compose Configuration

```yaml
# docker-compose.yml
version: '3.8'

services:
  api:
    build: .
    ports:
      - "8001:8001"
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@db:5432/nexdata
      - LOG_LEVEL=INFO
    depends_on:
      - db

  db:
    image: postgres:14
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=nexdata
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:
```

---

## Production Deployment

### Infrastructure Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| API Server | 2 CPU, 4GB RAM | 4 CPU, 8GB RAM |
| Database | 2 CPU, 4GB RAM | 4 CPU, 16GB RAM |
| Storage | 50GB SSD | 200GB SSD |

### Deployment Options

#### Option 1: AWS ECS

```bash
# Build and push to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin $ECR_URL
docker build -t nexdata-api .
docker tag nexdata-api:latest $ECR_URL/nexdata-api:latest
docker push $ECR_URL/nexdata-api:latest

# Deploy to ECS
aws ecs update-service --cluster production --service nexdata-api --force-new-deployment
```

#### Option 2: Kubernetes

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexdata-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: nexdata-api
  template:
    metadata:
      labels:
        app: nexdata-api
    spec:
      containers:
      - name: api
        image: nexdata-api:latest
        ports:
        - containerPort: 8001
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: nexdata-secrets
              key: database-url
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
```

#### Option 3: Simple VPS

```bash
# Install dependencies
sudo apt update
sudo apt install python3.10 python3.10-venv postgresql nginx

# Clone and setup
git clone https://github.com/rao-usha/zerostack-data.git
cd zerostack-data
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run with Gunicorn
gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8001
```

### Nginx Configuration

```nginx
# /etc/nginx/sites-available/nexdata
server {
    listen 80;
    server_name api.nexdata.example.com;

    location / {
        proxy_pass http://127.0.0.1:8001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Systemd Service

```ini
# /etc/systemd/system/nexdata.service
[Unit]
Description=Nexdata API
After=network.target postgresql.service

[Service]
User=www-data
Group=www-data
WorkingDirectory=/opt/nexdata
Environment="PATH=/opt/nexdata/venv/bin"
ExecStart=/opt/nexdata/venv/bin/gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8001
Restart=always

[Install]
WantedBy=multi-user.target
```

---

## Database Setup

### Create Database

```sql
CREATE DATABASE nexdata;
CREATE USER nexdata_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE nexdata TO nexdata_user;
```

### Run Migrations

```bash
# Using SQLAlchemy (current approach)
python -c "
from app.core.database import engine
from app.core.models import Base
from app.core.people_models import *
Base.metadata.create_all(bind=engine)
"
```

### Backup and Restore

```bash
# Backup
pg_dump -h localhost -U postgres nexdata > backup.sql

# Restore
psql -h localhost -U postgres nexdata < backup.sql
```

### People Platform Tables

The following tables are created:

| Table | Description |
|-------|-------------|
| `people` | Person master records |
| `industrial_companies` | Company records |
| `company_people` | Person-company relationships |
| `people_experience` | Work history |
| `people_education` | Education records |
| `leadership_changes` | Executive movements |
| `org_chart_snapshots` | Point-in-time org charts |
| `people_collection_jobs` | Collection job tracking |
| `people_portfolios` | Portfolio definitions |
| `people_portfolio_companies` | Portfolio membership |
| `people_peer_sets` | Peer set definitions |
| `people_peer_set_members` | Peer set membership |
| `people_watchlists` | Watchlist definitions |
| `people_watchlist_people` | Watchlist membership |

---

## Configuration

### Environment Variables

```bash
# Required
DATABASE_URL=postgresql://user:password@host:5432/dbname

# Optional - LLM
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
LLM_PROVIDER=anthropic  # or openai

# Optional - Logging
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR

# Optional - Rate Limiting
MAX_REQUESTS_PER_MINUTE=100
WEBSITE_REQUESTS_PER_MINUTE=30

# Optional - Collection
MAX_CONCURRENCY=5
COLLECTION_TIMEOUT_SECONDS=60

# Optional - Scheduling
ENABLE_SCHEDULED_JOBS=true
WEBSITE_REFRESH_CRON="0 2 * * 0"  # Weekly Sunday 2am
SEC_CHECK_CRON="0 6 * * *"        # Daily 6am
NEWS_SCAN_CRON="0 8 * * *"        # Daily 8am
```

### Configuration File

```python
# app/core/config.py

class Settings(BaseSettings):
    database_url: str
    log_level: str = "INFO"
    max_concurrency: int = 5

    # LLM
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    llm_provider: str = "anthropic"

    # Rate Limiting
    max_requests_per_minute: int = 100
    website_requests_per_minute: int = 30

    class Config:
        env_file = ".env"
```

---

## Monitoring

### Health Check

```bash
curl http://localhost:8001/api/v1/health
```

Expected response:
```json
{
  "status": "healthy",
  "database": "connected",
  "version": "1.0.0"
}
```

### Metrics Endpoints

```bash
# Job statistics
curl http://localhost:8001/api/v1/people-jobs/stats?days=7

# Data quality stats
curl http://localhost:8001/api/v1/people-data-quality/stats

# Freshness stats
curl http://localhost:8001/api/v1/people-data-quality/freshness
```

### Logging

Logs are output in JSON format for easy parsing:

```json
{
  "timestamp": "2024-01-30T12:00:00Z",
  "level": "INFO",
  "message": "Collection job completed",
  "job_id": 123,
  "people_found": 25,
  "duration_seconds": 45.2
}
```

### Recommended Alerts

| Metric | Threshold | Action |
|--------|-----------|--------|
| Job failure rate | > 10% | Check API keys, rate limits |
| Stale data | > 50% over 90 days | Increase collection frequency |
| Database connections | > 80% | Scale database |
| API response time | > 2s | Scale API servers |

### Prometheus Metrics (Optional)

```python
# Add to app/main.py
from prometheus_client import Counter, Histogram, make_asgi_app

requests_total = Counter('requests_total', 'Total requests', ['method', 'endpoint'])
request_duration = Histogram('request_duration_seconds', 'Request duration')

# Mount metrics endpoint
app.mount("/metrics", make_asgi_app())
```

---

## Security

### Best Practices

1. **Use HTTPS** - Always use TLS in production
2. **Secure Database** - Use strong passwords, limit access
3. **API Authentication** - Implement API keys or JWT (future)
4. **Rate Limiting** - Prevent abuse
5. **Input Validation** - Sanitize all user input

### Database Security

```sql
-- Restrict user permissions
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO nexdata_user;
```

### Secrets Management

```bash
# Use environment variables for secrets
export DATABASE_URL="postgresql://..."
export OPENAI_API_KEY="sk-..."

# Or use AWS Secrets Manager / HashiCorp Vault
```

---

## Troubleshooting

### Common Issues

**Database connection failed**
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check connection string
psql $DATABASE_URL
```

**API not responding**
```bash
# Check process is running
ps aux | grep uvicorn

# Check logs
docker-compose logs api
```

**Collection jobs failing**
```bash
# Check job stats
curl http://localhost:8001/api/v1/people-jobs/stats

# View recent job errors
curl http://localhost:8001/api/v1/people-jobs/?status=failed&limit=5
```

### Getting Help

- API Docs: http://localhost:8001/docs
- GitHub Issues: https://github.com/rao-usha/zerostack-data/issues

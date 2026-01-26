# Claude Code Project Instructions

> This file is read by Claude Code on startup. Follow these guidelines.

## Project: Nexdata External Data Ingestion API

FastAPI service for ingesting data from 25+ public APIs into PostgreSQL.

---

## Standard Workflow

**Always follow this sequence:**

```
1. PLAN      → Write plan, get user approval BEFORE coding
2. EXECUTE   → Write code, use TodoWrite to track progress
3. TEST      → docker-compose up --build -d, curl endpoints
4. FIX       → If tests fail, check logs, fix, re-test
5. COMMIT    → git add <files>, git commit with good message
6. PUSH      → git push origin main
7. CI CHECK  → Verify GitHub Actions passes
```

**Do NOT skip steps.** Always test before committing.

### Planning Phase (Step 1)
- **Record the plan** in `docs/plans/PLAN_XXX_<name>.md`
- Include: goal, endpoints, features, models, files to create, example usage
- Update status in `PARALLEL_WORK.md` plan table
- **Wait for user to mark [x] Approved** before writing any code
- Never assume approval - explicit confirmation required

**Plan file template:** See existing plans in `docs/plans/`

---

## Parallel Work

If working with another Claude instance:

1. **Read `PARALLEL_WORK.md`** before doing anything
2. **Only touch files assigned to your tab**
3. **Update your status** in PARALLEL_WORK.md
4. **Tab 1 handles:** main.py integration, commits, pushes
5. **Use Communication Log** for async coordination

See `.claude/WORKFLOW.md` for full parallel workflow details.

---

## Key Commands

```bash
# Rebuild Docker (port 8001)
docker-compose up --build -d

# Check logs for errors
docker-compose logs api --tail 50

# Test endpoint
curl -s http://localhost:8001/api/v1/<endpoint> | python -m json.tool

# Check CI
curl -s "https://api.github.com/repos/rao-usha/zerostack-data/actions/runs?per_page=3"
```

---

## File Structure

```
app/
├── api/v1/          # FastAPI routers (one per source)
├── core/            # Shared services (models, database, config)
├── sources/         # Source-specific clients and ingestors
└── main.py          # App entry point, router registration
```

---

## Adding New Features

1. **New data source:**
   - Create `app/sources/<name>/` with client.py, ingest.py
   - Create `app/api/v1/<name>.py` with endpoints
   - Add router to `app/main.py`
   - Add OpenAPI tag to main.py

2. **New core feature:**
   - Add models to `app/core/models.py`
   - Create service in `app/core/<feature>_service.py`
   - Create API in `app/api/v1/<feature>.py`
   - Register router in main.py

---

## Commit Format

```
<type>: <short description>

- Detail 1
- Detail 2

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`

---

## Rules

- **Always test before committing**
- **Don't modify files you don't own in parallel mode**
- **Use TodoWrite for multi-step tasks**
- **Check CI after every push**
- **Ask user before making architectural decisions**

---

## Long-Running Task Protocol

For complex multi-step tasks that may take extended time or could be interrupted:

### 1. CREATE TASK LIST
Use TaskCreate to break down work into trackable tasks:
```
- Create task for each major phase
- Include clear descriptions and success criteria
- Mark dependencies between tasks
```

### 2. EXECUTE WITH CHECKPOINTS
For each task:
```
1. Mark task as "in_progress" before starting
2. Complete the work
3. Mark task as "completed" when done
4. If blocked, note the blocker and move on
```

### 3. CHECKPOINT FORMAT
After completing significant work, update task with:
```
Last completed: [specific action]
Next action: [what to do next]
Blockers: [any issues]
Resume: [how to continue if interrupted]
```

### 4. ON RESUME
If session was interrupted:
```
1. Use TaskList to see current state
2. Use TaskGet on in_progress tasks
3. Continue from last checkpoint
4. Don't repeat completed work
```

### 5. TASK LIST EXAMPLE

```markdown
## Comprehensive Investor Data Expansion

### Phase 1: Database Setup
- [x] Create lp_aum_history table
- [x] Create lp_allocation_history table
- [ ] Create lp_13f_holding table (IN PROGRESS)
- [ ] Create lp_manager_commitment table

### Phase 2: Collectors
- [ ] SEC 13F collector
- [ ] Form 990 collector
- [ ] CAFR parser with LLM

### Checkpoint
Last: Created allocation history table
Next: Create 13F holding table
Resume: Run remaining CREATE TABLE statements
```

### 6. COMPLETION
When all tasks done:
```
1. Mark all tasks as completed
2. Summarize what was accomplished
3. Note any remaining issues or follow-ups
4. Commit changes with comprehensive message
```

---

## Querying the Nexdata API

The API runs at `http://localhost:8001`. Use the Python client for data queries:

```python
from scripts.nexdata_client import NexdataClient
client = NexdataClient()

# Search investors
investors = client.search_investors("Sequoia")

# Get portfolio
portfolio = client.get_portfolio(investor_id)

# Get company health score
score = client.get_company_score("Stripe")

# Get deal predictions
prediction = client.get_deal_prediction(deal_id)

# Get pipeline insights
insights = client.get_pipeline_insights()
```

For full API documentation, see `.claude/skills/nexdata.md`.

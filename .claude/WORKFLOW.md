# Claude Code Workflow Guide

> Standard development workflow for this project. All Claude instances should follow this.

---

## Development Workflow (Single Tab)

```
┌─────────────────────────────────────────────────────────────┐
│  1. PLAN          Discuss approach with user before coding  │
│         ↓                                                   │
│  2. EXECUTE       Write code, create files                  │
│         ↓                                                   │
│  3. TEST          Rebuild Docker, test endpoints            │
│         ↓                                                   │
│  4. FIX           If tests fail, fix and re-test            │
│         ↓                                                   │
│  5. COMMIT        Stage and commit with descriptive message │
│         ↓                                                   │
│  6. PUSH          Push to origin/main                       │
│         ↓                                                   │
│  7. CI CHECK      Verify CI passes                          │
└─────────────────────────────────────────────────────────────┘
```

### Step Details

| Step | What To Do | Commands |
|------|------------|----------|
| **PLAN** | Ask user to confirm approach. Use `EnterPlanMode` for complex features. Don't code until user approves. | - |
| **EXECUTE** | Create/edit files. Use TodoWrite to track progress. | Write, Edit |
| **TEST** | Rebuild and test | `docker-compose up --build -d`, `curl` endpoints |
| **FIX** | Read logs, fix errors, re-test | `docker-compose logs api --tail 50` |
| **COMMIT** | Stage specific files, write good commit message | `git add <files>`, `git commit -m "..."` |
| **PUSH** | Push to remote | `git push origin main` |
| **CI CHECK** | Verify CI passes | Check GitHub Actions status |

---

## Parallel Development Workflow (Multiple Tabs)

### Setup
1. Create/update `PARALLEL_WORK.md` in project root
2. Assign clear file ownership to each tab
3. One tab handles `app/main.py` integration

### Coordination File: `PARALLEL_WORK.md`
```markdown
## Tab Assignments
- TAB 1: [feature] - owns [files]
- TAB 2: [feature] - owns [files]

## Status
| Tab | Status | Notes |
|-----|--------|-------|

## Rules
1. Only touch YOUR files
2. Update status in PARALLEL_WORK.md
3. Tab 1 handles main.py integration
4. Tab 1 handles final commit/push
```

### Parallel Workflow
```
TAB 1                         TAB 2
  │                             │
  ├─► Read PARALLEL_WORK.md ◄───┤
  │                             │
  ├─► Work on assigned files    ├─► Work on assigned files
  │                             │
  ├─► Update status: DONE ◄─────┤
  │                             │
  ├─► INTEGRATION PHASE         │
  │   - Update main.py          │
  │   - Commit all changes      │
  │   - Push                    │
  │   - CI check                │
  └─────────────────────────────┘
```

### Rules
1. **Check PARALLEL_WORK.md before editing**
2. **Only touch files in YOUR section**
3. **Update status when: starting, blocked, done**
4. **Use Communication Log for async messages**
5. **Tab 1 handles all git operations**

---

## Commit Message Format

```
<type>: <short description>

- Detail 1
- Detail 2

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
```

**Types:** `feat`, `fix`, `refactor`, `docs`, `test`, `chore`

---

## Docker Commands

```bash
# Rebuild and start
docker-compose up --build -d

# Check logs
docker-compose logs api --tail 50

# Test endpoint (port 8001 for nexdata)
curl -s http://localhost:8001/api/v1/endpoint | python -m json.tool
```

---

## File Ownership Rules

| Directory/File | Who Can Edit |
|----------------|--------------|
| `app/main.py` | Tab 1 only (during integration) |
| `app/core/models.py` | Tab 1 only (coordinate if needed) |
| `app/sources/<source>/` | Assigned tab only |
| `app/api/v1/<endpoint>.py` | Assigned tab only |
| `PARALLEL_WORK.md` | All tabs (status updates only) |

---

## Testing Checklist

- [ ] Docker builds without errors
- [ ] API starts without import errors
- [ ] New endpoints return expected responses
- [ ] Existing endpoints still work
- [ ] CI passes after push

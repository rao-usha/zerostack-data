# Parallel Development Coordination

> **Both tabs: Read this file before making changes. Update your status when done.**

---

## Workflow Reminder

```
1. PLAN      → Write plan to this file, wait for user approval
2. EXECUTE   → Only after user says "approved"
3. TEST      → Docker rebuild, curl endpoints
4. FIX       → If needed
5. INTEGRATE → Tab 1 updates main.py
6. COMMIT    → Tab 1 commits all
7. PUSH      → Tab 1 pushes
```

---

## Tab Assignments

### TAB 1 - Export & Integration
**Status:** COMPLETE
**Owner files (ONLY touch these):**
- `app/core/export_service.py` (create)
- `app/api/v1/export.py` (create)

**Do NOT touch until integration phase:**
- `app/main.py`

---

### TAB 2 - USPTO Patent Data Source
**Status:** NOT_STARTED
**Owner files (ONLY touch these):**
- `app/sources/uspto/` (create directory)
- `app/sources/uspto/__init__.py`
- `app/sources/uspto/client.py`
- `app/sources/uspto/ingest.py`
- `app/api/v1/uspto.py` (create)

**Do NOT touch:**
- `app/main.py` (Tab 1 handles integration)
- `app/core/models.py` (coordinate with Tab 1 if needed)

---

## PLANS (Stored in docs/plans/)

| Plan | File | Status | Approved |
|------|------|--------|----------|
| Tab 1: Export & Integration | [PLAN_001_export_integration.md](docs/plans/PLAN_001_export_integration.md) | APPROVED | [x] |
| Tab 2: USPTO Patents | [PLAN_002_uspto_patents.md](docs/plans/PLAN_002_uspto_patents.md) | NOT_STARTED | [ ] |

**Instructions:**
1. Write your full plan in your assigned plan file
2. Update status in this table
3. Wait for user to check the box [x] in this table
4. Only then start coding

---

## Status Updates

| Tab | Phase | Status | Last Updated | Notes |
|-----|-------|--------|--------------|-------|
| Tab 1 | TEST | COMPLETE | 2026-01-14 | Export feature tested and working |
| Tab 2 | - | NOT_STARTED | - | - |

---

## Integration Checklist (After both approved & done)

- [x] Tab 1 code complete
- [ ] Tab 2 code complete
- [ ] Tab 1 updates `app/main.py` with both routers
- [ ] Docker rebuild successful
- [ ] All endpoints tested
- [ ] Tab 1 commits all changes
- [ ] Tab 1 pushes
- [ ] CI passes

---

## Communication Log

```
[TAB 1] Plan written, waiting for user approval
[TAB 1] Plan approved, starting implementation
[TAB 1] COMPLETE - Export feature tested and working
```

---

## Rules

1. **Write your PLAN in this file first**
2. **Wait for user to say "approved" before coding**
3. **Only touch files in YOUR section**
4. **Update status after each phase**
5. **Tab 1 handles final integration and commit**

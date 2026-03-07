---
name: plan-task
description: Plan a task with checklist before starting. Use this before any non-trivial work to create a plan and task checklist. Enforces the mandatory workflow.
allowed-tools:
  - Read
  - Write
  - Edit
  - Glob
  - Grep
  - Bash
  - Agent
  - TaskCreate
  - TaskUpdate
  - TaskList
  - EnterPlanMode
argument-hint: "<description of what you want to build or fix>"
---

You have been asked to plan and set up tracking for a task. Follow these steps exactly.

## Step 1: Understand the Request

Read `$ARGUMENTS` to understand what the user wants. If no arguments, ask what they want to work on.

## Step 2: Research

Before planning, explore the codebase to understand:
- What files are relevant
- What patterns exist that you should follow
- What dependencies or constraints apply

Use Glob, Grep, Read, and Agent (Explore) as needed. Spend at least 2-3 tool calls on research.

## Step 3: Enter Plan Mode

Call `EnterPlanMode` to formally enter planning mode. This shows the user you're being deliberate.

## Step 4: Write the Plan

Create or update `docs/plans/PLAN_XXX_<name>.md` with:

```markdown
# PLAN XXX — [Title]

**Status:** Draft
**Date:** YYYY-MM-DD

## Goal
[1-2 sentences: what are we building and why]

## Approach
[High-level description of the approach]

## Checklist
- [ ] Step 1: [Concrete action]
- [ ] Step 2: [Concrete action]
- [ ] ...

## Files to Change
| File | Action | Description |
|------|--------|-------------|
| `path/to/file.py` | Modify | What changes |
| `path/to/new.py` | Create | What it does |

## Risks / Open Questions
- [Any unknowns or decisions needed]
```

## Step 5: Create Task Checklist

Use `TaskCreate` to create a task for each checklist item from the plan. These should be concrete and completable. Example:

- "Add ownership_type column to medspa_prospects table"
- "Create OwnershipClassifier service class"
- "Add POST /classify API endpoint"
- "Write unit tests for classifier"
- "Restart API and run classifier"
- "Log session checkpoint"

ALWAYS include a final task for logging: "Log work to session log".

## Step 6: Exit Plan Mode

Call `ExitPlanMode` to present the plan for user approval. Do NOT start writing code until the user approves.

## Key Rules

- NEVER skip the plan for multi-file changes
- NEVER skip the task checklist — the user needs to see progress
- ALWAYS include file paths in the plan
- ALWAYS include a verification/testing step
- ALWAYS include a logging step as the final task
- If the task is truly trivial (1 file, < 10 lines), still create tasks but skip the plan document

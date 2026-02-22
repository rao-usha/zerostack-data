---
name: test
description: Run Nexdata unit tests, integration tests, or specific test files. Knows the right pytest commands, markers, and test patterns. Use when the user wants to run tests.
allowed-tools:
  - Bash
argument-hint: "[file, module, or 'all']"
---

Run Nexdata tests with the correct configuration.

## Behavior

1. **Parse `$ARGUMENTS`** to determine what to test:

   | Input | Action |
   |-------|--------|
   | (empty) or `all` | Run all unit tests |
   | `<filename>` | Run specific test file |
   | `<source_name>` | Find and run tests for that source |
   | `integration` | Run integration tests (requires API keys + network) |
   | `coverage` | Run with coverage report |
   | `quick` | Run just the fast tests |
   | `last-failed` or `failed` | Re-run only previously failed tests |

2. **Run the appropriate command:**

   **All unit tests (default):**
   ```bash
   cd "C:/Users/awron/projects/Nexdata" && python -m pytest tests/ -v --ignore=tests/integration/ -x 2>&1 | tail -50
   ```

   **Specific test file:**
   ```bash
   cd "C:/Users/awron/projects/Nexdata" && python -m pytest tests/test_<name>.py -v 2>&1
   ```

   **Find tests for a source:**
   ```bash
   # Find test files matching the source name
   find tests/ -name "*<source>*" -o -name "*<source_variant>*" | head -10
   # Then run them
   cd "C:/Users/awron/projects/Nexdata" && python -m pytest tests/test_<match>.py -v 2>&1
   ```

   **Integration tests:**
   ```bash
   cd "C:/Users/awron/projects/Nexdata" && RUN_INTEGRATION_TESTS=true python -m pytest tests/integration/ -v 2>&1 | tail -50
   ```

   **With coverage:**
   ```bash
   cd "C:/Users/awron/projects/Nexdata" && python -m pytest tests/ -v --ignore=tests/integration/ --cov=app --cov-report=term-missing 2>&1 | tail -80
   ```

   **Last failed:**
   ```bash
   cd "C:/Users/awron/projects/Nexdata" && python -m pytest tests/ --lf -v 2>&1
   ```

3. **Parse and summarize results:**

   After pytest completes, extract:
   - Total tests run
   - Passed / Failed / Skipped / Errors
   - Duration
   - Failed test names and first line of error (if any)

   ```bash
   # The last line of pytest output has the summary
   # e.g., "=== 42 passed, 3 failed, 1 skipped in 12.34s ==="
   ```

4. **If tests fail**, show:
   - The specific test name(s) that failed
   - The assertion error or traceback (first few lines)
   - The file and line number
   - Suggest running with `-x` (stop on first failure) for debugging

5. **If no tests found for the source**, suggest:
   - Creating tests with `/add-source`
   - Check available test files: `ls tests/`

## Test markers

The project uses pytest markers:
- `@pytest.mark.unit` — Offline tests, no API calls needed
- `@pytest.mark.integration` — Requires API keys and network access
- Run specific markers: `python -m pytest -m unit` or `python -m pytest -m integration`

## Project test structure

```
tests/
├── test_*.py              # Unit tests for various modules
├── integration/           # Integration tests (require API keys)
│   └── test_*.py
└── conftest.py           # Shared fixtures
```

## Common issues

- **Import errors**: The test needs to run from the project root. Always `cd` first.
- **Database tests**: Some tests need the DB container running. Check with `/health`.
- **Missing dependencies**: Run `pip install -e ".[test]"` or check requirements.
- **Slow tests**: Use `-x` to stop on first failure, or `--timeout=30` to limit per-test time.

## Important
- Always run from the project root directory
- Use `--ignore=tests/integration/` for unit tests to avoid needing API keys
- The `-v` flag gives verbose output showing each test name
- Use `-x` to stop on first failure (useful for debugging)
- Use `--tb=short` for shorter tracebacks or `--tb=long` for full detail
- Linting: `ruff check app/` (non-blocking in CI)

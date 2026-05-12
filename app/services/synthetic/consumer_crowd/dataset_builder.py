"""
Dataset builder for the synthetic consumer crowd (SPEC_050 / PLAN_062 addendum 1 Phase 1).

Iterates the (persona × scenario) cell grid through the GPT-4o teacher and
writes a parquet file with the v1 dataset schema.

CLI:
  # Smoke (5 cells across categories, ~$0.02)
  python -m app.services.synthetic.consumer_crowd.dataset_builder --smoke

  # Full 2,000-cell run, ~$6 at GPT-4o pricing
  python -m app.services.synthetic.consumer_crowd.dataset_builder --full \\
    --output /app/data/synthetic_consumer/v1.parquet

Concurrency: a small asyncio.Semaphore keeps us under OpenAI's tier rate
limits (default 4 concurrent). The on-disk cache makes re-runs free, so
interrupted runs resume cheaply.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

from app.services.synthetic.consumer_crowd.personas import (
    Persona,
    PERSONAS,
    get_persona_by_id,
)
from app.services.synthetic.consumer_crowd.scenarios import (
    Scenario,
    SCENARIOS,
    get_scenario_by_id,
)
from app.services.synthetic.consumer_crowd.teacher_runner import (
    BudgetExceededError,
    TeacherRunner,
    TeacherResponse,
)

logger = logging.getLogger(__name__)


# Cells chosen for the 5-cell smoke test (spans 5 different scenario categories
# and 5 different personas).
SMOKE_CELLS = [
    (8, 2),    # Rural Working-Class Boomer × price up 20%       (pricing)
    (5, 11),   # Urban Affluent DINK × premium add               (product)
    (15, 22),  # Urban Millennial Eco-Conscious × sustainability  (brand)
    (2, 25),   # Urban Gen-Z Novelty Seeker × urban expansion    (geographic)
    (27, 32),  # Suburban Affluent Gen-X × competitor improved    (competitor)
]


# Parquet output schema (column order)
COLUMNS = [
    # Identity
    "persona_id",
    "scenario_id",
    "tabddpm_company_id",
    "brand_context",
    # Persona descriptive
    "persona_archetype",
    "age_bracket",
    "income_bracket",
    "geography",
    "price_sensitivity",
    "brand_loyalty",
    "novelty_seeking",
    # Scenario descriptive
    "scenario_category",
    "scenario_event_type",
    "scenario_magnitude",
    "scenario_description",
    # Outcomes
    "purchase_intent",
    "wtp_delta",
    "sentiment",
    "wom_amplitude",
    "churn_probability",
    # Confidences
    "purchase_intent_confidence",
    "wtp_delta_confidence",
    "sentiment_confidence",
    "wom_amplitude_confidence",
    "churn_probability_confidence",
    # Provenance
    "rationale",
    "teacher_model",
    "teacher_call_id",
    "generated_at",
    "cost_usd",
    "input_tokens",
    "output_tokens",
    "from_cache",
]


def _row_for_cell(
    persona: Persona,
    scenario: Scenario,
    response: TeacherResponse,
    brand_context: Optional[str],
    tabddpm_company_id: Optional[str] = None,
) -> dict:
    return {
        "persona_id": persona.id,
        "scenario_id": scenario.id,
        "tabddpm_company_id": tabddpm_company_id,
        "brand_context": brand_context,
        "persona_archetype": persona.archetype,
        "age_bracket": persona.age_bracket,
        "income_bracket": persona.income_bracket,
        "geography": persona.geography,
        "price_sensitivity": persona.price_sensitivity,
        "brand_loyalty": persona.brand_loyalty,
        "novelty_seeking": persona.novelty_seeking,
        "scenario_category": scenario.category,
        "scenario_event_type": scenario.event_type,
        "scenario_magnitude": scenario.magnitude,
        "scenario_description": scenario.description,
        "purchase_intent": response.purchase_intent,
        "wtp_delta": response.wtp_delta,
        "sentiment": response.sentiment,
        "wom_amplitude": response.wom_amplitude,
        "churn_probability": response.churn_probability,
        "purchase_intent_confidence": response.purchase_intent_confidence,
        "wtp_delta_confidence": response.wtp_delta_confidence,
        "sentiment_confidence": response.sentiment_confidence,
        "wom_amplitude_confidence": response.wom_amplitude_confidence,
        "churn_probability_confidence": response.churn_probability_confidence,
        "rationale": response.rationale,
        "teacher_model": response.teacher_model,
        "teacher_call_id": response.teacher_call_id,
        "generated_at": response.generated_at,
        "cost_usd": response.cost_usd,
        "input_tokens": response.input_tokens,
        "output_tokens": response.output_tokens,
        "from_cache": response.from_cache,
    }


async def build_dataset(
    cells: List[tuple],
    output_path: Path,
    runner: TeacherRunner,
    concurrency: int = 4,
    log_every: int = 50,
) -> dict:
    """Run cells through the teacher and write a parquet file.

    Args:
        cells: list of (persona_id, scenario_id) tuples
        output_path: where to write parquet
        runner: TeacherRunner with budget already set
        concurrency: max parallel OpenAI calls (keep low to respect rate limits)
        log_every: log progress every N completed cells

    Returns:
        Aggregate stats dict.
    """
    sem = asyncio.Semaphore(concurrency)
    rows: List[dict] = []
    failures: List[dict] = []
    completed = 0

    async def _process(persona_id: int, scenario_id: int):
        nonlocal completed
        async with sem:
            persona = get_persona_by_id(persona_id)
            scenario = get_scenario_by_id(scenario_id)
            try:
                response = await runner.run_cell(persona, scenario, brand_context=None)
                rows.append(_row_for_cell(persona, scenario, response, brand_context=None))
            except BudgetExceededError:
                raise
            except Exception as exc:
                logger.warning("Cell (persona=%d, scenario=%d) failed: %s", persona_id, scenario_id, exc)
                failures.append({"persona_id": persona_id, "scenario_id": scenario_id, "error": str(exc)[:200]})

            completed += 1
            if completed % log_every == 0 or completed == len(cells):
                logger.info(
                    "Progress: %d/%d cells (cost=$%.4f, calls=%d, cache_hits=%d)",
                    completed, len(cells), runner.cost_so_far_usd,
                    runner.calls_so_far, runner.cache_hits,
                )

    try:
        await asyncio.gather(*[_process(p, s) for p, s in cells])
    except BudgetExceededError as exc:
        logger.error("Budget exceeded — stopping run: %s", exc)

    # Write dataset
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=COLUMNS)
    suffix = output_path.suffix.lower()
    if suffix == ".parquet":
        # pyarrow may not be installed in this image (deferred to Phase 2 rebuild)
        try:
            df.to_parquet(output_path, engine="pyarrow", index=False)
        except ImportError:
            logger.warning("pyarrow not available; falling back to JSONL")
            jsonl_path = output_path.with_suffix(".jsonl")
            df.to_json(jsonl_path, orient="records", lines=True)
            output_path = jsonl_path
    elif suffix in (".jsonl", ".ndjson"):
        df.to_json(output_path, orient="records", lines=True)
    elif suffix == ".csv":
        df.to_csv(output_path, index=False)
    else:
        # Default to JSONL for unknown suffixes
        jsonl_path = output_path.with_suffix(".jsonl")
        df.to_json(jsonl_path, orient="records", lines=True)
        output_path = jsonl_path

    return {
        "cells_requested": len(cells),
        "rows_written": len(rows),
        "failures": len(failures),
        "failure_samples": failures[:10],
        "total_cost_usd": runner.cost_so_far_usd,
        "total_calls": runner.calls_so_far,
        "cache_hits": runner.cache_hits,
        "output_path": str(output_path),
    }


# -----------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------

def _all_cells() -> List[tuple]:
    """All 50 × 40 = 2,000 (persona_id, scenario_id) cells."""
    return [(p.id, s.id) for p in PERSONAS for s in SCENARIOS]


async def _main(args):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.smoke:
        cells = SMOKE_CELLS
        output = Path(args.output or "/app/data/synthetic_consumer/smoke.parquet")
        budget = 1.0
    elif args.full:
        cells = _all_cells()
        output = Path(args.output or "/app/data/synthetic_consumer/v1.parquet")
        budget = 100.0
    else:
        print("Specify --smoke or --full", file=sys.stderr)
        sys.exit(2)

    runner = TeacherRunner(
        model=args.model,
        budget_usd=budget,
        cache_dir=args.cache_dir,
    )

    logger.info("Building dataset: %d cells → %s (budget=$%.2f, model=%s)",
                len(cells), output, budget, args.model)

    stats = await build_dataset(cells, output, runner, concurrency=args.concurrency)
    print("DONE")
    for k, v in stats.items():
        if k == "failure_samples":
            continue
        print(f"  {k}: {v}")
    if stats["failures"]:
        print(f"  failure_samples: {stats['failure_samples'][:3]}")


def main():
    parser = argparse.ArgumentParser(description="Build the synthetic consumer crowd Phase 1 dataset.")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--smoke", action="store_true", help="5-cell smoke test (~$0.02)")
    g.add_argument("--full", action="store_true", help="Full 2,000-cell run (~$6)")
    parser.add_argument("--output", help="Output parquet path")
    parser.add_argument("--model", default="gpt-4o")
    parser.add_argument("--cache-dir", default="/app/data/synthetic_consumer/teacher_cache_v1/")
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()

    asyncio.run(_main(args))


if __name__ == "__main__":
    main()

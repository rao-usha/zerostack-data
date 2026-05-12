# `app/services/synthetic/training/` — W2 scaffolding for PLAN_062

This directory houses the trainers for PLAN_062's learned synthetic generators
(Phase A1 TabDDPM, Phase A2 Diffusion-TS, future Phase B graph completers).

## Status

This is **W2 scaffolding** (PLAN_062). Actual training (W3) is gated on:
1. `torch >= 2.4`, `einops`, `tsgm` added to `requirements.txt`
2. `flash-attn` (optional, Blackwell-optimized) for the 5070
3. Docker GPU passthrough configured (currently the api container is CPU-only)
4. ~30 GB disk for model checkpoints during training

## Layout

```
training/
    __init__.py                # Package marker + design notes
    README.md                  # This file
    tabddpm_trainer.py         # Phase A1 trainer skeleton (W2.2)
    diffusion_ts_trainer.py    # Phase A2 trainer skeleton (W2 follow-on)
    tabddpm/                   # Vendored yandex-research/tab-ddpm source — drop in at W3 start
```

## Why vendoring is deferred

TabDDPM (`yandex-research/tab-ddpm`) is not pip-installable. The intent is to
clone the relevant Python modules into `training/tabddpm/` so we control the
exact source we train against (some forks add bug fixes; the upstream repo
hasn't been touched in a year).

But vendoring code we can't run yet adds noise to the repo. So at W2 we ship:
- The trainer **skeleton** that implements `LearnedSyntheticGenerator`
- Documentation of exactly what comes next at W3

When W3 starts (deps in, GPU passthrough working), the implementer:
1. Clones the yandex-research/tab-ddpm modules into `training/tabddpm/`
2. Removes the `NotImplementedError` bodies in `tabddpm_trainer.py`
3. Wires the vendored modules into the trainer
4. Runs the training command in this README

## W3 training command (for reference)

```bash
# Assumes torch + GPU passthrough are set up
docker-compose exec api python -m app.services.synthetic.training.tabddpm_trainer \
    --input-table public_company_financials \
    --output-dir /app/data/synthetic_models/tabddpm_v1/ \
    --epochs 30000 \
    --batch-size 4096 \
    --diffusion-steps 1000 \
    --noise-schedule cosine
```

## References

- `docs/strategy/LEARNED_SYNTHETIC_GENERATORS_RESEARCH.md` §2 — TabDDPM architecture + hyperparameters
- `docs/plans/PLAN_062_learned_synthetic_generators.md` — Phase A1 acceptance criteria
- Kotelnikov et al., "TabDDPM: Modelling Tabular Data with Diffusion Models", ICML 2023 — [arXiv:2209.15421](https://arxiv.org/abs/2209.15421)
- Source: [github.com/yandex-research/tab-ddpm](https://github.com/yandex-research/tab-ddpm)

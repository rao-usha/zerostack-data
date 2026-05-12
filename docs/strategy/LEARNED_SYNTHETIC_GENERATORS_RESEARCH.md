# Learned Synthetic Generators — Implementation Research

**Date:** 2026-05-10
**Purpose:** Comprehensive research synthesis required to implement PLAN_062 Phase A1 (TabDDPM for private company financials) and Phase A2 (Diffusion-TS for FRED macro scenarios), running both simultaneously on a single RTX 5070 (Blackwell, 12 GB VRAM).
**Scope:** The math, papers, repos, validation metrics, hyperparameters, infrastructure, and composability decisions required to ship learned diffusion-based synthetic data generators alongside the existing parametric v1 generators.

---

## Part 0 — Why this document exists

PLAN_052 specified upgrading the v1 synthetic stack from parametric (multivariate Gaussian + Ornstein-Uhlenbeck) to learned diffusion (TabDDPM + Diffusion-TS). The v1 stack works but ceilings out on:

- **Multimodal distributions** — private company financials by sector have multi-modal margin distributions (a "two humps" pattern in some sectors: profitable scale + unprofitable growth). Gaussian collapses both into a single hump.
- **Stylized facts in financial time series** — FRED-derived returns exhibit heavy tails (kurtosis 5–15), volatility clustering (squared-return ACF decays slowly), and gain-loss asymmetry. An O-U process produces Gaussian-tailed, uncorrelated-vol paths that systematically under-estimate stress-scenario severity.
- **Conditional generation fidelity** — "generate a private $50M-revenue industrial in Texas, vintage 2021" requires joint conditioning on (NAICS-4, revenue bucket, state, year). The Gaussian peer-pooling approach can only condition on coarse sector + size buckets.

Learned diffusion models address all three. This document is the technical foundation an implementer needs in their head before writing training code.

---

## Part 1 — Foundations: DDPM math (Ho et al. 2020)

Both TabDDPM and Diffusion-TS are descendants of **Denoising Diffusion Probabilistic Models** (Ho, Jain, Abbeel — NeurIPS 2020, [arXiv:2006.11239](https://arxiv.org/abs/2006.11239)). Anyone touching the training loop must understand the four equations below; without them, debugging is guessing.

### 1.1 Forward diffusion (fixed, no learnable params)

A clean data sample `x_0` is progressively corrupted with Gaussian noise across `T` steps:

```
q(x_t | x_{t-1}) = N(x_t; sqrt(1 - β_t)·x_{t-1}, β_t·I)
```

where `β_t ∈ (0, 1)` is the **noise schedule**. The schedule choice is one of the most impactful hyperparameters:

- **Linear schedule (Ho 2020):** `β_t` linearly interpolates from 1e-4 to 0.02 over T=1000 steps. Simple, well-studied, fine for tabular.
- **Cosine schedule (Nichol & Dhariwal 2021, [arXiv:2102.09672](https://arxiv.org/abs/2102.09672)):** `ᾱ_t = cos²((t/T + s) / (1+s) · π/2)`. Adds noise more slowly at the start, faster at the end. Strictly better for image/time-series generation; usually neutral-or-better for tabular.

Closed-form jump from `x_0` to arbitrary `x_t` (no need to iterate):

```
x_t = sqrt(ᾱ_t)·x_0 + sqrt(1 - ᾱ_t)·ε,    ε ~ N(0, I)
ᾱ_t = Π_{s=1}^{t} (1 - β_s)
```

This is what the training loop actually uses. You never iterate the forward process — you sample `t` and jump.

### 1.2 Reverse process (the learnable part)

A neural network `ε_θ(x_t, t)` is trained to predict the noise `ε` that was added. The "simple" loss from Ho 2020 (which is what TabDDPM and Diffusion-TS both use):

```
L_simple = E_{t, x_0, ε} [ || ε - ε_θ(sqrt(ᾱ_t)·x_0 + sqrt(1 - ᾱ_t)·ε, t) ||² ]
```

Training is therefore:
1. Sample `x_0` from real data
2. Sample `t ~ Uniform(1, T)` and `ε ~ N(0, I)`
3. Compute `x_t` via the closed form
4. Compute loss
5. Backprop

Sampling at inference:
```
x_{t-1} = (1/sqrt(α_t)) · (x_t - (β_t / sqrt(1 - ᾱ_t)) · ε_θ(x_t, t)) + σ_t · z,    z ~ N(0, I)
```
Iterated from `t=T` down to `t=1`, starting at `x_T ~ N(0, I)`. `σ_t` is typically `sqrt(β_t)`.

### 1.3 Conditioning

To condition on a label `c` (e.g., NAICS-4 code), feed `c` into the network alongside `t`:

```
ε_θ(x_t, t, c)
```

In practice, two mechanisms dominate:
- **Classifier-free guidance (Ho & Salimans 2022, [arXiv:2207.12598](https://arxiv.org/abs/2207.12598)):** train the same network on labeled and unlabeled examples (drop `c` with probability `p_uncond ≈ 0.1`). At inference, extrapolate: `ε̃_θ = ε_θ(x_t, t, c) + w · (ε_θ(x_t, t, c) - ε_θ(x_t, t, ∅))`. Higher `w` = stronger adherence to the condition. This is the standard tool.
- **FiLM (Feature-wise Linear Modulation, Perez et al. 2017):** project `c` through two MLPs into per-feature scale/shift parameters applied to network activations. Simpler, no inference-time tuning.

For TabDDPM we'll use classifier-free guidance with conditioning on the categorical sector / bucket variables.

### 1.4 Why diffusion beats GAN/VAE for our use case

- **Mode coverage:** GANs suffer from mode collapse — they happily ignore rare modes (e.g., distressed companies). Diffusion models explicitly cover the full distribution.
- **Training stability:** no adversarial dynamics, no Nash equilibrium, no min-max instability. Single regression-style loss.
- **Likelihood-tractable:** ELBO bound gives a usable log-likelihood, which helps with validation.
- **Trade-off:** sampling is slow (T=1000 reverse steps). DDIM (Song, Meng, Ermon 2020, [arXiv:2010.02502](https://arxiv.org/abs/2010.02502)) reduces this to ~50 steps deterministically. We'll use DDIM at inference.

---

## Part 2 — Phase A1 deep dive: TabDDPM

**Reference:** Kotelnikov, Baranchuk, Rubachev, Babenko — "TabDDPM: Modelling Tabular Data with Diffusion Models" — ICML 2023 — [arXiv:2209.15421](https://arxiv.org/abs/2209.15421) — [GitHub: yandex-research/tab-ddpm](https://github.com/yandex-research/tab-ddpm)

### 2.1 The mixed-type problem

Tabular data has **continuous columns** (revenue, gross_margin, EBITDA) and **categorical columns** (NAICS code, state). Plain DDPM only handles continuous (Gaussian-noised) data. TabDDPM's contribution is running two diffusion processes in parallel:

- **Continuous columns:** standard Gaussian diffusion as in DDPM
- **Categorical columns:** **multinomial diffusion** (Hoogeboom et al. 2021, [arXiv:2102.05379](https://arxiv.org/abs/2102.05379))

Both processes share a single denoising MLP that operates on the joint state (continuous values concatenated with categorical one-hots).

### 2.2 Multinomial diffusion in plain English

For a K-way categorical variable, the forward process at step `t` mixes the current one-hot with a uniform-over-K distribution:

```
q(x_t | x_{t-1}) = Cat(x_t; (1 - β_t)·x_{t-1} + β_t·(1/K))
```

After T steps, the distribution is uniform across all K categories. The reverse process learns to denoise toward the correct category. Loss for categorical columns is KL divergence between predicted and true category distribution.

### 2.3 Network architecture (the actual neural net)

TabDDPM uses a deliberately simple MLP:
- Input: `[x_t_continuous; x_t_categorical_onehot; t_embedding; condition_embedding]`
- Hidden: 4 layers × 1024 units with residual connections + ReLU
- Output: predicted noise (continuous part) + predicted logits (categorical part)

**Why so simple?** Tabular distributions are lower-dimensional than images. A transformer or U-Net is overkill and harder to train on the dataset sizes we have (~30K–100K rows). The paper shows MLP-1024×4 matches or beats more complex architectures.

### 2.4 Preprocessing (load-bearing, easy to get wrong)

- **Continuous columns:** quantile-normalize to roughly Gaussian. Use `sklearn.preprocessing.QuantileTransformer(output_distribution='normal')`. Inverse-transform at the end.
- **Categorical columns:** one-hot encode. NAICS-4 has ~1000 codes; we'll group rare codes (< 50 occurrences) into an "OTHER" bucket per sector to keep dimensionality manageable.
- **Per-row outlier clipping:** trim revenue, total_assets, total_debt at the 99.5th percentile to prevent extreme tail values from dominating the loss.

### 2.5 Hyperparameters for our run

| Param | Value | Notes |
|---|---|---|
| Diffusion steps T | 1000 | Standard. DDIM-50 at inference. |
| Noise schedule | Cosine | Slightly better than linear for tabular |
| Hidden dim | 1024 | Standard from paper |
| Hidden layers | 4 | Standard |
| Batch size | 4096 | RTX 5070 fits this easily for tabular |
| Learning rate | 1e-3 | Adam, no warmup needed |
| Epochs | 30000 | Paper standard. Will checkpoint at 10K, 20K, 30K. |
| Classifier-free guidance dropout `p_uncond` | 0.1 | Standard |
| Inference guidance scale `w` | 1.5 | Tunable per evaluation |
| Mixed precision | bf16 | Blackwell native support |

### 2.6 Conditioning strategy

We condition on the joint of (NAICS-4, revenue_bucket, state_fips, year). Encoding:
- NAICS-4: learned embedding, dim=128
- Revenue bucket (6 buckets: <$10M, $10–50M, $50–250M, $250M–1B, $1–5B, $5B+): one-hot then embed, dim=32
- State: 50-way one-hot then embed, dim=32
- Year: scalar normalized to [0, 1] across 2000–2025

Total condition embedding: 192 dims, projected to hidden_dim=1024 via single linear layer, then added to the timestep embedding.

### 2.7 Generation API

```python
generator = TabDDPMGenerator.load("models/tabddpm_v1.pt")
samples = generator.sample(
    n=100,
    condition={"naics_4": "5413", "revenue_bucket": "50-250M",
               "state_fips": "48", "year": 2024},
    guidance_scale=1.5,
    ddim_steps=50,
)
# samples is a DataFrame with columns: revenue, gross_margin, ebitda_margin,
# net_margin, total_assets, total_debt, employee_count
```

### 2.8 Known failure modes

- **Mode collapse on rare conditions:** if (NAICS-4, revenue_bucket) combo has < 30 training examples, output collapses to the marginal distribution. **Mitigation:** detect at inference and fall back to v1 parametric generator with a warning.
- **Negative revenues:** continuous diffusion can produce small negatives. **Mitigation:** post-hoc clamp to ≥ 0 for revenue and total_assets.
- **Implausible ratios:** generated row might have EBITDA margin > gross margin (impossible). **Mitigation:** post-hoc reject-and-resample; we expect ~3% rejection rate based on TabDDPM paper benchmarks.

---

## Part 3 — Phase A2 deep dive: Diffusion-TS

**Reference:** Yuan & Qiao — "Interpretable Diffusion for General Time Series Generation" — ICLR 2024 — [arXiv:2403.01742](https://arxiv.org/abs/2403.01742) — [GitHub: Y-debug-sys/Diffusion-TS](https://github.com/Y-debug-sys/Diffusion-TS)

### 3.1 What makes time series different from tabular

A time series sample is not a single row — it's a length-L sequence with temporal structure. Generating it requires:
- **Preserving autocorrelation** at multiple lags
- **Preserving cross-series correlation** (FFR moves with DGS10)
- **Preserving stylized facts** (volatility clustering, heavy tails, mean reversion)
- **Multi-scale dynamics** (long-term trend, seasonal cycle, short-term shock)

Plain DDPM on flattened sequences works but loses the inductive bias. Diffusion-TS adds three things to vanilla DDPM:

### 3.2 STL decomposition before training

For each input sequence `y_{1:L}`, decompose via Seasonal-Trend-Loess (STL):

```
y_t = T_t + S_t + R_t
```

where `T_t` is trend (slow-moving), `S_t` is seasonal (period-12 monthly), `R_t` is residual.

Diffusion-TS predicts each component separately within a single denoising pass, then sums. This is the "interpretable" part — at inference you can visualize trend, seasonal, residual contributions to a generated path.

### 3.3 Denoising backbone: Transformer with Fourier-aware loss

Network architecture:
- Encoder: causal Transformer over the noisy sequence
- Decoder: 3 parallel heads producing trend, seasonal, residual reconstructions
- Cross-channel attention layer enables multi-variate (our 8 FRED series) coupling
- Time embedding via sinusoidal + learned mixture

The loss combines:
```
L = L_simple + λ · L_fourier
L_fourier = || FFT(x_0_pred) - FFT(x_0_true) ||²  (in frequency domain)
```

`L_fourier` is what makes the model preserve stylized facts. Without it, the model preserves the *mean* path well but generates sequences with the wrong frequency-domain power spectrum (i.e., wrong volatility-clustering signature).

Default λ = 0.01. Higher emphasizes frequency-domain fidelity at the cost of pointwise accuracy.

### 3.4 Hyperparameters for our run

| Param | Value | Notes |
|---|---|---|
| Sequence length L | 60 months | 5-year context window |
| Forecast horizon | 24 months | What we sample |
| Diffusion steps T | 500 | Diffusion-TS paper uses 500 for time series |
| Noise schedule | Cosine | |
| Transformer layers | 4 | Small enough for 5070 VRAM |
| Hidden dim | 256 | Reduced from paper's 512 to fit our GPU |
| Attention heads | 4 | |
| Channels (series) | 8 | DFF, DGS10, DGS2, UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO |
| Batch size | 256 | Sequence batches — much smaller than tabular |
| Learning rate | 1e-4 | Adam + cosine schedule warmup |
| Epochs | 10000 | Will checkpoint at 3K, 6K, 10K. |
| Fourier loss weight λ | 0.01 | Per paper. |
| Mixed precision | bf16 | |

### 3.5 Preprocessing for FRED data

- **Differencing:** train on monthly **changes** (Δy_t = y_t - y_{t-1}), not levels. Mean reversion is built in at the post-processing stage. This is critical — diffusion on raw levels learns the trend dominantly and ignores returns.
- **Per-series standardization:** z-score each series using training-set mean and std. Invert at the end.
- **Missing values:** forward-fill up to 3 months, drop sequences with more gaps.
- **Splits:** 1965–2015 train, 2016–2020 val, 2021–2025 test. Walk-forward validation only (no random splits for time series).

### 3.6 Generation API

```python
generator = DiffusionTSGenerator.load("models/diffusion_ts_v1.pt")
paths = generator.sample(
    n=1000,
    horizon=24,
    series=["DFF", "DGS10", "DGS2", "UNRATE", "CPIAUCSL", "UMCSENT", "INDPRO", "DCOILWTICO"],
    initial_state="latest",   # or pass dict of (series, value)
    ddim_steps=50,
)
# paths.shape = (1000, 24, 8) — 1000 scenarios × 24 months × 8 series
```

### 3.7 Known failure modes

- **Drift in long-horizon paths:** at horizons > 36 months, the model can drift unrealistically. **Mitigation:** restrict horizon to 24mo for v1; train a longer-horizon variant later if needed.
- **Mode collapse on rare regimes:** stagflation (high inflation + high rates) was rare in training data; the model under-samples it. **Mitigation:** post-hoc reweight scenarios at sampling time, or train a regime-conditional variant.
- **Discontinuities between context and forecast:** generated path doesn't smoothly continue from current state. **Mitigation:** use the Diffusion-TS paper's "conditioning on context" mode rather than fully unconditional.

---

## Part 4 — TSGM framework (the harness that wraps both)

**Reference:** Nikitin et al. — "TSGM: A Flexible Framework for Generative Modeling of Synthetic Time Series" — NeurIPS 2024 — [arXiv:2305.11567](https://arxiv.org/abs/2305.11567) — [GitHub: AlexanderVNikitin/tsgm](https://github.com/AlexanderVNikitin/tsgm)

TSGM is the "SDV for time series": a single Python framework that wraps TimeGAN, RCGAN, DoppelGANger, TabDDPM, Diffusion-TS, and several other generators under a unified API, plus a built-in evaluation suite.

We use TSGM as:
- The training scaffolding for Diffusion-TS (saves us implementing the training loop from scratch)
- The evaluation harness for **both** A1 and A2 (the discriminative score, predictive score, and stylized-facts tests are built in)

### 4.1 The TSGM evaluation suite

| Metric | What it measures | Interpretation |
|---|---|---|
| **Discriminative score** | Train binary classifier real-vs-synthetic on full sequences; report AUROC | 0.5 = indistinguishable, 1.0 = perfectly distinguishable. Target: < 0.6 |
| **Predictive score** | Train forecaster on synthetic only, evaluate on real | MAE relative to forecaster-trained-on-real. Target: within 10% |
| **TSTR (Train-on-Synth, Test-on-Real)** | Same as predictive but with explicit downstream model | Standard ML utility metric |
| **TRTS (Train-on-Real, Test-on-Synth)** | Inverse — checks that real-trained model generalizes to synthetic | Diagnostic |

### 4.2 Why not just write our own evaluator

We could. TSGM gives us four things for free that we'd otherwise have to build and validate:
- Reference implementations of metrics that researchers actually use (so our numbers are comparable to paper benchmarks)
- The framework's data adapters handle the conversion between pandas, numpy, and tensor formats
- Built-in visualization (t-SNE, PCA) of real vs synthetic distributions
- A standard interface to plug new generators into later (Phase B graph completion will reuse this harness)

---

## Part 5 — Validation theory (the math behind the acceptance criteria)

This section explains *what each acceptance-criterion metric actually measures* so the implementer can debug a failing run by understanding why a metric is off.

### 5.1 Kolmogorov-Smirnov test (KS)

KS measures the maximum distance between two empirical cumulative distribution functions:

```
D = sup_x | F_real(x) - F_synth(x) |
```

Under the null hypothesis (real and synth from same distribution), `D` follows a known distribution. A p-value > 0.05 means we cannot reject the null at α=0.05.

**When KS fails:** the synth distribution has a different shape in some region — usually the tails. Diagnose by plotting both CDFs and looking for where they diverge.

### 5.2 Frobenius norm of covariance difference

```
||Σ_real - Σ_synth||_F = sqrt( Σ_{i,j} (Σ_real[i,j] - Σ_synth[i,j])² )
```

This measures whether the cross-feature (or cross-series) correlation structure is preserved. KS only checks marginals; Frobenius catches "marginals are right but features are uncorrelated" failures.

Target < 0.15 (tabular) and < 0.10 (time series) — these are normalized values where each series is z-scored before computing.

### 5.3 Discriminative score

Train a binary classifier (gradient-boosted trees by default) on labeled (real, synth) examples. Report test-set AUROC.

- AUROC = 0.5: perfectly indistinguishable — ideal
- AUROC = 1.0: trivially separable — worst case
- AUROC = 0.6: classifier finds *some* signal but it's weak — acceptable target

This is the most honest single metric for synth quality: if a competent classifier can't tell them apart, downstream consumers can't either.

### 5.4 ML utility (TSTR)

Train a downstream task model on synthetic data only, then evaluate on real held-out data. Compare to the same model trained on real data. If the synthetic-trained model is within 5% of the real-trained model's performance, the synthetic data is **utility-preserving** — it captures the patterns the downstream task needs.

For TabDDPM we evaluate utility on a sector classification task (predict NAICS-4 from the financial features). For Diffusion-TS we evaluate utility on a downstream forecasting task (predict next-3-month FFR from prior 12).

### 5.5 Stylized facts (time series only)

Three numerical tests specific to financial time series:

**Kurtosis** of monthly changes:
```
kurt = E[(X - μ)⁴] / σ⁴
```
Gaussian has kurtosis = 3. Financial returns typically have kurtosis 5–15 (heavy tails). O-U processes generate kurtosis ≈ 3. Diffusion-TS should match historical (within ±0.3).

**ACF of squared returns:**
```
ACF(k) = corr(r²_t, r²_{t-k})
```
Volatility clustering means ACF(k) decays slowly (e.g., 0.3 at k=1, 0.2 at k=12 for equity returns). Independent-return models (including O-U) have ACF(k) ≈ 0 for all k > 0.

**Gain-loss asymmetry:** plot the CDF of returns vs the CDF of -returns. Real financial returns show asymmetry; generated paths should preserve it.

---

## Part 6 — RTX 5070 infrastructure recalibration

The plan originally assumed A100-class GPU (40–80 GB VRAM). RTX 5070 is the consumer Blackwell card: ~12 GB GDDR7, ~6,144 CUDA cores, FP4/FP8/BF16 support, ~250 W TDP. Significantly less VRAM and ~30–40% less compute than A100, but Blackwell adds native FP4 + improved Tensor Cores that close some of the gap on diffusion workloads.

### 6.1 VRAM budget for both models running simultaneously

| Component | TabDDPM | Diffusion-TS | Total |
|---|---|---|---|
| Model parameters (bf16) | ~30 MB | ~80 MB | 110 MB |
| Optimizer state (Adam, fp32) | ~120 MB | ~320 MB | 440 MB |
| Activations (batch + sequence) | ~600 MB | ~2.0 GB | 2.6 GB |
| Gradients | ~30 MB | ~80 MB | 110 MB |
| Misc (CUDA kernels, buffers) | ~500 MB | ~500 MB | 1.0 GB |
| **Peak per model** | **~1.3 GB** | **~3.0 GB** | **~4.3 GB combined** |

Both models can train concurrently on the 5070 with ~8 GB headroom. We **will** train concurrently to halve wall-clock time — that's the "all operating at once" requirement.

### 6.2 Settings that matter on Blackwell

- **bf16 mixed precision**: native Blackwell support, no scaler needed (unlike fp16). Use `torch.cuda.amp.autocast(dtype=torch.bfloat16)`.
- **torch.compile**: PyTorch 2.x compiler gives 30–50% speedup on transformer + MLP workloads. Enable with `model = torch.compile(model, mode='reduce-overhead')`.
- **Flash Attention 3** (Blackwell-optimized): for Diffusion-TS transformer attention layers, install `flash-attn` and use the FA3 kernel — 2× speedup over PyTorch's default scaled-dot-product-attention on Blackwell.
- **Gradient accumulation**: not strictly needed at our batch sizes, but useful if we want to scale to TabDDPM batch=8192 later.
- **CUDA graph capture**: for the fixed-shape DDIM sampling loop, capture the reverse-step subgraph for ~20% speedup at inference.

### 6.3 Wall-clock training estimates (RTX 5070, both models concurrent)

| Phase | Operation | Estimated time | Notes |
|---|---|---|---|
| A1 TabDDPM | Train 30K epochs on ~50K rows | ~8–12 hrs | bf16 + compile |
| A2 Diffusion-TS | Train 10K epochs on ~720 sequences (60 yrs × overlap) | ~16–24 hrs | The longer one |
| **Both concurrent** | | **~24 hrs wall clock** | Diffusion-TS dominates |

If we accept that training is a single overnight run, the schedule looks workable. If acceptance criteria fail and we need to iterate, expect 24-hr cycles per iteration.

### 6.4 Process layout

Run two Python processes:
- `python -m app.services.synthetic.training.train_tabddpm` — pinned to CUDA stream 0
- `python -m app.services.synthetic.training.train_diffusion_ts` — pinned to CUDA stream 1

Each process owns its own data loader, model, optimizer, checkpointing. They share only:
- GPU device (sharded by VRAM budget above)
- Training logs (Weights & Biases project `nexdata-synthetic-v2`)
- A shared lock file in `app/services/synthetic/models/` to prevent both writing to the same checkpoint slot

---

## Part 7 — Running everything at once: the unified runtime

"I want all these things operating at once" means at three different time scales. Each requires explicit design.

### 7.1 Training-time concurrency (one-shot, overnight)

Both training jobs run simultaneously on the 5070 as separate processes (§6.4). Validation runs happen automatically after each training job completes (orchestrated by a wrapper script that watches the model artifacts directory).

### 7.2 Inference-time concurrency (every API call)

Both v2 generators are loaded into a single FastAPI worker process at startup as singletons:

```python
# app/services/synthetic/loaded_models.py
@lru_cache(maxsize=1)
def get_tabddpm() -> TabDDPMGenerator:
    return TabDDPMGenerator.load("models/tabddpm_v1.pt", device="cuda")

@lru_cache(maxsize=1)
def get_diffusion_ts() -> DiffusionTSGenerator:
    return DiffusionTSGenerator.load("models/diffusion_ts_v1.pt", device="cuda")
```

Both models live on GPU simultaneously (~110 MB params combined — trivial). API requests dispatch to whichever model the user requested via the `algorithm` parameter:

```
POST /api/v1/synthetic/private-financials
Body: {"algorithm": "diffusion", "n": 100, "naics_4": "5413", ...}
                         ↓
                 get_tabddpm().sample(...)

POST /api/v1/synthetic/macro-scenarios
Body: {"algorithm": "diffusion", "n": 1000, "horizon": 24, ...}
                         ↓
                 get_diffusion_ts().sample(...)
```

Concurrent inference requests are serialized on the GPU by PyTorch's default behavior (one CUDA stream per request, kernels run sequentially). At our expected request rate (<10/min), this is fine. If we need higher throughput later, add a request queue + dedicated worker.

### 7.3 Validation-time concurrency (continuous, post-training)

The validation engine runs both phases through the same harness. A single `validate_all()` entry point produces a unified report:

```python
report = SyntheticValidator(db).validate_all(algorithms=["parametric", "diffusion"])
# Returns: per-generator × per-algorithm validation results
# Saved to: app/services/synthetic/validation_history/{timestamp}.json
```

The validation dashboard (`frontend/synthetic-validation.html`) renders both algorithm variants side-by-side per generator so the v1 vs v2 quality delta is visible at a glance.

### 7.4 The unified generator interface

Both v2 generators implement a single base class. This makes composability explicit and means Phase B (graph completion) will plug into the same scaffolding.

```python
class LearnedSyntheticGenerator(ABC):
    @abstractmethod
    def train(self, db: Session, config: TrainingConfig) -> ModelArtifact: ...
    @abstractmethod
    def load(self, path: Path) -> "LearnedSyntheticGenerator": ...
    @abstractmethod
    def sample(self, n: int, **conditions) -> pd.DataFrame: ...
    @abstractmethod
    def validate(self, db: Session, held_out: pd.DataFrame) -> ValidationReport: ...
```

TabDDPMGenerator and DiffusionTSGenerator both subclass this. Phase B's RotatE-based KG completer will subclass it too. This is the contract that lets the validation dashboard render heterogeneous generators uniformly.

---

## Part 8 — Privacy and data integrity (even though XBRL is public)

EDGAR XBRL is fully public, so differential privacy is not legally required. But two integrity concerns still apply:

### 8.1 Memorization risk

Diffusion models can memorize training examples, especially with small/repetitive datasets. If a user samples a "private $50M industrial in Texas," the model might literally reproduce a specific public company's financials with the labels swapped.

**Mitigations:**
- Train for fewer epochs than the "perfect overfit" point (track training vs validation loss, stop when validation loss plateaus)
- At inference, check generated samples against training nearest-neighbors; reject if cosine similarity > 0.95
- Random-flip 5% of categorical conditions during training (label smoothing for categoricals)

### 8.2 "Synthetic discoveries" risk

Reference: *"Does Differentially Private Synthetic Data Lead to Synthetic Discoveries?"* (PubMed 2024). Synthetic data can introduce spurious statistical patterns that look real but don't generalize. If a downstream scorer trains on synthetic-augmented data, it might learn to rely on a fake signal.

**Mitigations:**
- Never train a *downstream* model exclusively on synthetic data. Always include a meaningful real-data weight (≥50%).
- Flag all synthetic-augmented scorer outputs with wider confidence intervals (the existing provenance system supports this; we tighten the multiplier).
- Periodically re-validate synthetic generators against fresh real ingestions — if a generator's KS p-value drops below threshold, retrain.

### 8.3 Provenance carry-through

Each generated sample carries an opaque provenance token: `{model: "tabddpm_v1", train_run_id: "...", generated_at: ...}`. The token is logged with the ingestion job (`data_origin='synthetic'`, `synthetic_model='tabddpm_v1'` — needs new column on `ingestion_jobs`, see PLAN_062). The provenance is required to audit and to retract samples if a training run is later invalidated.

---

## Part 9 — Required dependencies

### 9.1 Python packages (add to requirements.txt)

```
torch>=2.4.0,<3.0.0
einops>=0.7.0,<1.0.0
flash-attn>=3.0.0,<4.0.0    # optional, Blackwell-specific
wandb>=0.16.0,<1.0.0          # experiment tracking
tsgm>=0.5.0,<1.0.0            # time-series generator harness
```

Note: `tab-ddpm` from yandex-research is not pip-installable; we'll vendor the relevant source into `app/services/synthetic/training/tabddpm/` and modify it for our preprocessing.

### 9.2 System requirements

- CUDA 12.4+ (Blackwell support)
- ~30 GB disk for model checkpoints across training runs (each checkpoint is 50–200 MB; we keep last 10)
- ~5 GB disk for training data caching (preprocessed XBRL + FRED arrays)

### 9.3 What we already have (do not reinstall)

- `scipy` (already in requirements per PLAN_057)
- PyTorch base — verify version
- SQLAlchemy, pandas, numpy

---

## Part 10 — Open research questions that will surface during implementation

These are unknowns we'll only resolve by actually training. Listed so the implementer isn't surprised:

1. **What's the minimum NAICS-4 × revenue-bucket cell size for TabDDPM to produce useful conditional samples?** The paper's smallest evaluated cell is ~100 rows. Cells smaller than that may need either (a) coarser conditioning (NAICS-2 instead of NAICS-4) or (b) fallback to v1 parametric.

2. **Does Diffusion-TS at L=60, H=24, T=500 actually capture FRED volatility clustering, or do we need to extend to L=120?** Only empirical testing tells us. If not, train a second model at L=120 and increment hyperparameters.

3. **How much does training data quality dominate over architecture?** PLAN_053 Phase C1 (EDGAR XBRL bulk ingest) needs to have shipped and produced clean data. If `public_company_financials` has < 30K rows or unusable nulls, retrain after data cleanup.

4. **Inference-time guidance scale `w` for TabDDPM** — paper recommends 1.0–3.0. The sweet spot depends on what we optimize: stricter adherence to NAICS condition (high w) trades off against tail-distribution coverage (low w). We'll grid-search w ∈ {1.0, 1.5, 2.0, 3.0} on the validation set.

5. **Does the Fourier-loss weight λ=0.01 from Diffusion-TS paper match our 8-channel FRED setup?** The paper used 1-channel data. Multi-channel may want different λ. Tune as a secondary hyperparameter.

---

## Part 11 — Comparison: v1 vs v2 expected delta

| Capability | v1 (parametric) | v2 (diffusion) |
|---|---|---|
| Marginal distribution of EBITDA margin | Unimodal Gaussian per sector | True multi-modal where applicable |
| Cross-feature correlation | Preserved via Cholesky | Preserved natively |
| Conditional sampling | NAICS+revenue bucket | NAICS-4 + revenue + state + year |
| Out-of-distribution conditions | Falls back to global priors | Refuses or returns wide-interval samples |
| FRED volatility clustering | None (O-U is uncorrelated) | Preserved via Fourier-loss |
| FRED fat tails | None (Gaussian shocks) | Heavy-tailed via diffusion |
| Generation latency (100 samples) | ~50 ms | ~300–500 ms (DDIM-50) |
| Training time | None | ~24 hrs concurrent |
| Inference VRAM | 0 | ~3 GB |
| Audit-trail provenance | Job-level | Job-level + model checkpoint hash |

The latency cost is real (~10× slower) but acceptable given the use case is offline scenario generation, not hot-path triage.

---

## Part 12 — References (all open access)

### Diffusion models foundations
- Ho, Jain, Abbeel — "Denoising Diffusion Probabilistic Models" — NeurIPS 2020 — [arXiv:2006.11239](https://arxiv.org/abs/2006.11239)
- Nichol & Dhariwal — "Improved Denoising Diffusion Probabilistic Models" — ICML 2021 — [arXiv:2102.09672](https://arxiv.org/abs/2102.09672)
- Song, Meng, Ermon — "Denoising Diffusion Implicit Models" (DDIM) — ICLR 2021 — [arXiv:2010.02502](https://arxiv.org/abs/2010.02502)
- Ho & Salimans — "Classifier-Free Diffusion Guidance" — NeurIPS Workshop 2022 — [arXiv:2207.12598](https://arxiv.org/abs/2207.12598)

### TabDDPM (Phase A1)
- Kotelnikov et al. — "TabDDPM: Modelling Tabular Data with Diffusion Models" — ICML 2023 — [arXiv:2209.15421](https://arxiv.org/abs/2209.15421) — [GitHub](https://github.com/yandex-research/tab-ddpm)
- Hoogeboom et al. — "Argmax Flows and Multinomial Diffusion" — NeurIPS 2021 — [arXiv:2102.05379](https://arxiv.org/abs/2102.05379)

### Diffusion-TS (Phase A2)
- Yuan & Qiao — "Interpretable Diffusion for General Time Series Generation" — ICLR 2024 — [arXiv:2403.01742](https://arxiv.org/abs/2403.01742) — [GitHub](https://github.com/Y-debug-sys/Diffusion-TS)
- Nikitin et al. — "TSGM: A Flexible Framework for Generative Modeling of Synthetic Time Series" — NeurIPS 2024 — [arXiv:2305.11567](https://arxiv.org/abs/2305.11567) — [GitHub](https://github.com/AlexanderVNikitin/tsgm)

### Validation / stylized facts
- Cont — "Empirical properties of asset returns: stylized facts and statistical issues" — Quantitative Finance 2001 (canonical reference for stylized facts)
- "Generation of Synthetic Financial Time Series by Diffusion Models" — Quantitative Finance 2025 — [arXiv:2410.18897](https://arxiv.org/abs/2410.18897) (validation methodology for financial diffusion models)

### Privacy / synthetic-discoveries
- "Does Differentially Private Synthetic Data Lead to Synthetic Discoveries?" — PubMed 2024
- Lin et al. — "Private Evolution (DPSDA)" — ICLR 2024 — [arXiv:2305.15560](https://arxiv.org/abs/2305.15560) (not used in v2 but reference for later DP layer)

### Internal references
- `docs/plans/completed/PLAN_052_data_interplay_synthetic_extensions.md` — origin research roadmap
- `docs/plans/completed/PLAN_053_synthetic_seed_strategy.md` — v1 scaffolding plan with EDGAR XBRL Phase C1
- `docs/plans/PLAN_057_statistical_validation_dashboard.md` — validation engine v1 (KS, chi-squared, Frobenius)
- `app/services/synthetic/validation.py` — current validation implementation (will extend)
- `app/services/synthetic/private_company_financials.py` — v1 generator (kept as fallback)
- `app/services/synthetic/macro_scenarios.py` — v1 generator (kept as fallback)

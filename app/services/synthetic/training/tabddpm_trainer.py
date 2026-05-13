"""
TabDDPM trainer — PLAN_062 W3 Phase A1 (private company financials).

Minimal-but-correct implementation from the paper (Kotelnikov et al., ICML 2023,
arXiv:2209.15421). v1 scope is narrower than the paper's full mixed-type model:
we diffuse continuous features only and use categorical features as conditioning
context (via classifier-free guidance) rather than running parallel multinomial
diffusion on them. Same insight, smaller surface area to get right.

Features (continuous, diffused):
  log_revenue, gross_margin, ebitda_margin, net_margin

Conditioning (categorical, NOT diffused — feeds into FiLM):
  revenue_bucket (<$10M / $10-50M / $50-250M / $250M-1B / $1-5B / $5B+)
  era (2010-14 / 2015-19 / 2020-25)

Training source: public_company_financials view (W1.A populated this in
the SEC bulk-ingest run — 76K rows across 1,879 CIKs).

Acceptance criteria (PLAN_062 §A1, applied to our minimal feature set):
- KS test on each of (gross_margin, ebitda_margin, net_margin) vs held-out — p > 0.05
- Frobenius norm of feature-correlation matrix vs held-out < 0.15
- Generation latency < 500ms for 100 samples
"""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from scipy import stats
from sqlalchemy import text

from app.core.database import get_session_factory
from app.services.synthetic.learned_generator_base import (
    LearnedSyntheticGenerator,
    ModelArtifact,
    TrainingConfig,
    ValidationReport,
)
from app.services.synthetic.training.diffusion_ts_trainer import (
    cosine_beta_schedule,
    SinusoidalTimeEmbedding,
)

logger = logging.getLogger(__name__)


# =======================================================================
# Hyperparameters
# =======================================================================

@dataclass
class TabDDPMConfig(TrainingConfig):
    """TabDDPM hyperparameters for the minimal continuous-features variant."""
    # Continuous feature names (will be quantile-normalized to ~N(0,1))
    continuous_features: List[str] = field(default_factory=lambda: [
        "log_revenue", "gross_margin", "ebitda_margin", "net_margin",
    ])
    # Categorical conditioning features (will be one-hot encoded).
    # rev_02 Step 1c iter 1: added naics_2 — passed Finance (3/3) but Manufacturing
    # / Professional Services / Construction still failed because those NAICS-2
    # buckets contain very heterogeneous sub-industries (Aero+Pharma+Steel all
    # in NAICS=31).
    # rev_02 Step 1c iter 2: added sic_code (~400 4-digit codes from EDGAR) for
    # much finer sector granularity. Training data has ~22 rows per SIC on
    # average; many top SICs have hundreds. Rare SICs will collapse to
    # neighbor-sector-mean behavior via the one-hot encoding's sparsity.
    categorical_features: List[str] = field(default_factory=lambda: [
        "revenue_bucket", "era", "naics_2", "sic_code",
    ])
    # Diffusion
    diffusion_steps: int = 1000
    noise_schedule: str = "cosine"
    # Network
    hidden_dim: int = 1024
    hidden_layers: int = 4
    dropout: float = 0.1
    # Training
    batch_size: int = 1024
    learning_rate: float = 1e-3
    epochs: int = 1500
    grad_clip: float = 1.0
    # Classifier-free guidance
    p_uncond: float = 0.1               # Probability of dropping conditioning during training
    inference_guidance_scale: float = 1.5
    # Sampling
    ddim_steps: int = 50
    # Mixed precision
    use_bf16: bool = True
    # Splits
    val_fraction: float = 0.1
    test_fraction: float = 0.1
    # Logging
    log_interval: int = 100


# =======================================================================
# Data extraction + preprocessing
# =======================================================================

REVENUE_BUCKETS = ["<$10M", "$10-50M", "$50-250M", "$250M-1B", "$1-5B", "$5B+"]
REVENUE_THRESHOLDS_USD = [10e6, 50e6, 250e6, 1e9, 5e9, float("inf")]


def revenue_to_bucket(rev_usd: float) -> str:
    for threshold, name in zip(REVENUE_THRESHOLDS_USD, REVENUE_BUCKETS):
        if rev_usd < threshold:
            return name
    return REVENUE_BUCKETS[-1]


def year_to_era(year: int) -> str:
    if year < 2015:
        return "2010-14"
    if year < 2020:
        return "2015-19"
    return "2020-25"


def load_training_dataframe() -> pd.DataFrame:
    """Pull from public_company_financials, derive margins + categorical buckets."""
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT
                revenue_usd,
                gross_profit_usd,
                ebitda_usd,
                net_income_usd,
                fiscal_year,
                naics_2,
                sic_code
            FROM public_company_financials
            WHERE fiscal_period = 'FY'
              AND revenue_usd > 1e6                     -- drop near-zero revenues
              AND revenue_usd < 1e12                    -- drop sentinel mega-values
              AND gross_profit_usd IS NOT NULL
              AND ebitda_usd IS NOT NULL
              AND net_income_usd IS NOT NULL
              AND fiscal_year BETWEEN 2010 AND 2025
        """)).fetchall()
    finally:
        db.close()

    if not rows:
        raise RuntimeError("No training rows from public_company_financials")

    df = pd.DataFrame(rows, columns=[
        "revenue_usd", "gross_profit_usd", "ebitda_usd", "net_income_usd", "fiscal_year",
        "naics_2", "sic_code",
    ])
    df = df.astype({
        "revenue_usd": "float64",
        "gross_profit_usd": "float64",
        "ebitda_usd": "float64",
        "net_income_usd": "float64",
        "fiscal_year": "int64",
    })
    # Impute missing NAICS-2 and SIC to "UN" / "0000" (UNKNOWN) so the row
    # stays in training rather than being dropped — better to model the
    # unknown-sector mass than leak those companies out of the training
    # distribution entirely.
    df["naics_2"] = df["naics_2"].fillna("UN")
    df["sic_code"] = df["sic_code"].fillna("0000")

    # Derived features
    df["log_revenue"] = np.log(df["revenue_usd"].clip(lower=1e6))
    df["gross_margin"] = df["gross_profit_usd"] / df["revenue_usd"]
    df["ebitda_margin"] = df["ebitda_usd"] / df["revenue_usd"]
    df["net_margin"] = df["net_income_usd"] / df["revenue_usd"]

    # Clip extreme outliers (some companies have weird ratios; we want the bulk)
    for col in ["gross_margin", "ebitda_margin", "net_margin"]:
        df[col] = df[col].clip(-2.0, 2.0)

    # Categorical conditioning
    df["revenue_bucket"] = df["revenue_usd"].apply(revenue_to_bucket)
    df["era"] = df["fiscal_year"].apply(year_to_era)

    return df


@dataclass
class FeatureSpec:
    continuous_features: List[str]
    categorical_features: List[str]
    continuous_means: List[float]
    continuous_stds: List[float]
    categorical_vocab: Dict[str, List[str]]  # feature_name → ordered list of values

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def fit(
        cls,
        df: pd.DataFrame,
        continuous_features: List[str],
        categorical_features: List[str],
    ) -> "FeatureSpec":
        # Quantile-normalize continuous to standard normal-ish: z-score is fine
        # at our N — quantile-transform is paper-style but adds a stateful object
        means = df[continuous_features].mean().values.tolist()
        stds = (df[continuous_features].std() + 1e-8).values.tolist()
        vocab = {
            col: sorted(df[col].dropna().unique().tolist())
            for col in categorical_features
        }
        return cls(
            continuous_features=continuous_features,
            categorical_features=categorical_features,
            continuous_means=means,
            continuous_stds=stds,
            categorical_vocab=vocab,
        )

    def encode_categorical(self, df: pd.DataFrame) -> np.ndarray:
        """One-hot encode categoricals; concatenated across all categorical columns."""
        parts = []
        for col in self.categorical_features:
            vocab = self.categorical_vocab[col]
            idx = df[col].map({v: i for i, v in enumerate(vocab)}).values
            onehot = np.zeros((len(df), len(vocab)), dtype=np.float32)
            for i, v in enumerate(idx):
                if not np.isnan(v):
                    onehot[i, int(v)] = 1.0
            parts.append(onehot)
        return np.concatenate(parts, axis=1) if parts else np.zeros((len(df), 0), dtype=np.float32)

    def encode_continuous(self, df: pd.DataFrame) -> np.ndarray:
        x = df[self.continuous_features].values.astype(np.float32)
        means = np.array(self.continuous_means, dtype=np.float32)
        stds = np.array(self.continuous_stds, dtype=np.float32)
        return (x - means) / stds

    def decode_continuous(self, z: np.ndarray) -> np.ndarray:
        means = np.array(self.continuous_means, dtype=np.float32)
        stds = np.array(self.continuous_stds, dtype=np.float32)
        return z * stds + means

    @property
    def n_cont(self) -> int:
        return len(self.continuous_features)

    @property
    def n_cat_dims(self) -> int:
        return sum(len(self.categorical_vocab[c]) for c in self.categorical_features)


# =======================================================================
# Denoising MLP — TabDDPM style
# =======================================================================

class TabularDenoiser(nn.Module):
    """4×1024 MLP with residual connections + FiLM conditioning on categoricals."""

    def __init__(
        self,
        n_cont: int,
        n_cond: int,
        hidden_dim: int,
        n_layers: int,
        dropout: float,
        time_dim: int = 128,
    ):
        super().__init__()
        self.n_cont = n_cont
        self.n_cond = n_cond
        self.time_dim = time_dim

        self.time_embed = SinusoidalTimeEmbedding(time_dim)
        self.time_mlp = nn.Sequential(
            nn.Linear(time_dim, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, hidden_dim * 2),  # for FiLM scale + shift
        )

        # Conditioning: project categorical-onehot to hidden_dim
        self.cond_mlp = nn.Sequential(
            nn.Linear(n_cond, hidden_dim),
            nn.GELU(),
            nn.Linear(hidden_dim, hidden_dim * 2),  # for FiLM scale + shift
        ) if n_cond > 0 else None

        self.proj_in = nn.Linear(n_cont, hidden_dim)

        self.layers = nn.ModuleList()
        for _ in range(n_layers):
            self.layers.append(nn.Sequential(
                nn.Linear(hidden_dim, hidden_dim),
                nn.GELU(),
                nn.Dropout(dropout),
                nn.Linear(hidden_dim, hidden_dim),
            ))

        self.proj_out = nn.Linear(hidden_dim, n_cont)

    def forward(
        self,
        x_t: torch.Tensor,        # (B, n_cont)
        t: torch.Tensor,          # (B,)
        cond: torch.Tensor,       # (B, n_cond)  — may be zeros for unconditional
    ) -> torch.Tensor:
        h = self.proj_in(x_t)

        # Time FiLM
        t_emb = self.time_mlp(self.time_embed(t))
        t_scale, t_shift = t_emb.chunk(2, dim=-1)

        # Conditional FiLM (cond can be all-zero for unconditional)
        if self.cond_mlp is not None:
            c_emb = self.cond_mlp(cond)
            c_scale, c_shift = c_emb.chunk(2, dim=-1)
        else:
            c_scale = c_shift = 0.0

        h = h * (1.0 + t_scale + c_scale) + (t_shift + c_shift)
        for layer in self.layers:
            h = h + layer(h)

        return self.proj_out(h)


# =======================================================================
# Diffusion model
# =======================================================================

class TabDDPMModel(nn.Module):
    def __init__(self, config: TabDDPMConfig, spec: FeatureSpec):
        super().__init__()
        self.config = config
        self.spec = spec
        self.denoiser = TabularDenoiser(
            n_cont=spec.n_cont,
            n_cond=spec.n_cat_dims,
            hidden_dim=config.hidden_dim,
            n_layers=config.hidden_layers,
            dropout=config.dropout,
        )

        betas = cosine_beta_schedule(config.diffusion_steps)
        alphas = 1.0 - betas
        ab = torch.cumprod(alphas, dim=0)
        self.register_buffer("betas", betas)
        self.register_buffer("alphas_cumprod", ab)
        self.register_buffer("sqrt_alphas_cumprod", torch.sqrt(ab))
        self.register_buffer("sqrt_one_minus_alphas_cumprod", torch.sqrt(1.0 - ab))

    def q_sample(self, x_0: torch.Tensor, t: torch.Tensor, noise: torch.Tensor) -> torch.Tensor:
        sa = self.sqrt_alphas_cumprod[t][:, None]
        som = self.sqrt_one_minus_alphas_cumprod[t][:, None]
        return sa * x_0 + som * noise

    def loss(self, x_0: torch.Tensor, cond: torch.Tensor) -> torch.Tensor:
        B = x_0.shape[0]
        t = torch.randint(0, self.config.diffusion_steps, (B,), device=x_0.device)
        noise = torch.randn_like(x_0)
        x_t = self.q_sample(x_0, t, noise)

        # Classifier-free guidance dropout — drop conditioning with prob p_uncond
        drop = (torch.rand(B, device=x_0.device) < self.config.p_uncond).float()
        cond_effective = cond * (1.0 - drop[:, None])

        noise_pred = self.denoiser(x_t, t, cond_effective)
        return F.mse_loss(noise_pred, noise)

    @torch.no_grad()
    def sample(
        self,
        cond: torch.Tensor,
        device: torch.device,
        ddim_steps: Optional[int] = None,
        guidance_scale: Optional[float] = None,
    ) -> torch.Tensor:
        """Sample one continuous-feature row per row of `cond`."""
        ddim_steps = ddim_steps or self.config.ddim_steps
        guidance_scale = guidance_scale if guidance_scale is not None else self.config.inference_guidance_scale

        n = cond.shape[0]
        T = self.config.diffusion_steps
        step_indices = torch.linspace(T - 1, 0, ddim_steps + 1).round().long().to(device)

        x_t = torch.randn(n, self.spec.n_cont, device=device)
        cond_zero = torch.zeros_like(cond)

        for i in range(ddim_steps):
            t_cur = step_indices[i].repeat(n)
            t_next = step_indices[i + 1]

            ab_cur = self.alphas_cumprod[t_cur][:, None]
            ab_next = self.alphas_cumprod[t_next].view(1, 1) if t_next >= 0 else torch.ones_like(ab_cur)

            # Classifier-free guidance: blend conditional + unconditional
            noise_pred_cond = self.denoiser(x_t, t_cur, cond)
            noise_pred_uncond = self.denoiser(x_t, t_cur, cond_zero)
            noise_pred = noise_pred_uncond + guidance_scale * (noise_pred_cond - noise_pred_uncond)

            x_0_pred = (x_t - torch.sqrt(1.0 - ab_cur) * noise_pred) / (torch.sqrt(ab_cur) + 1e-8)
            x_0_pred = torch.clamp(x_0_pred, -5.0, 5.0)
            x_t = torch.sqrt(ab_next) * x_0_pred + torch.sqrt(1.0 - ab_next) * noise_pred

        return x_t


# =======================================================================
# Trainer + LearnedSyntheticGenerator wrapper
# =======================================================================

class TabDDPMGenerator(LearnedSyntheticGenerator):
    model_name: str = "tabddpm_v1"
    schema_version: str = "v1"

    def __init__(self, config: TabDDPMConfig, spec: FeatureSpec):
        self.config = config
        self.spec = spec
        self.model: Optional[TabDDPMModel] = None
        self.device: Optional[torch.device] = None

    # ---------- ABC: train ----------

    @classmethod
    def train(cls, config: TrainingConfig) -> ModelArtifact:  # type: ignore[override]
        if not isinstance(config, TabDDPMConfig):
            raise TypeError("TabDDPMGenerator.train requires TabDDPMConfig")

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info("Training TabDDPM on device=%s", device)

        # 1. Load + prep data
        df = load_training_dataframe()
        logger.info("Training rows: %d (from public_company_financials)", len(df))

        spec = FeatureSpec.fit(
            df,
            continuous_features=config.continuous_features,
            categorical_features=config.categorical_features,
        )

        # Encode
        x_all = spec.encode_continuous(df)          # (N, n_cont)
        cond_all = spec.encode_categorical(df)     # (N, n_cat_dims)

        # Walk-forward-ish split: shuffle then split
        rng = np.random.default_rng(config.random_state)
        idx = rng.permutation(len(df))
        n_total = len(df)
        n_test = max(1, int(n_total * config.test_fraction))
        n_val = max(1, int(n_total * config.val_fraction))
        n_train = n_total - n_test - n_val

        idx_train = idx[:n_train]
        idx_val = idx[n_train : n_train + n_val]
        idx_test = idx[n_train + n_val :]

        x_train = torch.from_numpy(x_all[idx_train]).float()
        c_train = torch.from_numpy(cond_all[idx_train]).float()
        x_val = torch.from_numpy(x_all[idx_val]).float()
        c_val = torch.from_numpy(cond_all[idx_val]).float()

        train_ds = torch.utils.data.TensorDataset(x_train, c_train)
        val_ds = torch.utils.data.TensorDataset(x_val, c_val)
        train_loader = torch.utils.data.DataLoader(
            train_ds, batch_size=config.batch_size, shuffle=True, drop_last=False,
        )
        val_loader = torch.utils.data.DataLoader(
            val_ds, batch_size=config.batch_size, shuffle=False,
        )

        # 2. Model + optimizer
        model = TabDDPMModel(config, spec).to(device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=config.learning_rate)

        # 3. Train loop
        config.output_dir.mkdir(parents=True, exist_ok=True)
        best_val = float("inf")
        history: List[Dict] = []
        train_start = time.perf_counter()

        for epoch in range(config.epochs):
            model.train()
            train_losses = []
            for x_b, c_b in train_loader:
                x_b = x_b.to(device, non_blocking=True)
                c_b = c_b.to(device, non_blocking=True)
                with torch.amp.autocast(device_type="cuda", dtype=torch.bfloat16, enabled=config.use_bf16):
                    loss = model.loss(x_b, c_b)
                optimizer.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), config.grad_clip)
                optimizer.step()
                train_losses.append(loss.item())

            model.eval()
            val_losses = []
            with torch.no_grad():
                for x_b, c_b in val_loader:
                    x_b = x_b.to(device, non_blocking=True)
                    c_b = c_b.to(device, non_blocking=True)
                    with torch.amp.autocast(device_type="cuda", dtype=torch.bfloat16, enabled=config.use_bf16):
                        l = model.loss(x_b, c_b)
                    val_losses.append(l.item())

            train_loss = float(np.mean(train_losses))
            val_loss = float(np.mean(val_losses)) if val_losses else float("nan")
            elapsed = time.perf_counter() - train_start
            history.append({"epoch": epoch + 1, "train_loss": train_loss, "val_loss": val_loss, "elapsed_sec": elapsed})

            if epoch % config.log_interval == 0 or epoch == config.epochs - 1:
                logger.info("epoch %d/%d  train=%.4f  val=%.4f  elapsed=%.0fs",
                            epoch + 1, config.epochs, train_loss, val_loss, elapsed)

            if val_loss < best_val:
                best_val = val_loss
                torch.save({
                    "model_state": model.state_dict(),
                    "config": asdict(config),
                    "feature_spec": spec.to_dict(),
                    "epoch": epoch + 1,
                    "val_loss": val_loss,
                }, config.output_dir / "best.pt")

        # Final artifact
        torch.save({
            "model_state": model.state_dict(),
            "config": asdict(config),
            "feature_spec": spec.to_dict(),
            "epoch": config.epochs,
            "val_loss": val_losses[-1] if val_losses else None,
        }, config.output_dir / "final.pt")
        (config.output_dir / "training_history.json").write_text(json.dumps(history, indent=2))
        (config.output_dir / "metadata.json").write_text(json.dumps({
            "model_name": cls.model_name,
            "schema_version": cls.schema_version,
            "config": asdict(config),
            "n_train": n_train,
            "n_val": n_val,
            "n_test": n_test,
            "best_val_loss": best_val,
            "feature_spec": spec.to_dict(),
            "elapsed_sec": time.perf_counter() - train_start,
        }, indent=2, default=str))

        return ModelArtifact(
            path=config.output_dir,
            model_name=cls.model_name,
            schema_version=cls.schema_version,
            training_run_id=time.strftime("%Y%m%d_%H%M%S"),
            metrics={"best_val_loss": best_val},
            metadata={"n_train": n_train, "epochs": config.epochs},
        )

    # ---------- ABC: load ----------

    @classmethod
    def load(cls, artifact_path: Path) -> "TabDDPMGenerator":
        ckpt_path = artifact_path / "best.pt"
        if not ckpt_path.exists():
            ckpt_path = artifact_path / "final.pt"
        if not ckpt_path.exists():
            raise FileNotFoundError(f"No checkpoint in {artifact_path}")

        ckpt = torch.load(ckpt_path, map_location="cpu", weights_only=False)
        cfg_dict = ckpt["config"]
        cfg = TabDDPMConfig(
            input_path=Path(cfg_dict.get("input_path", "/")),
            output_dir=Path(cfg_dict.get("output_dir", artifact_path)),
        )
        for k, v in cfg_dict.items():
            if hasattr(cfg, k) and k not in ("input_path", "output_dir"):
                setattr(cfg, k, v)

        spec = FeatureSpec(**ckpt["feature_spec"])

        gen = cls(cfg, spec)
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        gen.device = device
        gen.model = TabDDPMModel(cfg, spec).to(device)
        gen.model.load_state_dict(ckpt["model_state"])
        gen.model.eval()
        logger.info("Loaded TabDDPM from %s", ckpt_path)
        return gen

    # ---------- ABC: sample ----------

    def sample(self, n: int, **conditions) -> pd.DataFrame:
        """Sample n synthetic rows. Conditioning kwargs: revenue_bucket, era."""
        if self.model is None:
            raise RuntimeError("Model not loaded")

        # Build conditioning vector
        cond_df = pd.DataFrame({
            "revenue_bucket": [conditions.get("revenue_bucket", "$50-250M")] * n,
            "era": [conditions.get("era", "2020-25")] * n,
        })
        cond_arr = self.spec.encode_categorical(cond_df)
        cond = torch.from_numpy(cond_arr).float().to(self.device)

        guidance = conditions.get("guidance_scale", self.config.inference_guidance_scale)
        ddim_steps = conditions.get("ddim_steps", self.config.ddim_steps)

        z = self.model.sample(cond, self.device, ddim_steps=ddim_steps, guidance_scale=guidance)
        z_np = z.float().cpu().numpy()
        x = self.spec.decode_continuous(z_np)

        df = pd.DataFrame(x, columns=self.spec.continuous_features)
        df["revenue_usd"] = np.exp(df["log_revenue"]).clip(1e6, 1e12)
        df["synthetic_model_name"] = self.model_name
        df["synthetic_schema_version"] = self.schema_version
        return df

    # ---------- ABC: validate ----------

    def validate(self, held_out: pd.DataFrame) -> ValidationReport:
        """Apply held-out data; sample n_synth = n_held_out rows; compute KS + Frobenius."""
        if self.model is None:
            raise RuntimeError("Model not loaded")

        n = len(held_out)
        # Match the conditioning to the held-out distribution
        cond_df = held_out[self.spec.categorical_features].copy()
        cond_arr = self.spec.encode_categorical(cond_df)
        cond = torch.from_numpy(cond_arr).float().to(self.device)
        z = self.model.sample(cond, self.device)
        synth_x = self.spec.decode_continuous(z.float().cpu().numpy())
        synth_df = pd.DataFrame(synth_x, columns=self.spec.continuous_features)

        # KS test on margins
        results: Dict[str, Dict[str, Any]] = {}
        all_pass = True
        for col in ["gross_margin", "ebitda_margin", "net_margin"]:
            if col not in held_out.columns:
                results[col] = {"skipped": True}
                continue
            real = held_out[col].dropna().values
            synth = synth_df[col].values
            ks_stat, ks_p = stats.ks_2samp(real, synth)
            passed = ks_p > 0.05
            all_pass = all_pass and passed
            results[col] = {
                "ks_statistic": float(ks_stat),
                "ks_p_value": float(ks_p),
                "passed": bool(passed),
            }

        # Frobenius on correlation matrix
        real_corr = held_out[self.spec.continuous_features].corr().values
        synth_corr = synth_df[self.spec.continuous_features].corr().values
        frob = float(np.linalg.norm(real_corr - synth_corr, "fro"))
        frob_pass = frob < 0.15
        all_pass = all_pass and frob_pass
        results["correlation_frobenius"] = {
            "value": frob,
            "threshold": 0.15,
            "passed": frob_pass,
        }

        return ValidationReport(
            model_name=self.model_name,
            all_passed=all_pass,
            metric_results=results,
            summary=f"KS + Frobenius across {len([k for k in results if k != 'correlation_frobenius'])} features",
        )


# =======================================================================
# CLI
# =======================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--epochs", type=int, default=1500)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--diffusion-steps", type=int, default=1000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = TabDDPMConfig(
        input_path=Path("/dev/null"),
        output_dir=Path(args.output_dir),
        epochs=args.epochs,
        batch_size=args.batch_size,
        diffusion_steps=args.diffusion_steps,
    )
    artifact = TabDDPMGenerator.train(config)
    print(f"\nTrained. Artifact: {artifact.path}")
    print(f"Metrics: {artifact.metrics}")


if __name__ == "__main__":
    main()

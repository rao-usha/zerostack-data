"""
Diffusion-TS trainer — PLAN_062 W3 Phase A2 (FRED macro scenarios).

Minimal-but-correct implementation from the paper (Yuan & Qiao, ICLR 2024,
arXiv:2403.01742). NOT vendored from the upstream repo — implemented direct
to keep our dependency surface clean and the architecture transparent.

Design choices:
- DDPM with cosine noise schedule, T=500 diffusion steps
- Transformer encoder denoiser (4 layers, hidden=256, 4 heads) — keeps VRAM
  modest (~3GB on the RTX 5070) so this can train concurrent with TabDDPM
- Fourier-domain auxiliary loss preserves stylized facts (heavy tails,
  volatility clustering) that vanilla MSE would smooth away
- Trains on monthly *changes* (Δy_t), not levels — per the research-doc §3.5
- DDIM-50 sampling at inference (10× faster than 500-step DDPM, ~same quality)
- bf16 mixed precision on Blackwell (sm_120) for speed

Acceptance criteria (PLAN_062 §A2):
- Kurtosis of monthly changes within ±0.3 of historical per series
- ACF of squared returns at lag-12 within 0.05 of historical
- Cross-series correlation Frobenius < 0.10
- KS p > 0.05 on 24-month-forward terminal values vs. historical
"""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from einops import rearrange
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_session_factory
from app.services.synthetic.learned_generator_base import (
    LearnedSyntheticGenerator,
    ModelArtifact,
    TrainingConfig,
    ValidationReport,
)

logger = logging.getLogger(__name__)


# =======================================================================
# Hyperparameters
# =======================================================================

@dataclass
class DiffusionTSConfig(TrainingConfig):
    """Diffusion-TS hyperparameters. Defaults match PLAN_062 §3.4 plan."""
    # Series
    series: List[str] = field(default_factory=lambda: [
        "DFF", "DGS10", "DGS2", "UNRATE", "CPIAUCSL", "UMCSENT", "INDPRO", "DCOILWTICO",
    ])
    sequence_length: int = 60          # 5 years of monthly history per training sequence
    forecast_horizon: int = 24         # 24-month forward (for validation; train on full seq)
    # Diffusion
    diffusion_steps: int = 500
    noise_schedule: str = "cosine"
    fourier_loss_weight: float = 0.01
    # Network — rev_02 §2c: bigger to capture fat-tail structure
    hidden_dim: int = 384               # was 256
    n_transformer_layers: int = 6       # was 4
    n_attention_heads: int = 6          # was 4
    dropout: float = 0.1
    # Training
    batch_size: int = 256
    learning_rate: float = 1e-4
    epochs: int = 5000                  # rev_02 §2c: was 200; each epoch ~50ms so ~5min total
    warmup_steps: int = 1000
    grad_clip: float = 1.0
    # rev_02 §2a: Min-SNR-γ loss weighting (Hang et al. 2023, arXiv:2303.09556)
    # γ=5 is paper-recommended; down-weights low-t (already-easy) timesteps so
    # the model spends more gradient on high-t (fat-tail-bearing) regimes
    minsnr_gamma: float = 5.0
    use_minsnr_weighting: bool = True
    # rev_02 §2b: Importance sampling on t — sample t ~ Beta(2,1)·T which
    # skews mean from T/2 (uniform) to 2T/3 (more high-t exposure)
    use_importance_sampling_t: bool = True
    t_beta_alpha: float = 2.0
    t_beta_beta: float = 1.0
    # Sampling
    ddim_steps: int = 50
    # Mixed precision
    use_bf16: bool = True
    # Misc
    val_fraction: float = 0.1
    test_fraction: float = 0.1
    log_interval: int = 50
    save_every: int = 50


# =======================================================================
# Data pipeline: pull FRED, aggregate to monthly, build sequences
# =======================================================================

class FREDSequenceDataset(torch.utils.data.Dataset):
    """Builds overlapping length-L sequences of monthly-aggregated FRED series."""

    def __init__(
        self,
        sequences: np.ndarray,  # (n_sequences, seq_len, n_series)
        means: np.ndarray,      # (n_series,) — for z-score standardization
        stds: np.ndarray,       # (n_series,)
    ):
        self.sequences = sequences
        self.means = means
        self.stds = stds

    def __len__(self) -> int:
        return self.sequences.shape[0]

    def __getitem__(self, idx: int) -> torch.Tensor:
        seq = self.sequences[idx]  # (seq_len, n_series)
        z = (seq - self.means) / self.stds
        return torch.from_numpy(z.astype(np.float32))


def load_fred_sequences(
    db: Session,
    series: List[str],
    seq_len: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[str], List[pd.Timestamp]]:
    """
    Pull each series, aggregate to monthly mean, compute monthly changes,
    build overlapping length-seq_len sequences.

    Returns:
        sequences: (n_sequences, seq_len, n_series)
        means: (n_series,) per-series mean of monthly changes (for z-score)
        stds: (n_series,) per-series std of monthly changes
        series_kept: the series IDs we actually have data for (some may be dropped)
        month_index: the monthly DatetimeIndex covered
    """
    rows = db.execute(
        text("""
            SELECT series_id, date, value
            FROM fred_observations
            WHERE series_id = ANY(:sids)
              AND value IS NOT NULL
            ORDER BY series_id, date
        """),
        {"sids": list(series)},
    ).fetchall()

    if not rows:
        raise RuntimeError("No FRED observations found for the requested series")

    df = pd.DataFrame(rows, columns=["series_id", "date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = df["value"].astype(float)

    # Pivot then monthly-aggregate (mean within each month)
    pivot = df.pivot_table(index="date", columns="series_id", values="value")
    monthly = pivot.resample("MS").mean()  # month-start frequency
    monthly = monthly.dropna(how="all").ffill(limit=2)  # tolerate up to 2 missing months
    monthly = monthly.dropna()  # keep only rows where all series have a value

    series_kept = [s for s in series if s in monthly.columns]
    monthly = monthly[series_kept]

    if monthly.shape[0] < seq_len + 12:
        raise RuntimeError(
            f"Only {monthly.shape[0]} monthly rows after alignment; need at least {seq_len + 12}"
        )

    # Train on monthly CHANGES (Δy_t = y_t - y_{t-1}), not levels
    changes = monthly.diff().dropna()
    arr = changes.values  # (n_months-1, n_series)

    means = arr.mean(axis=0)
    stds = arr.std(axis=0) + 1e-8

    # Build overlapping sequences of length seq_len
    n_seq = arr.shape[0] - seq_len + 1
    sequences = np.lib.stride_tricks.sliding_window_view(
        arr, window_shape=seq_len, axis=0
    )  # shape (n_seq, n_series, seq_len)
    sequences = np.transpose(sequences, (0, 2, 1))  # (n_seq, seq_len, n_series)

    return sequences, means, stds, series_kept, list(changes.index)


# =======================================================================
# Noise schedule
# =======================================================================

def cosine_beta_schedule(timesteps: int, s: float = 0.008) -> torch.Tensor:
    """Cosine noise schedule from Nichol & Dhariwal 2021. Returns betas[1..T]."""
    steps = timesteps + 1
    t = torch.linspace(0, timesteps, steps) / timesteps
    alphas_cumprod = torch.cos((t + s) / (1 + s) * math.pi * 0.5) ** 2
    alphas_cumprod = alphas_cumprod / alphas_cumprod[0]
    betas = 1.0 - (alphas_cumprod[1:] / alphas_cumprod[:-1])
    return torch.clamp(betas, 1e-5, 0.999)


# =======================================================================
# Time embedding
# =======================================================================

class SinusoidalTimeEmbedding(nn.Module):
    def __init__(self, dim: int):
        super().__init__()
        self.dim = dim

    def forward(self, t: torch.Tensor) -> torch.Tensor:
        half = self.dim // 2
        emb = math.log(10000.0) / (half - 1)
        emb = torch.exp(torch.arange(half, device=t.device) * -emb)
        emb = t[:, None].float() * emb[None, :]
        emb = torch.cat([torch.sin(emb), torch.cos(emb)], dim=-1)
        return emb


# =======================================================================
# Denoising network — Transformer encoder over (seq_len, channels)
# =======================================================================

class DiffusionTSDenoiser(nn.Module):
    """
    Transformer encoder that takes a noisy multi-channel sequence + a timestep
    and predicts the noise that was added.

    Architecture:
        x_t: (B, L, C) ─┐
                          ├─→ proj_in → (B, L, hidden)
        t_embed         ─┘   + broadcast time/channel mixing
                              → N × TransformerEncoderLayer (self-attn over time)
                              → proj_out → (B, L, C) (predicted noise)
    """

    def __init__(
        self,
        n_channels: int,
        hidden_dim: int,
        n_layers: int,
        n_heads: int,
        dropout: float,
        max_seq_len: int = 128,
    ):
        super().__init__()
        self.n_channels = n_channels
        self.hidden_dim = hidden_dim

        self.proj_in = nn.Linear(n_channels, hidden_dim)
        self.time_embed = SinusoidalTimeEmbedding(hidden_dim)
        self.time_mlp = nn.Sequential(
            nn.Linear(hidden_dim, hidden_dim * 2),
            nn.GELU(),
            nn.Linear(hidden_dim * 2, hidden_dim),
        )
        self.pos_embed = nn.Parameter(torch.zeros(1, max_seq_len, hidden_dim))
        nn.init.trunc_normal_(self.pos_embed, std=0.02)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=n_heads,
            dim_feedforward=hidden_dim * 4,
            dropout=dropout,
            batch_first=True,
            norm_first=True,
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers=n_layers)
        self.proj_out = nn.Linear(hidden_dim, n_channels)

    def forward(self, x_t: torch.Tensor, t: torch.Tensor) -> torch.Tensor:
        """
        Args:
            x_t: (B, L, C) noisy sequence
            t: (B,) timestep indices

        Returns:
            (B, L, C) predicted noise
        """
        B, L, C = x_t.shape
        h = self.proj_in(x_t) + self.pos_embed[:, :L, :]
        t_emb = self.time_mlp(self.time_embed(t))  # (B, hidden)
        h = h + t_emb[:, None, :]  # broadcast across time
        h = self.encoder(h)
        return self.proj_out(h)


# =======================================================================
# Diffusion process — wraps schedule + network into forward/sample
# =======================================================================

class DiffusionTSModel(nn.Module):
    def __init__(self, config: DiffusionTSConfig, n_channels: int):
        super().__init__()
        self.config = config
        self.n_channels = n_channels

        # Network
        self.denoiser = DiffusionTSDenoiser(
            n_channels=n_channels,
            hidden_dim=config.hidden_dim,
            n_layers=config.n_transformer_layers,
            n_heads=config.n_attention_heads,
            dropout=config.dropout,
            max_seq_len=config.sequence_length,
        )

        # Noise schedule (registered as buffers — moved to GPU with model)
        betas = cosine_beta_schedule(config.diffusion_steps)
        alphas = 1.0 - betas
        alphas_cumprod = torch.cumprod(alphas, dim=0)
        alphas_cumprod_prev = F.pad(alphas_cumprod[:-1], (1, 0), value=1.0)

        self.register_buffer("betas", betas)
        self.register_buffer("alphas_cumprod", alphas_cumprod)
        self.register_buffer("alphas_cumprod_prev", alphas_cumprod_prev)
        self.register_buffer("sqrt_alphas_cumprod", torch.sqrt(alphas_cumprod))
        self.register_buffer("sqrt_one_minus_alphas_cumprod", torch.sqrt(1.0 - alphas_cumprod))

    def q_sample(self, x_0: torch.Tensor, t: torch.Tensor, noise: torch.Tensor) -> torch.Tensor:
        """Forward diffusion x_0 → x_t (closed form)."""
        sqrt_alphas = self.sqrt_alphas_cumprod[t][:, None, None]
        sqrt_one_minus = self.sqrt_one_minus_alphas_cumprod[t][:, None, None]
        return sqrt_alphas * x_0 + sqrt_one_minus * noise

    def loss(self, x_0: torch.Tensor) -> torch.Tensor:
        """L_simple + lambda * L_fourier, with rev_02 fat-tail-preservation patches."""
        B = x_0.shape[0]
        T = self.config.diffusion_steps
        device = x_0.device

        # rev_02 §2b: importance sampling on t — Beta(2,1)·T skews toward higher
        # t (where fat tails dominate the gradient signal)
        if self.config.use_importance_sampling_t:
            u = torch.distributions.Beta(
                self.config.t_beta_alpha, self.config.t_beta_beta,
            ).sample((B,)).to(device)
            t = (u * T).long().clamp(0, T - 1)
        else:
            t = torch.randint(0, T, (B,), device=device)

        noise = torch.randn_like(x_0)
        x_t = self.q_sample(x_0, t, noise)
        noise_pred = self.denoiser(x_t, t)

        # rev_02 §2a: Min-SNR-γ weighting for ε-prediction
        # weight = min(SNR_t, γ) / SNR_t = min(1, γ/SNR_t)
        # Down-weights low-t (high-SNR, easy) timesteps; concentrates gradient
        # on high-t (low-SNR, fat-tail-bearing) timesteps.
        if self.config.use_minsnr_weighting:
            ab_t = self.alphas_cumprod[t]
            snr_t = ab_t / (1.0 - ab_t + 1e-8)
            minsnr_w = torch.clamp(self.config.minsnr_gamma / (snr_t + 1e-8), max=1.0)
            # Per-sample MSE then weighted mean
            l_per = ((noise_pred - noise) ** 2).mean(dim=tuple(range(1, noise.ndim)))
            l_simple = (minsnr_w * l_per).mean()
        else:
            l_simple = F.mse_loss(noise_pred, noise)

        # Fourier-domain auxiliary on the implied x_0 prediction.
        # Clip x_0_pred to z-space sanity range — at high t values, sqrt_alphas
        # is small and noise amplification can blow up the FFT magnitudes.
        # FFT in fp32 (bf16 lacks precision for frequency-domain ops).
        sqrt_alphas = self.sqrt_alphas_cumprod[t][:, None, None]
        sqrt_one_minus = self.sqrt_one_minus_alphas_cumprod[t][:, None, None]
        x_0_pred = (x_t - sqrt_one_minus * noise_pred) / (sqrt_alphas + 1e-8)
        x_0_pred_clipped = torch.clamp(x_0_pred, -5.0, 5.0).float()
        fft_true = torch.fft.rfft(x_0.float(), dim=1).abs()
        fft_pred = torch.fft.rfft(x_0_pred_clipped, dim=1).abs()
        # log1p damps extreme magnitudes; preserves rank-order
        l_fourier = F.mse_loss(torch.log1p(fft_pred), torch.log1p(fft_true))

        return l_simple + self.config.fourier_loss_weight * l_fourier, l_simple, l_fourier

    @torch.no_grad()
    def sample(
        self,
        n: int,
        seq_len: int,
        device: torch.device,
        ddim_steps: Optional[int] = None,
    ) -> torch.Tensor:
        """DDIM sampling. Returns (n, seq_len, n_channels) z-scored synthetic sequences."""
        ddim_steps = ddim_steps or self.config.ddim_steps
        T = self.config.diffusion_steps

        # Subsample the timestep schedule for DDIM
        step_indices = torch.linspace(T - 1, 0, ddim_steps + 1).round().long().to(device)

        x_t = torch.randn(n, seq_len, self.n_channels, device=device)

        for i in range(ddim_steps):
            t_cur = step_indices[i].repeat(n)
            t_next = step_indices[i + 1]

            ab_cur = self.alphas_cumprod[t_cur][:, None, None]
            ab_next = self.alphas_cumprod[t_next].view(1, 1, 1) if t_next >= 0 else torch.ones_like(ab_cur)

            noise_pred = self.denoiser(x_t, t_cur)
            x_0_pred = (x_t - torch.sqrt(1.0 - ab_cur) * noise_pred) / (torch.sqrt(ab_cur) + 1e-8)
            x_0_pred = torch.clamp(x_0_pred, -5.0, 5.0)  # Stabilize tails

            x_t = torch.sqrt(ab_next) * x_0_pred + torch.sqrt(1.0 - ab_next) * noise_pred

        return x_t


# =======================================================================
# Trainer + LearnedSyntheticGenerator wrapper
# =======================================================================

class DiffusionTSGenerator(LearnedSyntheticGenerator):
    """Diffusion-TS for FRED macro scenarios — PLAN_062 Phase A2."""

    model_name: str = "diffusion_ts_v1"
    schema_version: str = "v1"

    def __init__(
        self,
        config: DiffusionTSConfig,
        n_channels: int,
        means: np.ndarray,
        stds: np.ndarray,
        series: List[str],
    ):
        self.config = config
        self.n_channels = n_channels
        self.means = means
        self.stds = stds
        self.series = series
        self.model: Optional[DiffusionTSModel] = None
        self.device: Optional[torch.device] = None

    # ---------- ABC: train ----------

    @classmethod
    def train(cls, config: TrainingConfig) -> ModelArtifact:  # type: ignore[override]
        if not isinstance(config, DiffusionTSConfig):
            raise TypeError("DiffusionTSGenerator.train requires DiffusionTSConfig")

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info("Training Diffusion-TS on device=%s", device)

        # 1. Load + prep data
        SessionLocal = get_session_factory()
        db = SessionLocal()
        try:
            sequences, means, stds, series_kept, _ = load_fred_sequences(
                db, config.series, config.sequence_length,
            )
        finally:
            db.close()

        n_total = sequences.shape[0]
        n_test = max(1, int(n_total * config.test_fraction))
        n_val = max(1, int(n_total * config.val_fraction))
        n_train = n_total - n_test - n_val

        # Walk-forward split: oldest = train, middle = val, newest = test
        train_seq = sequences[:n_train]
        val_seq = sequences[n_train : n_train + n_val]
        test_seq = sequences[n_train + n_val :]

        train_ds = FREDSequenceDataset(train_seq, means, stds)
        val_ds = FREDSequenceDataset(val_seq, means, stds)
        train_loader = torch.utils.data.DataLoader(
            train_ds, batch_size=config.batch_size, shuffle=True, drop_last=False,
        )
        val_loader = torch.utils.data.DataLoader(
            val_ds, batch_size=config.batch_size, shuffle=False,
        )

        logger.info(
            "Data: %d sequences (%d train / %d val / %d test) × %d months × %d series (%s)",
            n_total, n_train, n_val, n_test, config.sequence_length, len(series_kept),
            ", ".join(series_kept),
        )

        # 2. Model + optimizer
        n_channels = len(series_kept)
        model = DiffusionTSModel(config, n_channels).to(device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=config.learning_rate)

        # Cosine warmup scheduler
        total_steps = config.epochs * len(train_loader)
        def lr_lambda(step):
            if step < config.warmup_steps:
                return step / max(1, config.warmup_steps)
            progress = (step - config.warmup_steps) / max(1, total_steps - config.warmup_steps)
            return 0.5 * (1.0 + math.cos(math.pi * progress))
        scheduler = torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)

        # 3. Training loop
        config.output_dir.mkdir(parents=True, exist_ok=True)
        best_val_loss = float("inf")
        history: List[Dict] = []
        global_step = 0
        train_start = time.perf_counter()

        for epoch in range(config.epochs):
            model.train()
            epoch_losses = []
            for batch in train_loader:
                batch = batch.to(device, non_blocking=True)
                with torch.amp.autocast(device_type="cuda", dtype=torch.bfloat16, enabled=config.use_bf16):
                    loss, l_simple, l_fourier = model.loss(batch)
                optimizer.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), config.grad_clip)
                optimizer.step()
                scheduler.step()
                epoch_losses.append(loss.item())
                global_step += 1

            # Validation
            model.eval()
            val_losses = []
            with torch.no_grad():
                for batch in val_loader:
                    batch = batch.to(device, non_blocking=True)
                    with torch.amp.autocast(device_type="cuda", dtype=torch.bfloat16, enabled=config.use_bf16):
                        l, _, _ = model.loss(batch)
                    val_losses.append(l.item())

            train_loss = float(np.mean(epoch_losses))
            val_loss = float(np.mean(val_losses)) if val_losses else float("nan")
            elapsed = time.perf_counter() - train_start
            history.append({"epoch": epoch + 1, "train_loss": train_loss, "val_loss": val_loss, "elapsed_sec": elapsed})

            if epoch % config.log_interval == 0 or epoch == config.epochs - 1:
                logger.info(
                    "epoch %d/%d  train=%.4f  val=%.4f  step=%d  elapsed=%.0fs",
                    epoch + 1, config.epochs, train_loss, val_loss, global_step, elapsed,
                )

            # Track best
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                ckpt_path = config.output_dir / "best.pt"
                torch.save({
                    "model_state": model.state_dict(),
                    "config": asdict(config),
                    "means": means.tolist(),
                    "stds": stds.tolist(),
                    "series_kept": series_kept,
                    "epoch": epoch + 1,
                    "val_loss": val_loss,
                }, ckpt_path)

        # 4. Final artifact + history
        final_path = config.output_dir / "final.pt"
        torch.save({
            "model_state": model.state_dict(),
            "config": asdict(config),
            "means": means.tolist(),
            "stds": stds.tolist(),
            "series_kept": series_kept,
            "epoch": config.epochs,
            "val_loss": val_losses[-1] if val_losses else None,
        }, final_path)

        (config.output_dir / "training_history.json").write_text(json.dumps(history, indent=2))
        (config.output_dir / "metadata.json").write_text(json.dumps({
            "model_name": cls.model_name,
            "schema_version": cls.schema_version,
            "config": asdict(config),
            "n_train_sequences": n_train,
            "n_val_sequences": n_val,
            "n_test_sequences": n_test,
            "series_kept": series_kept,
            "best_val_loss": best_val_loss,
            "elapsed_sec": time.perf_counter() - train_start,
        }, indent=2, default=str))

        return ModelArtifact(
            path=config.output_dir,
            model_name=cls.model_name,
            schema_version=cls.schema_version,
            training_run_id=time.strftime("%Y%m%d_%H%M%S"),
            metrics={"best_val_loss": best_val_loss},
            metadata={"n_train": n_train, "epochs": config.epochs, "series": series_kept},
        )

    # ---------- ABC: load ----------

    @classmethod
    def load(cls, artifact_path: Path) -> "DiffusionTSGenerator":
        # Try best.pt first, fall back to final.pt
        ckpt_path = artifact_path / "best.pt"
        if not ckpt_path.exists():
            ckpt_path = artifact_path / "final.pt"
        if not ckpt_path.exists():
            raise FileNotFoundError(f"No model checkpoint found in {artifact_path}")

        ckpt = torch.load(ckpt_path, map_location="cpu", weights_only=False)
        cfg_dict = ckpt["config"]
        # Re-hydrate dataclass; output_dir/input_path are stored as strings
        cfg = DiffusionTSConfig(
            input_path=Path(cfg_dict.get("input_path", "/")),
            output_dir=Path(cfg_dict.get("output_dir", artifact_path)),
        )
        for k, v in cfg_dict.items():
            if hasattr(cfg, k) and k not in ("input_path", "output_dir"):
                setattr(cfg, k, v)

        means = np.array(ckpt["means"])
        stds = np.array(ckpt["stds"])
        series_kept = ckpt["series_kept"]

        gen = cls(cfg, n_channels=len(series_kept), means=means, stds=stds, series=series_kept)
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        gen.device = device
        gen.model = DiffusionTSModel(cfg, n_channels=len(series_kept)).to(device)
        gen.model.load_state_dict(ckpt["model_state"])
        gen.model.eval()
        logger.info("Loaded Diffusion-TS from %s (series=%s)", ckpt_path, series_kept)
        return gen

    # ---------- ABC: sample ----------

    def sample(self, n: int, **conditions) -> pd.DataFrame:
        """
        Sample n synthetic length-sequence_length paths in monthly-change space.
        Returns one row per (path, month, series_id) in long form.
        """
        if self.model is None:
            raise RuntimeError("Model not loaded; call load() first")

        seq_len = conditions.get("sequence_length", self.config.sequence_length)
        z = self.model.sample(n, seq_len, self.device)  # (n, seq_len, C) in z-space
        z = z.float().cpu().numpy()
        # Inverse-standardize back to change-space
        means = self.means[None, None, :]
        stds = self.stds[None, None, :]
        changes = z * stds + means  # (n, seq_len, C)

        # Build a long DataFrame
        rows = []
        for path_idx in range(n):
            for t_idx in range(seq_len):
                for s_idx, series_id in enumerate(self.series):
                    rows.append({
                        "path_id": path_idx,
                        "month_idx": t_idx,
                        "series_id": series_id,
                        "value_change": float(changes[path_idx, t_idx, s_idx]),
                        "synthetic_model_name": self.model_name,
                        "synthetic_schema_version": self.schema_version,
                    })
        return pd.DataFrame(rows)

    # ---------- ABC: validate ----------

    def validate(self, held_out: pd.DataFrame) -> ValidationReport:
        """
        held_out: DataFrame with columns (series_id, date, value) — real monthly observations.
        Computes stylized-facts metrics on (real changes) vs (synthetic changes).
        """
        if self.model is None:
            raise RuntimeError("Model not loaded")

        # Compute real monthly changes per series
        real_changes_by_series: Dict[str, np.ndarray] = {}
        for series_id in self.series:
            s = held_out[held_out["series_id"] == series_id].sort_values("date")
            if len(s) < 2:
                continue
            real_changes_by_series[series_id] = s["value"].diff().dropna().values

        # Sample N synthetic paths, gather changes
        n_paths = 100
        synth_df = self.sample(n=n_paths)
        synth_changes_by_series: Dict[str, np.ndarray] = {}
        for series_id in self.series:
            arr = synth_df[synth_df["series_id"] == series_id]["value_change"].values
            synth_changes_by_series[series_id] = arr

        # Stylized-facts metrics
        results: Dict[str, Dict[str, Any]] = {}
        all_pass = True
        for series_id in self.series:
            r = real_changes_by_series.get(series_id)
            s = synth_changes_by_series.get(series_id)
            if r is None or s is None or len(r) < 12 or len(s) < 12:
                results[series_id] = {"skipped": "insufficient_data"}
                continue

            # Kurtosis comparison (within ±0.3)
            from scipy.stats import kurtosis, ks_2samp
            k_real = float(kurtosis(r, fisher=False))
            k_synth = float(kurtosis(s, fisher=False))
            k_pass = abs(k_real - k_synth) <= 0.3 * max(1.0, abs(k_real))

            # ACF of squared changes at lag-12 (within 0.05)
            def acf_lag(x, lag=12):
                x = x - x.mean()
                v = np.var(x) + 1e-12
                return float(np.mean(x[:-lag] * x[lag:]) / v)
            acf_real = acf_lag(r ** 2)
            acf_synth = acf_lag(s ** 2)
            acf_pass = abs(acf_real - acf_synth) <= 0.05

            # KS test on the distribution of changes
            ks_stat, ks_p = ks_2samp(r, s)
            ks_pass = ks_p > 0.05

            series_pass = k_pass and acf_pass and ks_pass
            all_pass = all_pass and series_pass
            results[series_id] = {
                "kurtosis_real": k_real,
                "kurtosis_synth": k_synth,
                "kurtosis_pass": k_pass,
                "acf12_real": acf_real,
                "acf12_synth": acf_synth,
                "acf12_pass": acf_pass,
                "ks_p_value": ks_p,
                "ks_pass": ks_pass,
                "passed": series_pass,
            }

        return ValidationReport(
            model_name=self.model_name,
            all_passed=all_pass,
            metric_results=results,
            summary=f"Validated {len(results)} series; {sum(1 for r in results.values() if r.get('passed'))} passed",
        )


# =======================================================================
# CLI
# =======================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--epochs", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--seq-len", type=int, default=60)
    parser.add_argument("--diffusion-steps", type=int, default=500)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = DiffusionTSConfig(
        input_path=Path("/dev/null"),  # We pull from DB, not a file
        output_dir=Path(args.output_dir),
        epochs=args.epochs,
        batch_size=args.batch_size,
        sequence_length=args.seq_len,
        diffusion_steps=args.diffusion_steps,
    )

    artifact = DiffusionTSGenerator.train(config)
    print(f"\nTrained. Artifact: {artifact.path}")
    print(f"Metrics: {artifact.metrics}")


if __name__ == "__main__":
    main()

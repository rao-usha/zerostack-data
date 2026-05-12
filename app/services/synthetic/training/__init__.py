"""
Training pipelines for v2 learned synthetic generators (PLAN_062 W2+).

Subpackages and files under here host:
- TabDDPM trainer (Phase A1 — private financials)
- Diffusion-TS trainer (Phase A2 — macro scenarios)
- (Future) RotatE / graph completion trainers (Phase B)

Training is deliberately decoupled from inference. Trainers run as separate
Python processes (potentially on a dedicated GPU host), produce model
artifacts to `app/services/synthetic/models/`, and the API container's
inference path loads those artifacts read-only.

This separation means:
- The API container does NOT need torch+CUDA installed (inference uses
  lighter artifacts — LightGBM .txt files, ONNX, etc.)
- Training environments can pin specific CUDA / cuDNN versions independently
- Failed training runs don't crash production inference
"""

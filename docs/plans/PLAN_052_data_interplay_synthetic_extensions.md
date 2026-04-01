# PLAN_052 — Data Source Interplay & Synthetic Data Extension Strategy

**Status:** Complete — Research integrated
**Date:** 2026-03-31
**Scope:** Cross-source architecture + synthetic data research → extension roadmap
**Context:** Nexdata has 47 ingested public data sources across 60+ DB tables. This plan maps how those sources interplay with one another, then identifies where synthetic data generation research can extend coverage into gaps that public data alone cannot fill.

---

## Part 1 — The Source Interaction Graph

Every source in Nexdata connects to at least two others via shared join keys. The table below maps the primary linkage architecture.

### 1.1 Core Join Keys Across Sources

| Join Key | Type | Sources That Share It |
|----------|------|-----------------------|
| `cik` | Company identity | SEC Edgar, Form D, Form ADV, EDGAR XBRL, PE Collection, LP Collection |
| `naics_code` | Industry classification | SAM.gov, USAspending, EPA ECHO, OSHA, BLS OES, Census CBP, Job Postings |
| `state_fips` | Geography: State | BLS LAUS, Census ACS, BEA Regional, EIA, USDA, EPA, FEMA, IRS SOI, FDIC |
| `county_fips` | Geography: County | Census ACS, BLS, USDA, EPA ECHO, OSHA, FEMA, IRS SOI, Investor Intel |
| `zip_code` | Geography: ZIP | NPPES, Yelp, EPA ECHO, OSHA, SAM.gov, Foot Traffic, FCC Broadband |
| `latitude/longitude` | Geography: Coords | EIA power plants, NOAA, EPA ECHO, NOAA, FEMA, Foot Traffic, Yelp |
| `npi` | Person/org identity | NPPES, CMS, FDA Device |
| `uei` / `cage_code` | Contractor identity | SAM.gov, USAspending |
| `series_id` | Time series | FRED, BLS (all tables), EIA |
| `year` / `period` | Time alignment | All temporal sources |
| `company_name` (fuzzy) | Entity name | PE Collection, People, Form D, SEC, Yelp, OSHA, EPA, SAM.gov |
| `linkedin_url` | Person identity | People collection, PE people |
| `eia_plant_id` | Energy asset | EIA power plants, Site Intelligence |

---

### 1.2 The Eight Signal Chains

These are the cross-source workflows that produce computable intelligence products. Each chain connects 3–8 sources.

---

#### Chain 1 — Deal Environment Scoring *(Already Built — PLAN_048)*

```
FRED (DFF, DGS10, DGS2, UMCSENT, CPIAUCSL)
   ↓ rate environment, yield curve, consumer sentiment, CPI
BLS CES (sector employment by NAICS super-sector)
   ↓ sector labor momentum
────────────────────────────────────────────
→ DealEnvironmentScore per PE sector (0–100, A/B/C/D)
```

**Current gaps:**
- EIA energy prices not wired in (energy/logistics sector accuracy)
- CFTC COT not wired in (commodity positioning for materials/energy)
- International econ FX rates not wired in (exit environment for cross-border deals)
- BEA GDP by Industry not wired in (sector growth rate factor)

**Extension:** Wire EIA DCOILWTICO (already in FRED proxy) + BEA GDP industry growth rates + OECD CLI (leading indicator composite) to add two more factors: "Sector revenue growth" and "Global demand signal."

---

#### Chain 2 — Company Diligence Composite

```
SAM.gov → government revenue concentration score
USAspending → contract pipeline, customer concentration
EPA ECHO → environmental liability (penalty amounts, violations)
OSHA → safety violation history (total penalties, repeat violations)
CourtListener → litigation exposure (party name matching)
FDIC → banking health (for financial sector targets)
USPTO → patent defensibility (assignee matching, citation count)
Job Postings → hiring growth trend (headcount growth proxy)
────────────────────────────────────────────────────────────
→ 6-factor company health score: Revenue Risk + Environmental + Safety + Legal + Innovation + Growth
```

**Join logic:**
- All sources joined on `company_name` (fuzzy) + `state` + `naics_code`
- FDIC joined on company name (for banks, credit unions)
- USPTO joined on `assignee_name` → CIK via SEC name match

**This does not exist yet.** Would be the "target screening" product.

---

#### Chain 3 — LP → GP Relationship Graph

```
SEC Form 990 (PE Extractor) → LP endowment size, GP commitments listed
SEC 13F → LP institutional equity holdings
CAFR scraper → public pension commitments by GP/strategy
SEC Form D (related_persons) → GP as placement agent/exec
SEC ADV → GP registration, AUM, strategy, minimum commitment
People Collection → LP investment committee members
────────────────────────────────────────────────────
→ LP Conviction Score (Built — PLAN_037)
→ NEW: GP Pipeline Score (which GPs are LP-favored, which are under-allocated)
→ NEW: LP→GP commitment graph (bipartite network)
```

**Current gap:** Form D `related_persons` field contains GP principals — crossing this to LP 990 data + PE Collection GP profiles creates a tripartite: LP → Fund → GP Firm. Only ~40% coverage right now.

---

#### Chain 4 — Executive Signal → Deal Sourcing Alerts

```
People Collection (company_people, leadership_changes)
   → officer departures, new C-suite hires (transition signals)
SEC DEF 14A / 8-K (filed executive changes)
   → formal departure/appointment announcements
Job Postings (C-suite, VP-level roles)
   → active executive search = company in transition
EDGAR 10-K (Item 1A risk factors)
   → "key person dependency" language detection
────────────────────────────────────────────────────────
→ Leadership Event Signal score:
   - "Succession in progress" flag
   - "Founder transition" flag (founder not in current roster)
   - "Management buildup" flag (hiring across multiple VP roles)
   All scored + dated for deal sourcing timeline
```

---

#### Chain 5 — Site Selection Intelligence *(Partially Built)*

```
EIA Power Plants → capacity, fuel mix, grid operator, CO2 rate
EIA Utility Territory → utility provider, rate class, reliability
NOAA → climate observations, extreme event history
FEMA → disaster declaration history, flood zone overlap
EPA ECHO → environmental violations at nearby facilities
OSHA → incident rates in the area/industry
FCC Broadband → fiber availability, provider count, speeds
Census ACS B01003 / B19013 → population, household income
BLS LAUS → state unemployment, labor availability
USDA QuickStats → agricultural land use context
AFDC → EV charging infrastructure density
────────────────────────────────────────────────────────
→ Site Score: Power Access + Climate Risk + Workforce + Connectivity + Regulatory Environment
```

---

#### Chain 6 — Portfolio Monitoring Alerts

```
BLS CES sector employment (monthly) → sector contraction signals
EIA petroleum/gas prices (monthly) → energy input cost pressure
FRED interest rates (daily) → refinancing risk (DFF, DGS10)
BLS LAUS state (monthly) → geographic portfolio labor market
OSHA violations (new filings) → operational incidents at NAICS+state level
EPA ECHO (new violations) → environmental incidents at NAICS+state level
Treasury yields → distribution capacity assumptions
────────────────────────────────────────────────────────────────
→ Portfolio Macro Stress Score per holding (sector × geo × leverage profile)
→ Monthly "push alert" if sector employment drops > threshold
```

---

#### Chain 7 — Healthcare Vertical Intelligence

```
NPPES (NPI registry) → provider discovery, specialty, practice location
CMS physician compare → accreditation, quality metrics
CMS utilization data → Medicare procedure volumes, revenue proxy
Census ACS demographics → patient population, insurance rate
Yelp → patient-facing presence, sentiment, competition density
FDA device registrations → procedure capability (Botox, laser, etc.)
OSHA → clinic safety incidents
────────────────────────────────────────────────────────────────
→ Practice profile: revenue estimate, patient demand, regulatory risk, competitive positioning
→ Acquisition score for healthcare roll-up verticals
```

---

#### Chain 8 — Roll-Up Market Attractiveness

```
Census County Business Patterns (CBP) → establishment count by NAICS+county
Census ACS B19013 → median household income by ZIP
BLS LAUS → unemployment by state
BEA Regional GDP → MSA-level economic growth rate
USDA / agricultural context → rural market sizing
Yelp → business density + review volume (proxy for market vitality)
NPPES → healthcare provider density (for healthcare verticals)
AFDC → EV charging infrastructure (for auto service verticals)
────────────────────────────────────────────────────────────────
→ Roll-up Market Score: Fragmentation + Affluence + Growth + Accessibility
→ County-level heat map: "where to build the roll-up"
```

---

### 1.3 Source Contribution Matrix

For each major intelligence product, which sources contribute:

| Product | FRED | BLS | BEA | EIA | Census | SEC | Form D | PE Coll | People | Yelp | NPPES | SAM | EPA | OSHA | USDA | Intl |
|---------|------|-----|-----|-----|--------|-----|--------|---------|--------|------|-------|-----|-----|------|------|------|
| Deal Environment Score | ✓ | ✓ | — | ○ | — | — | — | — | — | — | — | — | — | — | — | ○ |
| Company Diligence | — | ○ | — | — | — | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | ✓ | ✓ | — | — |
| LP→GP Graph | ✓ | — | — | — | — | ✓ | ✓ | ✓ | ✓ | — | — | — | — | — | — | — |
| Exec Signal / Deal Alert | — | — | — | — | — | ✓ | — | ✓ | ✓ | — | — | — | — | — | — | — |
| Site Intelligence | — | ✓ | ✓ | ✓ | ✓ | — | — | — | — | — | — | — | ✓ | ✓ | ✓ | — |
| Portfolio Monitoring | ✓ | ✓ | — | ✓ | — | — | — | ✓ | — | — | — | — | ✓ | ✓ | — | — |
| Healthcare Vertical | — | ○ | — | — | ✓ | — | — | — | — | ✓ | ✓ | — | — | ✓ | — | — |
| Roll-up Market Score | — | ✓ | ✓ | — | ✓ | — | — | — | — | ✓ | ✓ | — | — | — | ✓ | — |

✓ = currently wired in, ○ = available but not yet wired in, — = not applicable

---

## Part 2 — Where Public Data Has Hard Ceilings

Before applying synthetic data, we need to understand exactly where public data stops and why:

| Data Gap | Why Public Data Stops | Scale of Gap |
|----------|----------------------|--------------|
| **Private company financials** | EDGAR is public companies only; Form D captures offering amounts but not P&L | ~95% of PE targets are private |
| **PE fund performance** | IRR, TVPI, DPI not reported publicly; only LP-to-LP via FOIA requests | ~100% of closed-end funds |
| **LP commitment history** | Only 990-filers (endowments) + CAFR-publishers (pensions) + 13F (equity) | ~60% of institutional LP universe |
| **Executive compensation at privates** | Only DEF 14A for public companies | ~95% of PE targets |
| **Job posting data for SMBs** | Greenhouse/Lever cover ~30% of companies; 70% of companies post elsewhere or not at all | ~70% of hiring activity invisible |
| **Org chart depth below VP** | Only C-suite on websites; middle management not published | ~80% of org chart missing |
| **Foot traffic at secondary sites** | SafeGraph/Placer coverage thin outside top 100 metros | Varies by market |
| **Rural site infrastructure data** | EIA/FCC coverage sparse in rural counties | ~20% of counties |
| **Healthcare practice revenue** | CMS covers Medicare (30-40% of revenue); no total revenue for private practices | ~60-70% of revenue imputed |
| **M&A deal terms (private)** | Press releases partial; no comprehensive private deal database | Deal terms for ~95% of PE deals |
| **CFTC + EIA price → industry-level margin** | Public data gives macro commodity prices but not firm-level margin impact | Firm-level cost structure |

---

## Part 3 — Synthetic Data Research Landscape

*Deep literature review covering 2019–2025. Each section: top papers with full citations, open-source implementations, and specific Nexdata application.*

---

### 3.1 Tabular Synthetic Data (Business Records)

**The Core Problem:** 95% of PE targets are private companies with no EDGAR filings. We need to generate statistically valid financial, operational, and behavioral data distributions for private companies conditioned on public company peers.

#### Top Papers

**CTGAN/TVAE — "Modeling Tabular data using Conditional GAN"**
- Lei Xu, Maria Skoularidou, Alfredo Cuesta-Infante, Kalyan Veeramachaneni. **NeurIPS 2019**
- Mode-specific normalization via Bayesian Gaussian Mixture Models handles multimodal continuous columns. Conditional generator with training-by-sampling addresses class imbalance — critical for rare event records (OSHA violations, Form D small raises). TVAE variant uses VAE preprocessing pipeline; lower mode-collapse risk, better marginal distributions for heavy-tailed financial data.
- GitHub: [sdv-dev/CTGAN](https://github.com/sdv-dev/CTGAN) — ~1,900 stars

**TabDDPM — "Modelling Tabular Data with Diffusion Models"**
- A. Kotelnikov, D. Baranchuk, I. Rubachev, A. Babenko (Yandex Research). **ICML 2023**
- Applies Gaussian diffusion to continuous features and multinomial diffusion to categoricals simultaneously. Demonstrated clear superiority over CTGAN/TVAE across 15 benchmark datasets. Supports DP noise injection at training time for privacy-preserving synthesis. **Currently best-performing method for pure tabular generation.**
- GitHub: [yandex-research/tab-ddpm](https://github.com/yandex-research/tab-ddpm)

**GReaT — "Language Models are Realistic Tabular Data Generators"**
- Vadim Borisov, Kathrin Sessler, Tobias Leemann, Martin Pawelczyk, Gjergji Kasneci (TU Munich). **ICLR 2023**
- Serializes each row as a natural language sentence, fine-tunes GPT-2, then samples new rows via text generation. Permutes column order during training. Enables conditional sampling by prompting: "The company NAICS is 5413, revenue bucket is $10-50M, generate..." — critical for private company financial synthesis. Supports logical constraints via guided decoding.
- GitHub: [tabularis-ai/be_great](https://github.com/tabularis-ai/be_great)

**REaLTabFormer — "Generating Realistic Relational and Tabular Data using Transformers"**
- Aivin V. Solatorio, Olivier Dupriez (World Bank). **arXiv 2302.02041, 2023**
- First transformer-native approach for multi-table relational data. GPT-2 for parent tables; Seq2Seq transformer conditioned on parent rows for child table generation. Preserves referential integrity and cross-table statistical dependencies. **Directly applicable to fund→portfolio→deal table relationships** where CTGAN on each table independently breaks FK integrity.
- GitHub: [worldbank/REaLTabFormer](https://github.com/worldbank/REaLTabFormer)

**Tabula — LLM-based generation with larger models (2023)**
- Uses LLaMA-2 (vs GPT-2 in GReaT) for tabular generation. Better on complex distributions with text fields (deal rationale, company descriptions alongside numeric financials). Few-shot prompting enables generation without fine-tuning for sparse target domains.

#### Open-Source Tools
- **SDV (Synthetic Data Vault)** — [sdv-dev/sdv](https://github.com/sdv-dev/sdv) — 2,200+ stars; unified Python API wrapping CTGAN, CopulaGAN, GaussianCopula, and HMA for multi-table synthesis
- **Synthcity** (van der Schaar Lab) — [vanderschaarlab/synthcity](https://github.com/vanderschaarlab/synthcity) — benchmark including CTGAN, TVAE, TabDDPM, GReaT, and DP variants under one API
- **Ydata-Synthetic** — [ydataai/ydata-synthetic](https://github.com/ydataai/ydata-synthetic) — 1,300 stars; production-grade wrapper for pandas workflows

#### Commercial Services
- **MOSTLY AI** — highest fidelity among commercial tools (97.8% ML accuracy vs SDV 52.7% in head-to-head benchmark); free tier; strong for financial data
- **Gretel.ai** — ACTGAN model (improved CTGAN), DP built in; $0.003/1k rows

#### Nexdata Application
1. Train TabDDPM on `public_company_financials` (EDGAR XBRL table) — 50K public companies × 10 years — condition on NAICS-4 + revenue_bucket + state → synthesize private company P&L/balance sheet distributions
2. REaLTabFormer for multi-table synthesis: generate synthetic fund→portfolio→deal chains where Form D provides parent fund data and the child portfolio companies need plausible financials
3. GReaT for executive profile synthesis: generate realistic title progressions + company histories for org chart gap-filling (impossible with pure tabular models)

---

### 3.2 Financial Time Series Synthesis

**The Core Problem:** The last 12 years of near-zero rates makes FRED historical data structurally inadequate for stress testing PE portfolio scenarios. Synthetic macro paths allow 1,000-scenario Monte Carlo over distributions that include rate shock, stagflation, and structural break scenarios undersampled in the 2010–2022 era.

#### Top Papers

**TimeGAN — "Time-series Generative Adversarial Networks"**
- Jinsung Yoon, Daniel Jarrett, Mihaela van der Schaar. **NeurIPS 2019** — canonical baseline through 2025
- Joint training with supervised loss forces the model to respect stepwise temporal dynamics (the "stepping" loss). Embedding/recovery architecture creates a latent space that preserves non-stationarity. Handles correlated multi-variate series. The benchmark all later methods compare against.
- GitHub: [jsyoon0823/TimeGAN](https://github.com/jsyoon0823/TimeGAN) — ~1,600 stars

**Diffusion-TS — "Interpretable Diffusion for General Time Series Generation"**
- Xinyu Yuan, Yan Qiao. **ICLR 2024**
- Combines seasonal-trend decomposition (STL-style) with a denoising diffusion Transformer backbone. Trains to reconstruct the sample directly (not the noise), with a Fourier-based auxiliary loss for frequency-domain fidelity. Handles conditional (forecasting, imputation) and unconditional generation without architecture changes. State-of-the-art on ETT, Stock, MuJoCo benchmarks. **Best current method for correlated macro series generation.**
- GitHub: [Y-debug-sys/Diffusion-TS](https://github.com/Y-debug-sys/Diffusion-TS)
- arXiv: [2403.01742](https://arxiv.org/abs/2403.01742)

**TSGM — "A Flexible Framework for Generative Modeling of Synthetic Time Series"**
- Alexander Nikitin et al. **NeurIPS 2024 (arXiv 2305.11567)**
- Modular framework wrapping TimeGAN, RCGAN, DoppelGANger, VAE models, and diffusion under a unified API. Includes evaluation metrics: discriminative score, predictive score, autocorrelation preservation. The "SDV for time series" — use as the research harness, plug in Diffusion-TS as the backend generator.
- GitHub: [AlexanderVNikitin/tsgm](https://github.com/AlexanderVNikitin/tsgm)

**FinDiff — "Diffusion Models for Financial Tabular Data Generation"**
- Sattarov et al. **ACM ICAIF 2023** (AI in Finance)
- Adapts DDPM for mixed-type financial tabular data (categorical + continuous). Embeds categoricals into dense vectors before diffusion. Label conditioning enables conditional generation (e.g., "generate deal records from 2018 vintage, industrial sector"). Outperforms CTGAN and TVAE on three real-world financial datasets across fidelity, privacy, and ML utility.
- GitHub: [sattarov/FinDiff](https://github.com/sattarov/FinDiff)
- arXiv: [2309.01472](https://arxiv.org/abs/2309.01472)

**"Generation of Synthetic Financial Time Series by Diffusion Models"**
- Quantitative Finance (Taylor & Francis, 2025; arXiv 2410.18897)
- Applies score-based diffusion to reproduce stylized facts: fat-tailed return distributions, volatility clustering, slow ACF decay, U-shaped intraday patterns. Benchmarks against TimeGAN and QuantGAN on equity and alternative return series. The stylized facts preservation methodology applies directly to generating synthetic PE return distributions from BLS/FRED macro history.

**FM-TS — "Flow Matching for Time Series Generation"**
- arXiv 2411.07506 (2024)
- Applies continuous normalizing flows (flow matching) as alternative to DDPM for time series. Straighter probability paths, faster sampling (fewer NFE), better training stability. Faster than Diffusion-TS at inference — useful when generating 1,000-scenario stress test batches in real time.

#### Open-Source Tools
- **TSGM** — framework harness; use with Diffusion-TS backend
- **Ydata-Synthetic** — wraps TimeGAN with production pandas API
- **Gretel.ai DGAN** — DoppelGANger variant, API-accessible; designed explicitly for time series

#### Nexdata Application
1. Train Diffusion-TS via TSGM on 60-year FRED monthly history (8 correlated series: DFF, DGS10, DGS2, UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO). Generate 1,000 synthetic macro paths → feed into DealEnvironmentScorer as stress-test overlay
2. FinDiff for mixed-type deal data: each "deal record" has static features (sector, vintage, leverage) plus quarterly performance time series — FinDiff handles this joint generation natively
3. Gaussian Copula baseline (quick start): fit Clayton/Frank copulas on FRED residuals to preserve rank correlations without deep learning infrastructure — deploy first, upgrade to Diffusion-TS later

---

### 3.3 Knowledge Graph Completion / Entity Linking

**The Core Problem:** The LP→GP→Portfolio Company→Executive graph in Nexdata is 40–60% complete. Many LP commitments go undisclosed; portfolio company lists are partial; board memberships lag. KGC imputes missing edges using the structural patterns of the known graph.

#### Top Papers

**RotatE — "Knowledge Graph Embedding by Relational Rotation in Complex Space"**
- Zhiqing Sun, Zhi-Hong Deng, Jian-Yun Nie, Jian Tang. **ICLR 2019** — dominant embedding method through 2025
- Models each relation as a rotation in complex vector space (‖h ∘ r − t‖). Handles symmetric, antisymmetric, inverse, and compositional relation patterns in one framework. Consistently outperforms TransE/DistMult on FB15k-237 and WN18RR. **Go-to choice for link prediction in company relationship graphs.**

**CompanyKG — "A Large-Scale Heterogeneous Graph for Company Similarity Quantification"**
- EQT Motherbrain Team (Carosia et al.). **ACM SIGKDD 2024**
- 1.17M company nodes with description embeddings + 51M edges across 15 relation types: investor→portfolio, competitor, supplier, acquirer, co-investor. Three benchmark tasks: similarity prediction, competitor retrieval, similarity ranking. Benchmarks 11 methods (node-only, edge-only, node+edge). **The only large-scale KG dataset built explicitly for the PE/investment domain — use as transfer-learning source for Nexdata's company graph.**
- GitHub: [EQTPartners/CompanyKG](https://github.com/EQTPartners/CompanyKG)
- arXiv: [2306.10649](https://arxiv.org/abs/2306.10649)

**KG-FIT — "Knowledge Graph Fine-Tuning Upon Open-World Knowledge"**
- Pengcheng Jiang et al. **NeurIPS 2024**
- Fine-tunes pre-trained KG embeddings (RotatE, TransE) using LLM-generated textual descriptions of entities, adapting to open-world entities not seen at training time. Directly applicable when Nexdata adds new funds or LPs that weren't in the original RotatE training set — the LLM description of the new entity bootstraps its embedding.

**KLR-KGC — "Knowledge-Guided LLM Reasoning for Knowledge Graph Completion"**
- Electronics 2024
- Hybrid: KG embedding provides structural priors; LLM provides semantic reasoning. LLM is prompted with neighborhood subgraphs and asked to predict missing links. Outperforms KGE-only and LLM-only on NELL-995 and FB15k-237. **The practical production architecture: RotatE for structural signal + Claude for semantic scoring of candidate edges.**

**ULTRA — Foundation Model for KG Reasoning**
- Galkin et al. 2024
- Transferable KG reasoning without source-specific training. Zero-shot link prediction on new entity types. Useful for extending to new sectors or LP types without full retraining.

#### Open-Source Tools
- **PyKEEN** — [pykeen/pykeen](https://github.com/pykeen/pykeen) — 1,500 stars; 40+ KGE models with unified training API; best starting point
- **DGL-KE** — GPU-accelerated KGE training; handles billion-edge graphs if Nexdata graph scales
- **CompanyKG** — dataset + baselines for transfer learning

#### Nexdata Application
1. Build entity graph: nodes = {LP, GP Firm, Fund, Portfolio Company, Executive}; edges = {committed_to, manages, portfolio_of, employed_at, board_member_of, co_invested_with}; source Form D, 990, 13F, ADV, People collection
2. Train RotatE on known triples; predict top-K missing LP→Fund commitment edges with confidence scores
3. Transfer-learn from CompanyKG (same 15 relation types) to bootstrap before Nexdata has enough native training triples
4. Use KLR-KGC hybrid: RotatE shortlists candidates; Claude scores each ("Does LP strategy description match GP investment focus?")

---

### 3.4 Missing Data Imputation (Cross-Source)

**The Core Problem:** Any given company appears in some but not all of the 47 sources. For a complete company diligence composite, every feature must be present — imputation from available signals is more reliable than nulls.

#### Top Papers

**GRAPE — "Handling Missing Data with Graph Representation Learning"**
- Jiaxuan You, Xiaobai Ma, Ailing Yi, Jure Leskovec (Stanford). **NeurIPS 2020** — still dominant through 2025
- Represents a dataset as a bipartite graph (observations × features) with observed values as edge attributes. Feature imputation = edge-level prediction; GNN propagation enables imputation respecting inter-feature correlation globally (not column-by-column). 20% lower MAE than best non-graph baselines on heterogeneous datasets.
- Paper: [cs.stanford.edu/people/jure/pubs/grape-neurips20.pdf](https://cs.stanford.edu/people/jure/pubs/grape-neurips20.pdf)

**HyperImpute — "Generalized Iterative Imputation with Automatic Model Selection"**
- Daniel Jarrett, Bogdan Cebere, Tennison Liu, Alicia Curth, Mihaela van der Schaar. **ICML 2022**
- Extends MICE with automatic model selection per column — tests XGBoost, neural networks, linear models, MissForest per column and selects based on held-out performance. Adapts dynamically as imputation converges. **Best choice for heterogeneous company tables where different columns have MCAR, MAR, and MNAR mechanisms simultaneously.**
- GitHub: [vanderschaarlab/hyperimpute](https://github.com/vanderschaarlab/hyperimpute)

**MIWAE — "Deep Generative Modelling and Imputation of Incomplete Data Sets"**
- Pierre-Alexandre Mattei, Jes Frellsen. **ICML 2019**
- Trains a deep latent variable model (VAE-style) on incomplete data using the MIWAE bound (importance-weighted ELBO). Produces both point estimates and full posterior distributions over missing values. Key advantage: uncertainty-quantified imputation — instead of a point estimate for imputed revenue, returns a posterior distribution. **Use when confidence intervals on imputed values matter** (e.g., LP AUM where 990 data is sparse).

**GAIN — "Generative Adversarial Imputation Nets"**
- Jinsung Yoon, James Jordon, Mihaela van der Schaar. **ICML 2018**
- GAN discriminator learns to distinguish observed vs. imputed values; generator learns to produce imputations that fool the discriminator. Performs well under MCAR and MAR mechanisms. The "hint mechanism" guides the generator with partial missingness information. Good for OSHA/EPA compliance tables where missingness is random.

**IVGAE — "Handling Incomplete Heterogeneous Data with a Variational Graph Autoencoder"**
- arXiv 2511.22116 (2025)
- Extends GRAPE to heterogeneous entity graphs where nodes are companies/people and edges are relationships. Jointly imputes missing node features and missing edges. **Most directly relevant to Nexdata's multi-source company graph** — simultaneously imputes missing company revenue AND missing portfolio company → fund relationships.

**TabImpute — Foundation Model Imputation**
- arXiv 2510.02625 (2024)
- Zero-shot imputation using a pre-trained foundation model; no per-dataset fine-tuning. In-context learning: prompt with semantically similar complete rows to impute missing values. Useful for quick imputation in new data domains before training data accumulates.

#### Open-Source Tools
- **HyperImpute** — [vanderschaarlab/hyperimpute](https://github.com/vanderschaarlab/hyperimpute) — pip installable, best maintained, recommended first choice
- **Synthcity** — includes GAIN, MIWAE, HyperImpute under unified API
- **Datawig** (Amazon) — deep learning imputation library; handles text + numeric mixed columns
- **MissForest** — simple tree-based baseline; R (missForest) + Python (missingpy)

#### Nexdata Application
1. **HyperImpute** as the default imputation layer: run on the company diligence composite table where each source covers different companies with different fields missing — auto-selects best model per column
2. **GRAPE** for graph-structured imputation: represent {company, feature} bipartite graph across all sources; impute private company revenue using graph neighborhood of same-NAICS public company peers
3. **IVGAE** as the advanced layer: jointly impute missing node features and missing LP→GP edges in the PE relationship graph simultaneously
4. **MIWAE** specifically for LP AUM and Form 990 endowment data: return posterior distributions (e.g., "LP endowment is $850M ± $200M at 90% CI") rather than point estimates — critical for LP conviction scoring confidence intervals

---

### 3.5 Geographic / Location Data Augmentation

**The Core Problem:** Foot traffic coverage is thin outside top-100 metros. EIA utility rate data and FCC broadband speeds have county-level gaps in rural markets. Site intelligence scores are NULL for ~20% of counties.

#### Top Papers

**"Generative Models for Synthetic Urban Mobility Data: A Systematic Literature Review"**
- ACM Computing Surveys 2024 (doi: 10.1145/3610224)
- Covers 50+ methods across GAN-based, VAE-based, diffusion-based, and agent-based simulation for urban mobility. Defines evaluation taxonomy: spatial fidelity (radius of gyration, visit frequency), temporal fidelity (OD matrix reconstruction), and privacy metrics (re-identification risk). **Essential reference for selecting the right method** — the review's conclusion: ST-TrajGAN + temporal Gaussian process is the best privacy-utility trade-off for business visit data.

**ST-TrajGAN — "Semantic and Transformer-based Trajectory GAN"**
- Future Generation Computer Systems (Elsevier, 2024)
- Encodes spatio-temporal + semantic features (visit category, dwell time) before GAN training. Evaluated on Trajectory-User Linking (TUL) to confirm synthetic trajectories cannot be re-linked to individuals. Strong privacy-utility trade-off for business visit data at secondary market locations.

**GeoGen — "A Two-stage Coarse-to-Fine Framework for Fine-grained Trajectory Generation"**
- arXiv 2510.07735 (October 2025)
- Two-stage: generates visit locations at district granularity first, then refines to POI-level. Uses LLM priors for semantic coherence ("a hospital worker visits pharmacies, not gyms"). **Most current approach** — highly relevant for generating synthetic business visit patterns for Nexdata's site scoring model.

**LSTM-TrajGAN**
- Rao, Gao et al. (GeoDS Lab, UW-Madison)
- End-to-end LSTM-GAN for trajectory generation with privacy preservation. Lacks formal DP guarantees but good utility on downstream mobility tasks. Well-documented implementation.
- GitHub: [GeoDS/LSTM-TrajGAN](https://github.com/GeoDS/LSTM-TrajGAN)

#### Open-Source Tools
- **LSTM-TrajGAN** — [GeoDS/LSTM-TrajGAN](https://github.com/GeoDS/LSTM-TrajGAN) — best documented spatial GAN for business locations
- **Scikit-mobility** — Python library for mobility data analysis and synthetic generation
- **Scikit-gstat / gstools** — Gaussian Process kriging for spatial interpolation of EIA/FCC coverage gaps

#### Nexdata Application
1. **Kriging (Gaussian Process)** for site intelligence gap-filling: interpolate FCC broadband speeds + EIA utility rates for uncovered counties using distance-to-nearest-measured-point + Census demographics as conditioning features. Immediately closes the 20% NULL county rate in site scores.
2. **ST-TrajGAN / GeoGen** for foot traffic simulation at Yelp locations outside SafeGraph coverage: condition on location demographics (Census ACS) + business category + Yelp rating/review count → generate plausible visit pattern distributions. Enables foot traffic signals for secondary market acquisition targets.
3. **Gravity model baseline** (quick start, no ML): visits ~ (Census population within 5 miles × Yelp attractiveness score) / distance² — deploy first, upgrade to ST-TrajGAN once Placer.ai data provides training signal.

---

### 3.6 Graph Synthesis (Org Charts, Board Interlock Networks)

**The Core Problem:** Org chart collection captures C-suite only (~20% of org chart depth). Board interlock network (which executives sit on multiple PE portfolio company boards) is only ~30% complete. Both have high value for deal sourcing and LP intelligence.

#### Top Papers

**ERGM — Exponential Random Graph Models**
- Canonical reference: statnet package (Handcock, Hunter, Butts, Goodreau, Morris)
- Models probability of network structure as function of local statistics (edges, triangles, degree distribution, homophily). Generative: fit ERGM to observed board interlock networks, then MCMC-sample new networks with the same statistical properties (degree sequence, transitivity, tenure patterns).
- R package: [statnet.org](https://statnet.org); Python: `graph-tool`, `pyERGM`

**"Director Interlocks: Information Transfer in Board Networks"**
- MDPI 2024
- Formalizes board interlock network metrics and empirical distributions (degree, clustering coefficient, path length) that a generative model must reproduce for synthetic networks to be realistic. Provides the evaluation targets for ERGM/GNN board network synthesis.

**CAPER — "Enhancing Career Trajectory Prediction using Temporal Knowledge Graph"**
- Bigdasgit team. **ACM SIGKDD 2025**
- Models career trajectories as a temporal KG with ternary relationships (person, company, position, time). 6.8% better next-employer prediction, 34.6% better next-title prediction vs. baselines. Sampling from the learned distribution generates synthetic executive career trajectories with realistic tenure patterns, role progressions, and industry transitions.
- GitHub: [Bigdasgit/CAPER](https://github.com/Bigdasgit/CAPER)
- arXiv: [2408.15620](https://arxiv.org/abs/2408.15620)

**"Unmasking Fake Careers: Detecting Machine-Generated Career Trajectories via Multi-layer Heterogeneous Graphs"**
- CareerScape. arXiv 2509.19677 (2025)
- Builds an HetGNN to detect LLM-generated vs. real career trajectories. Identifies what makes synthetic careers detectable: uniform role progression, missing tenure gaps, implausible company combinations. **Quality rubric for Nexdata's synthetic org chart generation** — use CareerScape as a discriminator to filter low-quality synthetic career paths.

#### Open-Source Tools
- **STATNET** — R; best ERGM fitting and MCMC simulation for board networks
- **NetworkX + BA/SBM generators** — Barabási-Albert (preferential attachment) + Stochastic Block Model for org chart DAGs
- **DGL / PyTorch Geometric** — GNN-based heterogeneous graph generation
- **graph-tool** (Python) — efficient MCMC ERGM with variational inference

#### Nexdata Application
1. **CAPER** for org chart completion: train on known (person, company, title, date) triples from `company_people` + SEC DEF 14A; sample synthetic career paths for executives where only partial history is known — complete missing tenure gaps and title progressions
2. **ERGM** for board interlock synthesis: fit to observed PE portfolio company board networks; generate synthetic interlock networks to expand training data for "board influence" scoring; validate synthetic networks against CareerScape discriminator
3. **GNN link prediction** for board completion: train edge predictor on known board memberships across PE-backed companies → predict which other portfolio companies a given executive likely serves on

---

### 3.7 PE Deal Valuation Synthesis

**The Core Problem:** LBO model training needs comparable deal data. Private deal comps are sparse, NDA-protected, and unevenly distributed across vintage years and sectors.

#### Top Papers

**CFA Institute — "Synthetic Data in Investment Management"**
- CFA Institute Research and Policy Center (Tait et al.). **July 2025**
- Comprehensive practitioner report covering VAEs, GANs, diffusion models, and LLMs for investment management. Key finding: Monte Carlo is better for tractable parametric distributions; generative models outperform when the distribution is complex and multi-modal (e.g., EBITDA multiple distributions across vintages/sectors — which are both). Includes case study fine-tuning an LLM for financial sentiment using synthetic data.
- GitHub: [CFA-Institute-RPC/Synthetic-Data-For-Finance](https://github.com/CFA-Institute-RPC/Synthetic-Data-For-Finance)

**TabDDPM** (cited in §3.1)
- Directly applicable: generate synthetic deal-level records (entry multiple, exit multiple, hold period, IRR, MOIC) conditioned on sector/vintage/geography using TabDDPM's categorical conditioning mechanism.

**FinDiff** (cited in §3.2)
- Label conditioning allows "generate synthetic buyout records from 2018 vintage, industrial sector." Mixed tabular+temporal architecture handles both static deal metadata and quarterly performance time series in a single model.

**"Generation of Synthetic Financial Time Series by Diffusion Models"**
- Quantitative Finance 2025 (arXiv 2410.18897)
- Demonstrates that stylized facts (fat tails, volatility clustering) must be explicitly preserved in PE return distribution synthesis — not just marginal distributions. The Fourier-space evaluation methodology is the correct validation approach for synthetic IRR/MOIC distributions.

**Gaussian Copula over PE Benchmarks (Applied Practice)**
- Standard applied approach at major hedge funds: fit Gaussian copula over (entry EV/EBITDA, leverage ratio, exit multiple, revenue CAGR, hold period) using Cambridge Associates public benchmark data → Monte Carlo samples generate correlated deal scenarios. Produces 50K synthetic deal records from 500 real observations while preserving cross-variable correlations.

#### Nexdata Application
1. Fit Gaussian copula on Nexdata's PE benchmark data (from `pe_benchmarks` endpoint) + Cambridge Associates public summary data → generate 50K synthetic deal training records
2. Use TabDDPM conditioned on (sector, vintage, strategy) to generate deal comps dataset with realistic joint distribution of entry multiple × leverage × exit multiple × hold period
3. GReaT for synthetic deal memo text: given a real deal's sector + revenue + growth + leverage, generate synthetic deal rationale narrative — training data for NLP deal classifier

---

### 3.8 Job Posting Intelligence Augmentation

**The Core Problem:** ~70% of companies (especially SMBs and portfolio companies) don't use tracked ATS platforms (Greenhouse, Lever). This makes hiring signal invisible for most PE acquisition targets.

#### Top Papers

**JobGen/JobSet — "Synthetic Job Advertisements Dataset for Labour Market Intelligence"**
- Colombo, D'Amico, Malandri, Mercorio, Seveso (University of Milano-Bicocca). **ACM SAC 2025**
- Fine-tunes LLMs on 2M real job ads from EUROSTAT Web Intelligence Hub using ESCO taxonomy as conditioning schema. Generates semantically aligned, diverse synthetic job ads. Released **JobSet** as an open dataset. Achieves 10+ point RP@10 improvement in skill extraction when training on LLM-generated synthetic data vs. distant supervision.
- GitHub: [Crisp-Unimib/JobGen](https://github.com/Crisp-Unimib/JobGen)
- ACM: [doi.org/10.1145/3672608.3707718](https://dl.acm.org/doi/10.1145/3672608.3707718)

**"A Framework for Generating Synthetic Job Postings"**
- ACL NLP4HR Workshop 2024 (doi: 10.18653/v1/2024.nlp4hr-1.4)
- Systematic evaluation of LLM prompting strategies for generating job postings that preserve salary distributions, skill co-occurrence patterns, and industry-specific vocabulary. Constrained decoding with ESCO ontology ensures generated skills are valid taxonomy nodes. **The methodological playbook for Nexdata's job posting augmentation pipeline.**

**CAPER** (cited in §3.6)
- Temporal KG of (person, company, position, time) can generate synthetic executive career trajectories → infer hiring patterns from trajectory distributions rather than real-time job postings.

**LLM4Jobs — "Unsupervised Occupation Extraction and Standardization Leveraging LLMs"**
- Knowledge-Based Systems 2025
- Extracts ESCO occupation codes from unstructured job text without labeled training data. Generates synthetic labeled examples via LLMs to bootstrap supervised classifiers. Applicable to standardizing Nexdata's `job_postings.title_normalized` column across ATS platforms.

#### Open-Source Tools
- **JobGen** — [Crisp-Unimib/JobGen](https://github.com/Crisp-Unimib/JobGen) — best documented LLM job posting generator
- **ESCO API** — [ec.europa.eu/esco/api](https://ec.europa.eu/esco/api) — free; 3,007 occupations, 13,896 skills
- **HuggingFace ESCO extractors** — pre-trained models for ESCO code classification

#### Nexdata Application
1. For companies identified from Yelp/NPPES/Form D that don't appear in Greenhouse/Lever: generate synthetic job posting signals using JobGen conditioned on (NAICS + company_size_bucket + region + growth_stage from job count trend). Signals extracted: "is this company hiring VPs? CFO? M&A counsel?"
2. Use LLM4Jobs to normalize `job_postings.title_normalized` — standardize titles across Greenhouse, Lever, SmartRecruiters into ESCO occupation codes for cross-company comparison
3. Use CAPER's temporal KG to generate synthetic career trajectories → infer expected hiring patterns for companies in growth/transition stages without waiting for real postings to appear

---

### 3.9 Regulatory / Compliance Record Synthesis

**The Core Problem:** OSHA/EPA inspection records are sparse for niche sub-industries (e.g., cold storage warehouses — only 50 real records for a given NAICS bucket). Training compliance risk scoring models requires augmentation to avoid overfitting.

#### Top Papers

**Private Evolution (DPSDA) — "Differentially Private Synthetic Data via Foundation Model APIs"**
- Zinan Lin, Sivakanth Gopi et al. (Microsoft Research) + Chulin Xie et al. **ICLR 2024 + ICML 2024 Spotlight**
- Generates DP synthetic data without training a generator — uses a foundation model's API in a privacy-preserving evolutionary loop. Private Selection + Exponential Mechanism selects which synthetic examples match the real distribution while spending privacy budget. Achieves ε=0.67 on CIFAR10, far better than DP-SGD approaches. **Applicable to generating synthetic OSHA/EPA inspection records that can be publicly shared** without company re-identification risk.
- GitHub: [microsoft/DPSDA](https://github.com/microsoft/DPSDA)

**Federal CDO Council RFI on Synthetic Data (Federal Register, January 2024)**
- US government formal framework authorizing agencies (including OSHA, EPA) to release DP-synthetic inspection database versions. EPA ECHO is explicitly targeted. **This regulatory clearance makes the compliance synthesis use case viable for Nexdata** — the output is authorized for release.

**"Does Differentially Private Synthetic Data Lead to Synthetic Discoveries?"**
- PubMed 2024. Critical finding: DP synthetic data can introduce spurious statistical patterns ("synthetic discoveries"). Risk for Nexdata: false compliance risk signals in the scoring model. **Mitigation:** validate synthetic records against held-out real inspections before using for model training; flag all synthetic-augmented model outputs with wider confidence intervals.

**HyperImpute** (cited in §3.4) — applicable to filling missing severity scores, establishment size, and outcome fields in OSHA records before augmentation.

#### Open-Source Tools
- **SmartNoise SDK** (OpenDP / Microsoft) — [opendp/smartnoise-sdk](https://github.com/opendp/smartnoise-sdk) — DP-CTGAN, DP-GAN, DP marginal synthesizers; US government-grade
- **OpenDP Library** — [opendp.org](https://opendp.org) — foundational DP primitives; used by US Census Bureau for 2020 disclosure avoidance
- **DPSDA** — [microsoft/DPSDA](https://github.com/microsoft/DPSDA)

#### Nexdata Application
1. Train SmartNoise DP-CTGAN on OSHA violation records (synthetic inspection data for niche NAICS categories with <100 real records) → expand to 5K synthetic training examples for compliance risk scoring model
2. For EPA ECHO: generate DP-synthetic facility violation records while preserving sector-level violation rate distributions — use to augment company health score training data
3. Apply HyperImpute first to fill structural missingness in real OSHA records before DP synthesis pass — better base distribution → higher quality synthetic output

---

## Part 4 — Priority Matrix: Synthetic Data Extensions

Ranked by (Impact × Feasibility × Research Maturity):

| # | Extension | Impact | Feasibility | Research Maturity | Score | First Source |
|---|-----------|--------|------------|------------------|-------|--------------|
| 1 | **Private company financials** (CTGAN/GRAPE on EDGAR XBRL → privates) | 10 | 8 | 9 | **720** | EDGAR XBRL → Form D + SAM.gov anchors |
| 2 | **Macro scenario generation** (Diffusion-TS/TimeGAN on FRED history) | 9 | 9 | 8 | **648** | FRED 60-year monthly series |
| 3 | **LP data imputation** (KGC on LP→GP graph) | 8 | 7 | 7 | **392** | Form D + 990 + 13F known edges |
| 4 | **Org chart completion** (GNN link prediction on people graph) | 8 | 7 | 7 | **392** | People collection + SEC DEF14A |
| 5 | **Healthcare revenue estimation** (MIWAE on CMS utilization) | 8 | 8 | 6 | **384** | NPPES + CMS utilization |
| 6 | **Deal valuation synthesis** (TabDDPM on EDGAR multiples) | 9 | 6 | 7 | **378** | EDGAR EV/EBITDA + press release deals |
| 7 | **Job posting augmentation** (LLM on ESCO + BLS OES) | 7 | 8 | 6 | **336** | BLS OES + existing job_postings |
| 8 | **Geographic imputation** (Kriging on EIA/FCC/NOAA) | 6 | 9 | 9 | **486** | EIA + FCC + Census (well-observed) |
| 9 | **PE performance modeling** (Hierarchical Bayes on ADV + vintage) | 7 | 5 | 6 | **210** | ADV AUM + Cambridge Associates benchmarks |
| 10 | **Foot traffic simulation** (Gravity model + DeepMove) | 6 | 6 | 6 | **216** | Yelp + Census demographics |

---

## Part 5 — Architecture: How Synthetic Data Plugs In

Synthetic data is not a replacement for ingested data — it is a **gap-filling layer** that sits between the raw ingestion pipeline and the intelligence products.

```
┌─────────────────────────────────────────────────────────────┐
│                    LAYER 1: INGESTED DATA                   │
│  47 public sources → 60+ tables (current state)            │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                LAYER 2: ENRICHMENT CHAINS                   │
│  Signal chains 1–8: cross-source joins + scoring models    │
└─────────────────────────┬───────────────────────────────────┘
                          │ Where chains fail due to missing data:
                          ▼
┌─────────────────────────────────────────────────────────────┐
│             LAYER 3: SYNTHETIC DATA LAYER (NEW)            │
│  A. Financial imputation (GRAPE/CTGAN for private cos)      │
│  B. Time series scenarios (Diffusion-TS for FRED paths)     │
│  C. Graph completion (KGC for LP/GP/portfolio edges)        │
│  D. Tabular augmentation (TabDDPM for deal comps)           │
│  E. Geographic interpolation (Kriging for site data)        │
│  F. Org chart completion (GraphRNN for missing mgmt)        │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│               LAYER 4: INTELLIGENCE PRODUCTS                │
│  Deal scores, company diligence, LP conviction, site scores │
│  All products now benefit from synthetic gap-filling        │
└─────────────────────────────────────────────────────────────┘
```

**Key principle:** Synthetic data is always:
1. Labeled as synthetic (never presented as real data)
2. Used only to augment model training or fill score gaps — not shown raw to users
3. Confidence-calibrated (products derived from synthetic data show wider confidence intervals)
4. Overridden immediately when real data arrives

---

## Part 6 — Implementation Phases

### Phase A — Foundation (Weeks 1–4)
**Goal:** Private company financials imputation + macro scenario generation

**A1: EDGAR XBRL → Private Company Financial Imputor**
- Build CTGAN + GRAPE pipeline training on `public_company_financials` (XBRL table)
- Condition model on: NAICS-4, revenue_bucket, headcount_bucket, state, vintage_year
- Output: `private_company_financials_synthetic` table
- Training data: ~50K public companies × 10 years of quarterly data
- Files new: `app/services/synthetic/financial_imputation.py`

**A2: FRED Macro Scenario Engine**
- Build Diffusion-TS / Gaussian Copula pipeline on 60-year FRED monthly history
- 8 correlated series: DFF, DGS10, DGS2, UNRATE, CPIAUCSL, UMCSENT, INDPRO, DCOILWTICO
- Output: `macro_scenarios` table (1,000 paths × N months × 8 series)
- Use in: DealEnvironmentScorer for stress-test overlay on current scores
- Files new: `app/services/synthetic/macro_scenarios.py`

### Phase B — Graph Completion (Weeks 5–8)
**Goal:** LP→GP→Portfolio graph completion + org chart imputation

**B1: LP→GP KGC Model**
- Entity graph from: lps + pe_firms + pe_funds + lp_holdings
- Train TransE/RotatE embeddings using `pykeen`
- Task: Predict missing LP→GP commitment relationships
- Output: `lp_gp_predicted_relationships` table with confidence scores
- Files new: `app/services/synthetic/lp_gp_graph_completion.py`

**B2: Org Chart Completion**
- Train GNN link predictor on known org chart edges (people_collection)
- Condition on: company_size, industry, known_executives, headcount estimate
- Output: `org_chart_completions` table with inferred reporting relationships
- Files new: `app/services/synthetic/org_chart_completion.py`

### Phase C — Domain-Specific Augmentation (Weeks 9–12)
**Goal:** Healthcare revenue, deal valuation, geographic imputation

**C1: Healthcare Revenue Imputation**
- MIWAE/GRAPE trained on CMS Medicare utilization × specialty mix
- Output: total practice revenue estimate per NPI
- Feeds: healthcare vertical acquisition scoring

**C2: Deal Valuation Comparables**
- TabDDPM trained on EDGAR public company EV/EBITDA multiples by NAICS × vintage
- Output: synthetic deal comp dataset for LBO model benchmarking

**C3: Geographic Gap Filling**
- Kriging interpolation for EIA utility rates + FCC broadband by county
- Output: complete county × dimension coverage (no more null site scores)

### Phase D — Job Posting Augmentation (Weeks 13–16)
**Goal:** Hiring signal coverage for 100% of company universe (not 30%)

**D1: ATS-Agnostic Hiring Signal**
- BLS OES occupation distributions + ESCO taxonomy + LLM synthesis
- Condition on: NAICS + headcount_bucket + growth_stage (from SAM.gov/Form D)
- Output: `hiring_signals_synthetic` table (company × occupational_group × signal_strength)

---

## Part 7 — Open Questions / Research Needed

1. **Differential privacy budget**: Synthetic data generation from public company data that is used to infer private company attributes may have privacy implications. Need to audit whether DP-SGD (Abadi et al. 2016) is needed in the CTGAN training loop.

2. **Calibration validation**: How to validate that synthetic private company financials have the right distribution without ground truth? Approach: hold-out set of public companies treated as "private" — compare imputed vs actual EDGAR.

3. **Temporal consistency in PE graphs**: REaLTabFormer handles relational FK integrity — but does it preserve temporal ordering (fund vintage → investment date → exit date)? May need custom constraint injection.

4. **CFTC COT as leading indicator**: Does CFTC commitment data predict sector equity performance with enough lead time to improve deal scores? Run Granger causality on COT disaggregated data × sector ETF returns.

5. **LLM fine-tuning for executive profile synthesis**: GReaT fine-tuned on `pe_people` + `company_people` — at what sample size does synthetic profile quality degrade below useful? Estimate: need ≥ 5,000 real profiles per sector.

---

## Part 8 — New Source Acquisitions That Unlock Synthetic Extension

Some synthetic extensions become dramatically more powerful with modest new data acquisitions:

| Acquisition | Cost | Unlocks |
|-------------|------|---------|
| Cambridge Associates PE benchmarks (public summary data) | Free | Phase A2 macro scenarios + Phase C2 deal valuation |
| Preqin free tier (fund-level strategy descriptions) | Free | LP→GP KGC training signal |
| BLS Quarterly Census of Employment & Wages (QCEW) | Free (existing BLS) | Better NAICS-level employment for chain 2 |
| Census Business Formation Statistics (BFS) | Free | Startup formation signal for roll-up timing |
| Census Longitudinal Business Database (public version) | Free | Establishment birth/death rates by NAICS+county |
| EPA TRI (Toxics Release Inventory) | Free | Deeper environmental chain 2 |
| FDA FAERS (adverse event reports) | Free | Healthcare vertical compliance risk |
| USAspending subawards | Free (already available) | Supply chain exposure for portfolio companies |

None require paid subscriptions — all are FOIA or public use datasets.

---

## Summary

This plan identifies:
- **8 cross-source signal chains** that produce computable PE intelligence products
- **~20 join keys** that form the backbone of the entity graph
- **10 synthetic data extensions** ranked by impact × feasibility × research maturity
- **4 implementation phases** covering 16 weeks
- **8 new free data acquisitions** that dramatically improve synthetic coverage

The highest ROI immediate action is **Phase A**: private company financial imputation (GRAPE/TabDDPM on EDGAR XBRL) + macro scenario generation (Diffusion-TS via TSGM on FRED history). These two extensions alone would:
1. Enable deal scoring for private company targets (not just sector-level macro)
2. Enable stress-tested deal scores across 1,000 macro scenarios (not just current conditions)

---

## Cross-Domain Recommended Stack

| Priority | Domain | Best Method | Implementation Phase |
|----------|---------|-------------|---------------------|
| **P0** | Tabular business records | TabDDPM + GReaT | Phase A |
| **P0** | Cross-source imputation | HyperImpute + GRAPE | Phase A |
| **P1** | PE deal flow synthesis | FinDiff + Gaussian Copula | Phase C |
| **P1** | KG completion (LP→GP) | RotatE + CompanyKG transfer | Phase B |
| **P1** | Org chart / people | CAPER + ERGM | Phase B |
| **P2** | Financial time series | Diffusion-TS via TSGM | Phase A |
| **P2** | Job posting augmentation | JobGen + ESCO API | Phase D |
| **P2** | Location/foot traffic | ST-TrajGAN + Kriging | Phase C |
| **P3** | Regulatory/compliance | SmartNoise DP-CTGAN | Phase C |

---

## References

### Time Series Synthesis
- Yoon, Jarrett, van der Schaar. "Time-series Generative Adversarial Networks." NeurIPS 2019. [GitHub](https://github.com/jsyoon0823/TimeGAN)
- Yuan, Qiao. "Interpretable Diffusion for General Time Series Generation (Diffusion-TS)." ICLR 2024. [arXiv:2403.01742](https://arxiv.org/abs/2403.01742) · [GitHub](https://github.com/Y-debug-sys/Diffusion-TS)
- Nikitin et al. "A Flexible Framework for Generative Modeling of Synthetic Time Series (TSGM)." NeurIPS 2024. [arXiv:2305.11567](https://arxiv.org/abs/2305.11567) · [GitHub](https://github.com/AlexanderVNikitin/tsgm)
- Sattarov et al. "FinDiff: Diffusion Models for Financial Tabular Data Generation." ACM ICAIF 2023. [arXiv:2309.01472](https://arxiv.org/abs/2309.01472) · [GitHub](https://github.com/sattarov/FinDiff)
- "Generation of Synthetic Financial Time Series by Diffusion Models." Quantitative Finance, 2025. [arXiv:2410.18897](https://arxiv.org/html/2410.18897v1)
- "FM-TS: Flow Matching for Time Series Generation." arXiv:2411.07506 (2024). [Link](https://arxiv.org/html/2411.07506v1)

### Tabular Synthesis
- Xu, Skoularidou, Cuesta-Infante, Veeramachaneni. "Modeling Tabular data using Conditional GAN (CTGAN)." NeurIPS 2019. [GitHub](https://github.com/sdv-dev/CTGAN)
- Kotelnikov, Baranchuk, Rubachev, Babenko. "TabDDPM: Modelling Tabular Data with Diffusion Models." ICML 2023. [arXiv:2209.15421](https://arxiv.org/abs/2209.15421) · [GitHub](https://github.com/yandex-research/tab-ddpm)
- Borisov et al. "Language Models are Realistic Tabular Data Generators (GReaT)." ICLR 2023. [arXiv:2210.06280](https://arxiv.org/abs/2210.06280) · [GitHub](https://github.com/tabularis-ai/be_great)
- Solatorio, Dupriez. "REaLTabFormer: Generating Realistic Relational and Tabular Data using Transformers." arXiv:2302.02041 (2023). [GitHub](https://github.com/worldbank/REaLTabFormer)

### Knowledge Graph Completion
- Sun, Deng, Nie, Tang. "RotatE: Knowledge Graph Embedding by Relational Rotation in Complex Space." ICLR 2019.
- Carosia et al. (EQT Motherbrain). "CompanyKG: A Large-Scale Heterogeneous Graph for Company Similarity Quantification." ACM SIGKDD 2024. [arXiv:2306.10649](https://arxiv.org/abs/2306.10649) · [GitHub](https://github.com/EQTPartners/CompanyKG)
- Jiang et al. "KG-FIT: Knowledge Graph Fine-Tuning Upon Open-World Knowledge." NeurIPS 2024.
- "KLR-KGC: Knowledge-Guided LLM Reasoning for Knowledge Graph Completion." Electronics 2024.
- PyKEEN library: [pykeen/pykeen](https://github.com/pykeen/pykeen)

### Imputation
- You, Ma, Yi, Leskovec. "Handling Missing Data with Graph Representation Learning (GRAPE)." NeurIPS 2020.
- Jarrett, Cebere, Liu, Curth, van der Schaar. "HyperImpute: Generalized Iterative Imputation with Automatic Model Selection." ICML 2022. [GitHub](https://github.com/vanderschaarlab/hyperimpute)
- Mattei, Frellsen. "MIWAE: Deep Generative Modelling and Imputation of Incomplete Data Sets." ICML 2019.
- Yoon, Jordon, van der Schaar. "GAIN: Generative Adversarial Imputation Nets." ICML 2018.
- "IVGAE: Handling Incomplete Heterogeneous Data with a Variational Graph Autoencoder." arXiv:2511.22116 (2025).

### Geographic / Location
- ACM Computing Surveys 2024. "Generative Models for Synthetic Urban Mobility Data: A Systematic Literature Review." [doi:10.1145/3610224](https://dl.acm.org/doi/10.1145/3610224)
- "ST-TrajGAN: A Synthetic Trajectory Generation Algorithm." Future Generation Computer Systems, Elsevier 2024.
- Rao, Gao et al. "LSTM-TrajGAN." GeoDS Lab, UW-Madison. [GitHub](https://github.com/GeoDS/LSTM-TrajGAN)
- "GeoGen: A Two-stage Coarse-to-Fine Framework for Fine-grained Trajectory Generation." arXiv:2510.07735 (2025).

### Org Chart / Career Graphs
- Bigdasgit team. "CAPER: Enhancing Career Trajectory Prediction using Temporal Knowledge Graph and Ternary Relationship." ACM SIGKDD 2025. [arXiv:2408.15620](https://arxiv.org/abs/2408.15620) · [GitHub](https://github.com/Bigdasgit/CAPER)
- STATNET team. "Exponential Random Graph Models." [statnet.org](https://statnet.org)
- "Unmasking Fake Careers: Detecting Machine-Generated Career Trajectories via Multi-layer Heterogeneous Graphs (CareerScape)." arXiv:2509.19677 (2025).

### PE Valuation
- CFA Institute Research and Policy Center. "Synthetic Data in Investment Management." July 2025. [GitHub](https://github.com/CFA-Institute-RPC/Synthetic-Data-For-Finance)

### Job Postings
- Colombo, D'Amico, Malandri, Mercorio, Seveso. "JobGen/JobSet: Synthetic Job Advertisements Dataset." ACM SAC 2025. [GitHub](https://github.com/Crisp-Unimib/JobGen)
- "A Framework for Generating Synthetic Job Postings." ACL NLP4HR Workshop 2024. [doi:10.18653/v1/2024.nlp4hr-1.4](https://aclanthology.org/2024.nlp4hr-1.4.pdf)
- "LLM4Jobs: Unsupervised Occupation Extraction and Standardization Leveraging LLMs." Knowledge-Based Systems 2025. [arXiv:2309.09708](https://arxiv.org/html/2309.09708)

### Regulatory / DP Synthesis
- Lin, Gopi et al. (Microsoft). "Private Evolution (DPSDA): Differentially Private Synthetic Data via Foundation Model APIs." ICLR 2024 + ICML 2024. [GitHub](https://github.com/microsoft/DPSDA)
- SmartNoise SDK (OpenDP): [opendp/smartnoise-sdk](https://github.com/opendp/smartnoise-sdk)
- Federal CDO Council RFI on Synthetic Data. Federal Register, January 2024.
- "Does Differentially Private Synthetic Data Lead to Synthetic Discoveries?" PubMed 2024.

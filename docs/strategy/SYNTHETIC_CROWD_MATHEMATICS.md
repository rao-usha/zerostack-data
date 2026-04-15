# PLAN_059: Mathematical Foundations for Synthetic Crowd Intelligence

> **Purpose:** Rigorous mathematical and statistical foundations for building high-quality
> synthetic survey simulations using LLM ensembles, grounded by 28+ real data sources,
> for PE deal evaluation on the Nexdata platform.
>
> **Author:** Deep research synthesis
> **Date:** 2026-04-12
> **Status:** Research Document

---

## Table of Contents

1. [Aggregation Theory](#1-aggregation-theory)
2. [Correlation and Independence in LLM Ensembles](#2-correlation-and-independence-in-llm-ensembles)
3. [Calibration and Validation](#3-calibration-and-validation)
4. [Persona Construction Mathematics](#4-persona-construction-mathematics)
5. [Response Distribution Modeling](#5-response-distribution-modeling)
6. [Ensemble Methods](#6-ensemble-methods)
7. [Grounding and RAG Mathematics](#7-grounding-and-rag-mathematics)
8. [Practical Formulas and Algorithms](#8-practical-formulas-and-algorithms)

---

## 1. Aggregation Theory

### 1.1 Galton's Ox-Weight Experiment: The Statistical Mechanism

In 1907, Francis Galton recorded 787 independent guesses of an ox's weight at a county
fair. The median guess was 1,207 lbs; the actual weight was 1,198 lbs (0.75% error).
The mean guess was 1,197 lbs (0.08% error).

**The mathematical mechanism is the Law of Large Numbers applied to estimators:**

Let the true value be `theta`. Each estimator `i` produces:

```
X_i = theta + epsilon_i
```

where `epsilon_i` is the error term. If errors are:
- **Unbiased:** `E[epsilon_i] = 0`
- **Independent:** `Cov(epsilon_i, epsilon_j) = 0` for `i != j`
- **Finite variance:** `Var(epsilon_i) = sigma_i^2 < infinity`

Then the sample mean:

```
X_bar = (1/N) * sum(X_i) = theta + (1/N) * sum(epsilon_i)
```

has variance:

```
Var(X_bar) = (1/N^2) * sum(sigma_i^2) = sigma_avg^2 / N
```

The MSE of the crowd is:

```
MSE(X_bar) = Bias^2 + Var(X_bar) = 0 + sigma_avg^2 / N
```

**Key insight:** The crowd's error shrinks as `O(1/N)` — but ONLY under independence. This
is the fundamental tension for LLM ensembles, where independence is violated.

**Why Galton's experiment worked:**
1. Diverse crowd (farmers, butchers, laypeople) = low correlation
2. Private guesses (written on cards) = no information cascading
3. Relevant knowledge distributed across crowd = low bias
4. Proper aggregation (arithmetic mean)

### 1.2 Condorcet's Jury Theorem: Formal Statement

**Theorem (Condorcet, 1785).** Consider `N` voters (N odd) deciding between two
alternatives by majority rule. Each voter independently selects the correct
alternative with probability `p`. Let `P_N` be the probability that the majority
is correct. Then:

```
P_N = sum_{k=ceil(N/2)}^{N} C(N,k) * p^k * (1-p)^(N-k)
```

**Three results follow:**

**(a) Competence condition:** If `p > 1/2`:
```
P_N > p   for all N >= 3
```

**(b) Growing reliability:** If `p > 1/2`:
```
lim_{N -> infinity} P_N = 1
```
The convergence rate is exponential: `1 - P_N ~ exp(-N * D(1/2 || 1-p))`
where `D(a || b) = a*ln(a/b) + (1-a)*ln((1-a)/(1-b))` is the KL divergence.

**(c) Incompetence curse:** If `p < 1/2`:
```
lim_{N -> infinity} P_N = 0
```

**Critical assumptions and when they break:**

| Assumption | Formal Statement | Failure Mode for LLMs |
|---|---|---|
| Independence | `P(X_i=1 | X_j=1) = P(X_i=1)` | Same training data = correlated errors |
| Competence | `p > 1/2` for all voters | LLMs can be systematically wrong on niche topics |
| Homogeneity | All voters share same `p` | Different models have different competence |
| Sincerity | Voters reveal true beliefs | LLMs may exhibit sycophancy bias |
| Dichotomy | Exactly 2 alternatives | Real surveys have continuous/multi-option responses |

**Relaxation for heterogeneous competence (Grofman, Owen, Feld 1983):**
If voters have competence `p_i > 1/2` for all `i`, the theorem still holds with:
```
P_N -> 1   as N -> infinity
```
provided `(1/N) * sum(p_i) > 1/2` (average competence exceeds 1/2).

**Relaxation for correlated voters (Ladha 1992):**
With pairwise correlation `rho` between voters, the effective number of independent voters is:
```
N_eff = N / (1 + (N-1)*rho)
```
The theorem holds (P_N -> 1) only if `N_eff -> infinity`, i.e., `rho -> 0` as `N -> infinity`.

### 1.3 Diversity Prediction Theorem (Scott Page, 2007)

**Theorem.** For any collection of `N` predictors making estimates `{x_1, ..., x_N}`
of a true value `theta`, define:

- **Collective prediction:** `x_bar = (1/N) * sum(x_i)`
- **Collective error:** `(x_bar - theta)^2`
- **Average individual error:** `(1/N) * sum((x_i - theta)^2)`
- **Prediction diversity:** `(1/N) * sum((x_i - x_bar)^2)`

Then the following identity holds **exactly** (it is algebraic, not statistical):

```
(x_bar - theta)^2 = (1/N) * sum((x_i - theta)^2) - (1/N) * sum((x_i - x_bar)^2)

Collective Error = Average Individual Error - Prediction Diversity
```

**Proof.** Expand the average individual error:
```
(1/N) * sum((x_i - theta)^2)
  = (1/N) * sum((x_i - x_bar + x_bar - theta)^2)
  = (1/N) * sum((x_i - x_bar)^2) + 2*(x_bar - theta)*(1/N)*sum(x_i - x_bar) + (x_bar - theta)^2
  = Diversity + 0 + Collective Error
```
The cross term vanishes because `sum(x_i - x_bar) = 0`. QED.

**Implications for LLM ensembles:**

| Scenario | Avg. Error | Diversity | Collective Error |
|---|---|---|---|
| Same model, same prompt, same temp | High | **Low** | High |
| Same model, varied prompts/temp | High | Medium | Medium |
| Multiple models, varied prompts | High | **High** | **Low** |
| Models + grounding data variation | Medium | **High** | **Low** |

**Measuring diversity in LLM ensembles:**
For continuous responses, diversity = variance of the ensemble:
```
D = (1/N) * sum((x_i - x_bar)^2) = Var(predictions)
```

For categorical responses (K categories), diversity can be measured as:
```
D = 1 - sum_k (p_k)^2     (Gini-Simpson diversity index)
```
where `p_k` is the fraction of ensemble members choosing category `k`.

For distributional predictions, use the average pairwise Jensen-Shannon divergence:
```
D_JS = (2 / (N*(N-1))) * sum_{i<j} JSD(P_i || P_j)
```

### 1.4 Hong-Page Theorem on Diverse Problem Solvers

**Theorem (Hong & Page, 2004, PNAS).** Consider a set `Phi` of problem solvers,
each with a set of local optima `L(phi)` on a fixed problem landscape. Define:

- **Ability** of solver `phi`: expected distance of `phi`'s solution from global optimum
- **Diversity** of a team `T`: `|union_{phi in T} L(phi)|` (coverage of local optima)

Then for sufficiently large initial pool `N`, a randomly selected team of size `N_1 < N`
outperforms the team of the `N_1` best individual solvers with probability approaching 1.

**Formal conditions:**
1. The problem has a finite landscape with multiple local optima
2. Each solver uses a deterministic local search heuristic
3. The solver population is diverse: `L(phi_i) != L(phi_j)` with high probability
4. Each solver is above a minimum competence threshold

**Mathematical intuition via bias-variance:**
```
Team Error = Avg. Individual Error - Team Diversity
```
The best solvers have low individual error but HIGH correlation (they find the same
local optima). Random solvers have higher individual error but their diverse heuristics
cover more of the solution space, and the diversity term dominates.

### 1.5 Bias-Variance Decomposition for Crowds

For the MSE of an averaged ensemble:

```
MSE(x_bar) = Bias(x_bar)^2 + Var(x_bar)
```

where:
```
Bias(x_bar) = E[x_bar] - theta = (1/N) * sum(E[x_i] - theta) = avg bias
Var(x_bar) = (1/N^2) * sum_i sum_j Cov(x_i, x_j)
```

**For equal-variance, equal-correlation ensemble:**
```
Var(x_bar) = (sigma^2 / N) * (1 + (N-1)*rho)    [WRONG — common error]
```

**Correct formula:**
```
Var(x_bar) = (1/N^2) * [N*sigma^2 + N*(N-1)*rho*sigma^2]
           = (sigma^2 / N) * [1 + (N-1)*rho]

As N -> infinity:
  If rho = 0: Var(x_bar) -> 0                      (full cancellation)
  If rho > 0: Var(x_bar) -> rho * sigma^2           (irreducible floor)
```

**This is the fundamental limit for LLM ensembles:** if responses are positively
correlated (rho > 0), averaging more responses cannot reduce variance below
`rho * sigma^2`. Adding more LLM responses beyond `N ~ 1/rho` is wasteful.

---

## 2. Correlation and Independence in LLM Ensembles

### 2.1 Correlation Structure Model

Model each LLM response as:

```
Y_{m,p,t} = mu + alpha_m + beta_p + gamma_t + epsilon_{m,p,t}
```

where:
- `mu` = population mean response
- `alpha_m` = model effect (GPT-4 vs Claude vs Gemini), `Var(alpha) = sigma_m^2`
- `beta_p` = prompt/persona effect, `Var(beta) = sigma_p^2`
- `gamma_t` = temperature/sampling effect, `Var(gamma) = sigma_t^2`
- `epsilon_{m,p,t}` = residual, `Var(epsilon) = sigma_e^2`

Total variance: `sigma_total^2 = sigma_m^2 + sigma_p^2 + sigma_t^2 + sigma_e^2`

**Correlation between two responses from the same model, same prompt, different samples:**
```
rho_within = (sigma_m^2 + sigma_p^2) / sigma_total^2
```

**Correlation between same model, different prompts:**
```
rho_model = sigma_m^2 / sigma_total^2
```

**Correlation between different models, different prompts:**
```
rho_across = 0   (ideally)
```

In practice, `rho_across > 0` because models share training data (Common Crawl, Wikipedia,
etc.), creating a "shared knowledge" effect that induces positive correlation.

### 2.2 Effective Sample Size

**Definition.** Given `N` correlated observations with pairwise correlation `rho`, the
effective sample size is:

```
N_eff = N / DEFF
```

where DEFF is the **design effect** (Kish, 1965):

```
DEFF = 1 + (n_bar - 1) * rho_ICC
```

For LLM ensembles where all responses come from the same "cluster" (same model):
```
DEFF = 1 + (N - 1) * rho
N_eff = N / (1 + (N-1) * rho)
```

**Numerical examples for LLM ensembles:**

| N responses | rho | DEFF | N_eff | Efficiency |
|---|---|---|---|---|
| 100 | 0.00 | 1.0 | 100.0 | 100% |
| 100 | 0.10 | 10.9 | 9.2 | 9.2% |
| 100 | 0.30 | 30.7 | 3.3 | 3.3% |
| 100 | 0.50 | 50.5 | 2.0 | 2.0% |
| 100 | 0.80 | 80.2 | 1.2 | 1.2% |
| 100 | 0.95 | 95.1 | 1.1 | 1.1% |

**At rho = 0.80, 100 LLM responses give you the statistical power of 1.2 independent
human respondents.** This is the core challenge.

**Empirical finding (Huang, Wu & Wang 2025):** For social opinion surveys, existing
LLMs' responses represent **at most 60 randomly selected people** in the general U.S.
population. For middle-school math, LLMs can barely mimic 10 real students.

### 2.3 Intraclass Correlation Coefficient (ICC) for LLM Ensembles

The ICC is estimated from a one-way random-effects ANOVA:

```
ICC = (MS_between - MS_within) / (MS_between + (k-1) * MS_within)
```

where `k` is the number of responses per "cluster" (model/prompt combination).

**How to measure ICC in practice:**
1. Generate `N_models * N_prompts * N_samples` responses
2. Treat (model, prompt) as the clustering variable
3. Compute between-cluster and within-cluster variance
4. `ICC = sigma_between^2 / (sigma_between^2 + sigma_within^2)`

**Interpretation for synthetic surveys:**
- ICC < 0.05: Low correlation, averaging is very effective
- ICC 0.05-0.25: Moderate correlation, need diversity strategies
- ICC 0.25-0.50: High correlation, diminishing returns from more samples
- ICC > 0.50: Very high correlation, ensemble is behaving like 1-2 respondents

### 2.4 Methods to Reduce Correlation (with Mathematical Effect)

**Method 1: Temperature Variation**

Temperature `tau` in softmax: `P(token_i) = exp(z_i / tau) / sum_j exp(z_j / tau)`

As `tau` increases:
- Response entropy increases: `H(P) = -sum P_i log P_i`
- The distribution approaches uniform as `tau -> infinity`
- Within-model correlation decreases

**Approximate effect on ICC:**
```
ICC(tau) ~ ICC(1.0) * (1 / tau)     for tau in [0.5, 2.0] (empirical approximation)
```

But there's a bias-variance tradeoff: higher `tau` reduces correlation but increases
individual error variance.

**Method 2: Model Diversity**

Using `M` different models with shared-knowledge correlation `rho_shared`:
```
rho_effective = rho_within / M + rho_shared * (M-1) / M
```

**Method 3: Asymmetric Information Injection (Grounding Diversity)**

Give each LLM respondent DIFFERENT subsets of grounding data:
```
Y_i = f(prompt_i, context_i, persona_i)
```

If the grounding data accounts for fraction `g` of total response variance:
```
ICC_grounded ~ ICC_base * (1 - g)
```

This is the most powerful lever for Nexdata: each synthetic respondent gets a
different portfolio of SEC filings, FRED data, BLS statistics, job postings, etc.

**Method 4: Adversarial/Devil's Advocate Prompting**

Explicitly prompt some fraction of the ensemble to argue the opposite position:
```
rho_adversarial ~ rho_base - 2 * f_adversarial * (1 - f_adversarial)
```
where `f_adversarial` is the fraction prompted adversarially.

### 2.5 Spearman-Brown Prophecy Formula for LLM Ensembles

The Spearman-Brown formula predicts the reliability of a composite measure made
by averaging `k` parallel measurements:

```
rho_k = (k * rho_1) / (1 + (k-1) * rho_1)
```

where:
- `rho_1` = reliability of a single measurement (ICC of one response)
- `rho_k` = reliability of the average of `k` measurements
- `k` = number of measurements (LLM responses)

**Solving for required `k` to achieve target reliability `rho_target`:**
```
k = rho_target * (1 - rho_1) / (rho_1 * (1 - rho_target))
```

**Example:** If single-response ICC is `rho_1 = 0.30` and target is `rho_target = 0.90`:
```
k = 0.90 * 0.70 / (0.30 * 0.10) = 21 responses needed
```

**Connection to Cronbach's Alpha:**
For an ensemble of `k` LLM responses with average inter-response correlation `r_bar`:
```
alpha = (k * r_bar) / (1 + (k-1) * r_bar)
```

This is identical to the Spearman-Brown formula. Cronbach's alpha gives the
reliability of the ensemble mean as a measure of the "true" synthetic crowd opinion.

---

## 3. Calibration and Validation

### 3.1 Brier Score for Synthetic Crowd Predictions

For probabilistic predictions of binary events, the **Brier Score** is:

```
BS = (1/N) * sum_{t=1}^{N} (f_t - o_t)^2
```

where `f_t` is the predicted probability and `o_t in {0,1}` is the actual outcome.
Range: [0, 1], lower is better. Perfect calibration: BS = 0.

**Murphy Decomposition (1973):**

```
BS = CAL - RES + UNC
```

where:

```
CAL (Calibration/Reliability) = (1/N) * sum_{k=1}^{K} n_k * (f_k - o_bar_k)^2
RES (Resolution)              = (1/N) * sum_{k=1}^{K} n_k * (o_bar_k - o_bar)^2
UNC (Uncertainty)             = o_bar * (1 - o_bar)
```

- `K` = number of probability bins
- `n_k` = number of forecasts in bin `k`
- `f_k` = average forecast probability in bin `k`
- `o_bar_k` = observed frequency of event in bin `k`
- `o_bar` = overall base rate

**For synthetic surveys:** CAL measures systematic bias in the LLM crowd. RES
measures the crowd's ability to discriminate between questions where the answer
is truly different. UNC is fixed by the data.

**To improve BS:** minimize CAL (via calibration/rectification) and maximize RES
(via diversity and grounding).

### 3.2 KL Divergence and Jensen-Shannon Divergence

**KL Divergence** (Kullback-Leibler):
```
D_KL(P || Q) = sum_x P(x) * log(P(x) / Q(x))
```

Properties:
- `D_KL >= 0` (Gibbs' inequality)
- `D_KL = 0` iff `P = Q`
- **Asymmetric:** `D_KL(P||Q) != D_KL(Q||P)`
- Undefined if `Q(x) = 0` where `P(x) > 0`

For continuous distributions:
```
D_KL(P || Q) = integral P(x) * log(P(x) / Q(x)) dx
```

**Jensen-Shannon Divergence** (symmetric, bounded):
```
JSD(P || Q) = (1/2) * D_KL(P || M) + (1/2) * D_KL(Q || M)
```
where `M = (P + Q) / 2` is the mixture distribution.

Properties:
- `0 <= JSD <= log(2)` (with natural log; `0 <= JSD <= 1` with base-2 log)
- Symmetric: `JSD(P||Q) = JSD(Q||P)`
- Always finite (even when supports differ)
- `sqrt(JSD)` is a proper metric (satisfies triangle inequality)

**Application to synthetic survey validation:**

Let `P_real` = distribution of responses from real human survey
Let `P_synth` = distribution from LLM synthetic survey

```
Fidelity = 1 - JSD(P_real || P_synth) / log(2)
```

A fidelity score > 0.90 indicates the synthetic distribution closely matches reality.

**For multi-dimensional surveys** (K questions with L response levels each):
```
JSD_total = (1/K) * sum_{k=1}^{K} JSD(P_real^k || P_synth^k)
```

### 3.3 Chi-Squared Goodness-of-Fit for Synthetic Validation

For categorical survey responses with `K` categories:

```
chi^2 = sum_{k=1}^{K} (O_k - E_k)^2 / E_k
```

where:
- `O_k` = observed count in category `k` from synthetic survey
- `E_k` = expected count based on real population distribution
- `df = K - 1`

**Critical values for validation:**
- `p > 0.05`: Synthetic distribution is consistent with real (PASS)
- `p < 0.05`: Statistically significant departure (WARN)
- `p < 0.001`: Severe mismatch (FAIL)

**Effect size (Cramer's V) for practical significance:**
```
V = sqrt(chi^2 / (N * min(K-1, L-1)))
```
- V < 0.10: negligible difference
- V 0.10-0.30: small difference (acceptable for synthetic)
- V > 0.30: large difference (synthetic needs recalibration)

**Important caveat for correlated samples:** The standard chi-squared test assumes
independent observations. For correlated LLM responses, use the Rao-Scott correction:

```
chi^2_RS = chi^2 / DEFF
df_RS = df / DEFF
```

where DEFF is the design effect from Section 2.2.

### 3.4 Confidence Intervals with Correlated Respondents

**Standard CI (independent samples):**
```
CI = x_bar +/- z_{alpha/2} * sigma / sqrt(N)
```

**Design-effect-adjusted CI (correlated LLM responses):**
```
CI = x_bar +/- z_{alpha/2} * sigma / sqrt(N_eff)
   = x_bar +/- z_{alpha/2} * sigma * sqrt(DEFF) / sqrt(N)
```

**Cluster-robust standard errors (sandwich estimator):**

For regression coefficients from synthetic survey data:
```
V_CR = (X'X)^{-1} * B_hat * (X'X)^{-1}
```
where:
```
B_hat = sum_{c=1}^{C} (sum_{i in c} X_i * e_i) * (sum_{i in c} X_i * e_i)'
```
- `C` = number of clusters (model-prompt combinations)
- `e_i` = residual for observation `i`

**Degrees-of-freedom correction:** With small number of clusters `C`, use:
```
V_CR2 = ((C * (N-1)) / ((C-1) * (N-K))) * V_CR
```

### 3.5 Bayesian Calibration with Small Real Samples

Given:
- Prior from synthetic survey: `pi_synth ~ Dir(alpha_1, ..., alpha_K)` for K-category response
- Small real sample: `n = (n_1, ..., n_K)` from `M` real respondents

**Posterior via Dirichlet-Multinomial conjugacy:**
```
pi_calibrated ~ Dir(alpha_1 + n_1, ..., alpha_K + n_K)
```

**Power prior approach** (Ibrahim & Chen 2000):

Weight the synthetic data by a discounting parameter `a_0 in [0,1]`:
```
pi_calibrated ~ Dir(a_0 * alpha_1 + n_1, ..., a_0 * alpha_K + n_K)
```

where `a_0` controls how much to trust the synthetic prior:
- `a_0 = 0`: ignore synthetic data entirely (pure real data)
- `a_0 = 1`: give synthetic data full weight
- `a_0 = N_eff_synth / N_synth`: weight by effective sample size ratio

**Optimal `a_0` via empirical Bayes:**
```
a_0_hat = argmax_{a_0} L(data | a_0) = argmax_{a_0} integral L(data | pi) * p(pi | a_0, synth) d_pi
```

**For continuous outcomes (Normal-Normal model):**

Synthetic prior: `mu ~ N(mu_synth, sigma_synth^2 / N_eff_synth)`
Real data: `x_bar_real` from `n_real` observations with known `sigma^2`

Posterior:
```
mu_calibrated = w * mu_synth + (1-w) * x_bar_real
```
where:
```
w = (sigma^2 / n_real) / (sigma^2 / n_real + sigma_synth^2 / N_eff_synth)
```

As `n_real -> infinity`, `w -> 0` (real data dominates).
As `N_eff_synth -> infinity`, `w -> 1` (synthetic dominates, but N_eff is capped).

**Rectification method (from Huang et al. 2025):**

Given synthetic distribution `P_synth` and small calibration sample,
use importance weighting:
```
w_i = P_real(x_i) / P_synth(x_i)
```

Then the calibrated estimate of any population parameter `theta`:
```
theta_calibrated = sum_i w_i * g(x_i) / sum_i w_i
```

Key finding: allocating ~20% of human data budget to fine-tuning and ~80% to
rectification minimizes total bias, reducing it below 5% while increasing
effective sample size by up to 14x.

---

## 4. Persona Construction Mathematics

### 4.1 Stratified Sampling for Persona Generation

**Target:** Match the synthetic respondent pool to a target population with known
marginal distributions across `D` demographic dimensions.

Let the target population have:
- Dimension 1 (role): `{CEO, CFO, VP, Director, Manager}` with proportions `p_1`
- Dimension 2 (sector): `{PE, VC, Corp Dev, IB, ...}` with proportions `p_2`
- Dimension 3 (AUM tier): `{<1B, 1-5B, 5-20B, 20B+}` with proportions `p_3`
- etc.

**Proportional allocation:** Sample `N * p_{d,k}` personas from stratum `(d,k)`.

**Optimal (Neyman) allocation** for minimum variance:
```
n_h = N * (N_h * sigma_h) / sum_{h'} (N_{h'} * sigma_{h'})
```
where `N_h` is the population size of stratum `h` and `sigma_h` is the within-stratum
standard deviation. Strata with more variance get more respondents.

### 4.2 Raking / Iterative Proportional Fitting (IPF)

When the full cross-tabulation of the target population is unknown (only marginals
are available), use raking to construct weights.

**Algorithm:**

```
Input:
  - N synthetic respondents with attributes (d_1, d_2, ..., d_D)
  - Target marginal distributions {p_d} for each dimension d
  - Initial weights w_i = 1/N

Repeat until convergence (typically 3-10 iterations):
  For each dimension d = 1, ..., D:
    For each category k in dimension d:
      Adjustment factor: f_{d,k} = p_{d,k} / sum_{i: d_i = k} w_i
      Update weights: w_i *= f_{d,k}  for all i where d_i = k

Output: Calibrated weights {w_i}
```

**Convergence guarantee (Csiszar 1975):** For any table without structural zeros,
IPF converges to the unique distribution that matches all specified marginals and
minimizes the KL divergence from the initial distribution:

```
w* = argmin_{w: marginals match} D_KL(w || w_init)
```

**Practical note for Nexdata:** Raking targets should come from real data sources:
- Role distribution: from LinkedIn/job postings data
- Sector distribution: from SEC filings universe
- AUM distribution: from PE fund databases
- Geography: from Census/BLS data

### 4.3 Synthetic Population Generation from Marginals

**Problem:** Given marginal distributions for `D` dimensions but not the full
joint distribution, generate a synthetic population of `N` personas.

**Method 1: IPF on a seed matrix**

Start with a uniform cross-tabulation, then apply IPF to match all marginals.
Result: a joint distribution that matches marginals with minimal assumptions
(maximum entropy principle).

**Method 2: Copula-based generation**

1. For each dimension `d`, define marginal CDF `F_d`
2. Specify a copula `C(u_1, ..., u_D)` for the dependency structure
3. Sample: `(U_1, ..., U_D) ~ C`, then `X_d = F_d^{-1}(U_d)`

Common choice: Gaussian copula with correlation matrix `R` estimated from
whatever partial joint information is available.

**Method 3: Bayesian network**

Specify a DAG over dimensions. For PE deal evaluation personas:
```
Role -> AUM tier -> Deal experience
Sector -> Role specialization
Geography -> Regulatory knowledge
```

Each conditional distribution is parameterized from real data.

### 4.4 Dimensionality of Persona Variation

**Question:** How many persona dimensions actually matter for response diversity?

**Answer via PCA / factor analysis:** Collect real survey data, compute the
covariance matrix of responses across persona dimensions, and find the number
of principal components explaining > 90% of variance.

**Empirical findings from social science:**
- For political opinion surveys: 2-3 dimensions (ideology, education, age) explain ~70% of variance
- For consumer surveys: 3-5 dimensions
- For PE deal evaluation (estimated): 4-6 dimensions:
  1. Role/seniority (determines perspective)
  2. Sector expertise (determines domain knowledge)
  3. Risk appetite (bullish vs conservative)
  4. Deal size preference (determines relevance thresholds)
  5. Geographic focus
  6. Investment horizon (growth vs value)

**Diminishing returns:** Beyond the top `d*` dimensions, additional persona variation
adds noise but not signal. Estimate `d*` by cross-validation: add dimensions until
out-of-sample prediction diversity stops increasing.

---

## 5. Response Distribution Modeling

### 5.1 The Under-Dispersion Problem

**Empirical finding:** LLM responses have systematically lower variance than real
human responses. Synthetic estimates show far smaller standard deviations than found
in real respondent data. Within-persona standard deviation can be 0.00 for LLMs,
compared to expected human within-group standard deviation of 1.0-1.5.

**Mathematical characterization:**

Let `sigma_human^2` = variance of real human responses to a survey question
Let `sigma_LLM^2` = variance of LLM responses (across diverse prompts/personas)

The **dispersion ratio:**
```
DR = sigma_LLM^2 / sigma_human^2
```

Typically `DR in [0.3, 0.7]` — LLM variance is 30-70% of human variance.

**Causes:**
1. **Mode collapse:** LLMs converge to the most frequent training-data response
2. **Sycophancy:** tendency to agree rather than take extreme positions
3. **Central tendency bias:** LLMs favor moderate, "safe" responses
4. **Homogeneous training data:** Common Crawl overrepresents certain viewpoints

**Correction Method 1: Variance inflation**

```
X_corrected = x_bar + (X_raw - x_bar) * (sigma_human / sigma_LLM)
```

This preserves the mean but inflates the spread to match target variance.

**Correction Method 2: Quantile mapping**

Map synthetic quantiles to real quantiles:
```
X_corrected = F_real^{-1}(F_synth(X_raw))
```

This preserves the rank ordering but adjusts the full distribution shape.

**Correction Method 3: Beta-binomial inflation** (for Likert-scale responses)

Fit a beta-binomial model and increase the overdispersion parameter:
```
Var(Y) = n * p * (1-p) * (1 + rho * (n-1))
```
Increase `rho` from `rho_LLM` to `rho_human` to match observed human overdispersion.

### 5.2 Beta-Binomial Model for Survey Responses

For a K-point Likert scale (e.g., 1-5 or 0-10), model the response distribution as:

**Beta-Binomial(n, alpha, beta):**
```
P(Y = k) = C(n,k) * B(k + alpha, n - k + beta) / B(alpha, beta)
```

where `B` is the Beta function and `n = K - 1`.

**Parameters:**
- Mean: `mu = n * alpha / (alpha + beta)`
- Variance: `n * alpha * beta * (alpha + beta + n) / ((alpha + beta)^2 * (alpha + beta + 1))`
- Overdispersion: `rho = 1 / (alpha + beta + 1)`

**Interpretation:**
- `rho = 0` (alpha + beta -> infinity): reduces to binomial (each respondent has same `p`)
- `rho > 0`: extra-binomial variation (heterogeneous respondents)
- Larger `rho` = more extreme/polarized responses

**Fitting from data:**
Method of moments:
```
p_hat = x_bar / n
rho_hat = (s^2 / (n * p_hat * (1 - p_hat)) - 1) / (n - 1)
alpha_hat = p_hat * ((1 - rho_hat) / rho_hat)
beta_hat = (1 - p_hat) * ((1 - rho_hat) / rho_hat)
```

### 5.3 Mixture Models for Multi-Modal Opinions

Real populations often have multi-modal opinion distributions (e.g., politically
polarized issues). LLMs tend to produce unimodal (bell-curve) responses.

**Gaussian Mixture Model (GMM):**
```
P(x) = sum_{c=1}^{C} pi_c * N(x | mu_c, sigma_c^2)
```

where `pi_c` is the mixing weight for component `c`, `sum pi_c = 1`.

**EM Algorithm for fitting GMM:**

E-step (compute responsibilities):
```
gamma_{i,c} = pi_c * N(x_i | mu_c, sigma_c^2) / sum_{c'} pi_{c'} * N(x_i | mu_{c'}, sigma_{c'}^2)
```

M-step (update parameters):
```
pi_c = (1/N) * sum_i gamma_{i,c}
mu_c = sum_i gamma_{i,c} * x_i / sum_i gamma_{i,c}
sigma_c^2 = sum_i gamma_{i,c} * (x_i - mu_c)^2 / sum_i gamma_{i,c}
```

Iterate until log-likelihood converges.

**For PE deal evaluation:** Model the "investment committee" opinion distribution as
a mixture of:
- Bullish camp: `N(mu_bull, sigma_bull^2)` with weight `pi_bull`
- Bearish camp: `N(mu_bear, sigma_bear^2)` with weight `pi_bear`
- Undecided: `N(mu_mid, sigma_mid^2)` with weight `pi_mid`

**Generating multi-modal synthetic responses:**
1. Fit GMM to any available real data or expert priors
2. Assign each persona to a component based on persona attributes
3. Sample response from the assigned component
4. This produces the multi-modality that raw LLM prompting misses

### 5.4 Temperature as a Variance Parameter

**Softmax with temperature:**
```
P(token_i | tau) = exp(z_i / tau) / sum_j exp(z_j / tau)
```

**Effect on entropy:**
```
H(tau) = -sum_i P(token_i | tau) * log P(token_i | tau)
```

- `tau -> 0`: `H -> 0` (deterministic, always picks argmax)
- `tau = 1`: standard softmax
- `tau -> infinity`: `H -> log(V)` (uniform over vocabulary `V`)

**Approximate relationship between temperature and response variance:**

For a response `Y` derived from token sampling at temperature `tau`:
```
Var(Y | tau) ~ Var(Y | 1) * tau^gamma
```

where `gamma in [1, 2]` depends on the specific question type.

**Empirical calibration:** For a 1-10 rating scale, typical values:
- `tau = 0.3`: Var ~ 0.5 (very concentrated)
- `tau = 0.7`: Var ~ 1.5
- `tau = 1.0`: Var ~ 3.0
- `tau = 1.5`: Var ~ 5.0
- `tau = 2.0`: Var ~ 6.5 (approaching maximum)

**Optimal temperature for matching human variance:**
```
tau* = (sigma_human / sigma_LLM_at_tau1)^{1/gamma}
```

But increasing temperature introduces more noise, not meaningful diversity.
Temperature adjustment should be combined with persona diversity and grounding
for best results.

---

## 6. Ensemble Methods

### 6.1 Bagging Analogy for LLM Crowds

**Bootstrap Aggregating (Bagging):**

1. Generate `B` bootstrap datasets from the grounding data
2. Train/prompt each LLM ensemble member on a different bootstrap sample
3. Average predictions

For LLM synthetic surveys, the "bagging" analogy is:

```
For b = 1 to B:
  context_b = random_subset(grounding_data, with_replacement)
  response_b = LLM(prompt, persona, context_b)
Aggregate: y_bar = mean(response_1, ..., response_B)
```

**Variance reduction from bagging:**
```
Var(y_bar_bagged) = rho * sigma^2 + (1 - rho) * sigma^2 / B
```

where `rho` is the average pairwise correlation between ensemble members.
The irreducible floor is `rho * sigma^2`.

### 6.2 Mixture of Experts for Synthetic Crowds

**Architecture:**
```
Response = sum_{m=1}^{M} g_m(x) * f_m(x)
```

where:
- `g_m(x)` = gating/routing function (weight for expert `m` given input `x`)
- `f_m(x)` = expert `m`'s response
- `sum_m g_m(x) = 1`

**For PE deal evaluation, experts map to analytical perspectives:**

| Expert | Perspective | Model/Prompt Strategy |
|---|---|---|
| Financial analyst | DCF, multiples, margins | GPT-4 + SEC filings context |
| Operations expert | Efficiency, workforce | Claude + BLS/job posting data |
| Market analyst | TAM, competition, trends | Gemini + industry reports |
| Risk assessor | Regulatory, ESG, litigation | GPT-4 + EPA/OSHA data |
| Deal veteran | Comparable transactions | Claude + historical deal data |

**Gating function:** For question type `q`:
```
g_m(q) = exp(w_m * phi(q)) / sum_{m'} exp(w_{m'} * phi(q))
```
where `phi(q)` is a feature embedding of the question.

### 6.3 Dempster-Shafer Theory for Combining Expert Judgments

**Setup:** Frame of discernment `Theta = {theta_1, ..., theta_K}` (possible outcomes).

Each expert `i` provides a **basic probability assignment (BPA):**
```
m_i: 2^Theta -> [0,1]
m_i(empty_set) = 0
sum_{A subseteq Theta} m_i(A) = 1
```

**Belief and Plausibility:**
```
Bel(A) = sum_{B subseteq A} m(B)       (lower probability bound)
Pl(A) = sum_{B cap A != empty} m(B)   (upper probability bound)
```

True probability lies in `[Bel(A), Pl(A)]`.

**Dempster's Rule of Combination** (for independent evidence):

For two experts with BPAs `m_1, m_2`:
```
m_12(C) = (1/K_norm) * sum_{A cap B = C} m_1(A) * m_2(B)
```

where the normalization factor handles conflict:
```
K_norm = 1 - sum_{A cap B = empty} m_1(A) * m_2(B)
```

**Application to PE deal scoring:** Each data source provides evidence about
deal quality on a scale `{Bad, Neutral, Good}`:

```
SEC filings evidence: m_SEC({Good}) = 0.4, m_SEC({Good, Neutral}) = 0.3, m_SEC(Theta) = 0.3
Job postings evidence: m_JP({Good}) = 0.6, m_JP(Theta) = 0.4
BLS data evidence: m_BLS({Neutral}) = 0.5, m_BLS(Theta) = 0.5
```

Combine via Dempster's rule to get a consensus BPA.

**Advantage over averaging:** D-S theory explicitly represents ignorance (mass on `Theta`)
and handles conflicting evidence via the normalization constant.

### 6.4 Bayesian Model Averaging (BMA)

**For `M` models (GPT-4, Claude, Gemini, etc.):**

```
P(y | data) = sum_{m=1}^{M} P(y | M_m, data) * P(M_m | data)
```

**Posterior model weights:**
```
P(M_m | data) = P(data | M_m) * P(M_m) / sum_{m'} P(data | M_{m'}) * P(M_{m'})
```

where the marginal likelihood (model evidence) is:
```
P(data | M_m) = integral P(data | theta, M_m) * P(theta | M_m) d_theta
```

**In practice for LLM ensembles:**

1. Hold out a calibration set of questions with known answers
2. Score each model's accuracy: `s_m = exp(-lambda * Brier_m)`
3. Normalize: `w_m = s_m / sum_{m'} s_{m'}`
4. Weight future predictions: `P(y) = sum_m w_m * P_m(y)`

**BMA predictive intervals** are better calibrated than any single model because
they account for model uncertainty.

### 6.5 The Extremizing Algorithm (Satopaa et al. 2014)

**Problem:** Simple averaging of forecasts is under-confident (too close to 50%).
Forecasters each have private information they don't fully incorporate.

**Algorithm:**

1. Convert probabilities to log-odds: `l_i = log(f_i / (1 - f_i))`
2. Compute geometric mean of odds: `l_bar = (1/N) * sum(l_i)`
3. Extremize: `l_final = d * l_bar`
4. Convert back: `f_final = 1 / (1 + exp(-l_final))`

where `d > 1` is the **extremizing factor**.

**Mathematical justification (information diversity model):**

If each forecaster observes a signal `s_i` and forms a posterior, but the aggregator
doesn't know what signals were observed, the optimal aggregation pushes the average
toward the extremes. The optimal `d` depends on:

```
d* = 1 + (N - 1) * (1 - rho_info)
```

where `rho_info` is the average correlation of forecasters' information sets.

- Fully overlapping information (`rho_info = 1`): `d* = 1` (simple average is optimal)
- Fully independent information (`rho_info = 0`): `d* = N` (strong extremizing)

**Empirical values:** From the Good Judgment Project, `d in [1.2, 3.9]` minimizes
Brier score. For LLM ensembles with shared training data, `d` should be closer to 1.

**Application to Nexdata:** When aggregating LLM deal-quality forecasts:
1. Each LLM gets different grounding data (increases information independence)
2. Aggregate via extremized mean of log-odds
3. Calibrate `d` on historical deal outcomes

---

## 7. Grounding and RAG Mathematics

### 7.1 Information-Theoretic Framework for RAG

**Mutual Information between context and response:**
```
I(C; Y) = H(Y) - H(Y | C)
```

where:
- `H(Y)` = entropy of response without context (prior uncertainty)
- `H(Y|C)` = entropy of response given context (posterior uncertainty)
- `I(C;Y)` = information gain from grounding

**For Nexdata's 28+ data sources, the total information from grounding is bounded by:**
```
I(C_total; Y) <= sum_{s=1}^{28} I(C_s; Y)     (with equality iff sources are independent)
```

In practice, sources are correlated (e.g., FRED macro data and BLS employment move together):
```
I(C_total; Y) = sum_s I(C_s; Y) - sum_{s<t} I(C_s; C_t; Y) + ...  (inclusion-exclusion)
```

### 7.2 How Grounding Reduces Response Correlation

**Without grounding:** All LLM responses draw from the same training data distribution.
```
rho_no_ground ~ 0.7 - 0.9
```

**With shared grounding:** All responses conditioned on same context.
```
rho_shared_ground ~ 0.8 - 0.95   (even higher — same context -> same answer)
```

**With DIVERSE grounding (Nexdata advantage):** Each respondent gets different data.
```
rho_diverse_ground ~ 0.2 - 0.5   (much lower)
```

**Mathematical model for diverse grounding:**

Let each synthetic respondent `i` receive context `C_i` drawn from:
```
C_i = sample(DataSources, coverage=0.5, seed=i)
```

Then:
```
Y_i = f(prompt, persona_i, C_i) = mu + h(C_i) + g(persona_i) + epsilon_i
```

The correlation between respondents `i` and `j`:
```
rho(Y_i, Y_j) = Cov(h(C_i), h(C_j)) / Var(Y)
```

If `C_i` and `C_j` overlap by fraction `omega`:
```
rho(Y_i, Y_j) ~ omega * (sigma_context^2 / sigma_total^2)
```

**Design recommendation:** Minimize context overlap `omega` while ensuring each
respondent has sufficient context for an informed response.

### 7.3 Pointwise Mutual Information as RAG Quality Metric

**PMI (Pointwise Mutual Information):**
```
PMI(c, y) = log(P(c, y) / (P(c) * P(y)))
```

High PMI between a context chunk and a response token indicates that the context
is genuinely informing the response (not just pattern matching).

**Application:** For each grounding data source, compute:
```
PMI_source = E_{c ~ source, y ~ response} [PMI(c, y)]
```

Rank sources by PMI to determine which grounding data is most valuable:
- SEC 10-K filings: typically high PMI for financial questions
- Job postings: high PMI for operational/growth questions
- FRED macro data: high PMI for macro sensitivity questions
- EPA/OSHA: high PMI for regulatory risk questions

### 7.4 Information Bottleneck for Context Compression

**Objective:** Find compressed representation `T` of context `C` that maximizes
relevant information about response `Y` while minimizing total information:

```
min_{P(T|C)} [I(C; T) - beta * I(T; Y)]
```

where `beta` controls the tradeoff between compression and relevance.

**Solution:** The optimal `P(T|C)` satisfies:
```
P(t|c) = P(t)/Z(c, beta) * exp(-beta * D_KL(P(Y|c) || P(Y|t)))
```

**For Nexdata:** This means we should compress each data source into the minimal
representation that preserves its information about deal quality, rather than
feeding raw 10-K filings (which are mostly noise).

---

## 8. Practical Formulas and Algorithms

### 8.1 Step-by-Step Algorithm for Calibrated Synthetic Survey

```
ALGORITHM: CalibratedSyntheticSurvey

INPUT:
  - Survey questions Q = {q_1, ..., q_K}
  - Target population description Pop
  - Grounding data sources D = {d_1, ..., d_28}
  - Small calibration sample from real respondents: R_cal (optional)
  - Desired effective sample size: N_target
  - Maximum budget: N_max LLM calls

STEP 1: ESTIMATE REQUIRED ENSEMBLE SIZE
  1a. Pilot: Generate 20 responses using single model, estimate within-model ICC
  1b. If using M models, estimate cross-model correlation rho_cross
  1c. Compute effective N per raw response:
      efficiency = 1 / (1 + (k-1) * rho_within)  [k = responses per model]
  1d. Required raw N:
      N_raw = N_target * DEFF
            = N_target * (1 + (N_per_cluster - 1) * ICC)

STEP 2: CONSTRUCT PERSONA POOL
  2a. Define D persona dimensions from target population
  2b. Obtain marginal distributions from real data:
      - Role distribution: from job postings / LinkedIn data
      - Sector: from SEC filings universe
      - AUM tier: from PE fund databases
      - Geography: from Census data
  2c. Generate initial persona pool via IPF:
      - Create D-dimensional cross-tabulation
      - Apply raking to match all marginals (3-10 iterations)
  2d. Sample N_raw personas proportional to raked weights

STEP 3: ASSIGN DIVERSE GROUNDING DATA
  3a. For each persona i, sample a grounding context C_i:
      - Relevant SEC filings (based on sector/size)
      - FRED macro snapshot (shared baseline)
      - BLS employment data (sector-specific)
      - Job postings data (company-specific)
      - EPA/OSHA regulatory data (if relevant)
  3b. Ensure context overlap omega < 0.5 across persona pairs
  3c. Apply information bottleneck compression to each context

STEP 4: GENERATE RESPONSES
  4a. Distribute across M models (e.g., GPT-4, Claude, Gemini)
  4b. For each persona-context pair:
      - Construct prompt: persona description + context + question
      - Set temperature to tau* calibrated to target human variance
      - Generate response
  4c. Include adversarial prompts for f_adv fraction (e.g., 15%)

STEP 5: VALIDATE RAW RESPONSES
  5a. Compute ensemble statistics:
      - Mean, variance, skewness, kurtosis per question
      - ICC across model clusters
      - Effective sample size N_eff
  5b. Check dispersion ratio: DR = Var_synth / Var_target
      - If DR < 0.5: apply variance inflation correction
      - If DR > 2.0: check for prompt issues
  5c. Test for multi-modality (Hartigan's dip test):
      - If significant: fit GMM, verify component structure

STEP 6: CALIBRATE (if calibration sample available)
  6a. Compute distribution shift:
      - JSD(P_synth || P_real) per question
      - Chi-squared goodness-of-fit
  6b. If JSD > 0.1:
      - Apply Bayesian calibration (power prior with a_0)
      - Or apply rectification weights
  6c. If JSD < 0.1:
      - Synthetic distribution is adequate, use directly

STEP 7: AGGREGATE AND REPORT
  7a. For probability/rating questions: compute extremized mean
      - Convert to log-odds, average, extremize by d, convert back
  7b. For distribution questions: report with design-effect-adjusted CIs
      - CI = x_bar +/- z * sigma / sqrt(N_eff)
  7c. Compute and report quality metrics:
      - Effective sample size N_eff
      - ICC per question cluster
      - Brier score (if outcomes known)
      - JSD vs calibration sample
      - Diversity index of ensemble

OUTPUT:
  - Aggregated responses with confidence intervals
  - Quality metrics and validation scores
  - Per-question Brier score decomposition (if applicable)
```

### 8.2 Minimum Ensemble Size Formulas

**For estimating a population mean to within margin `e` with confidence `1-alpha`:**

Standard formula (independent samples):
```
N = (z_{alpha/2} * sigma / e)^2
```

Design-effect adjusted (correlated LLM responses):
```
N_raw = N_independent * DEFF = (z_{alpha/2} * sigma / e)^2 * (1 + (k-1) * ICC)
```

**For estimating a proportion `p` to within margin `e`:**
```
N_raw = (z_{alpha/2}^2 * p * (1-p) / e^2) * DEFF
```

**Example for PE deal evaluation:**
- Estimating "% of experts who would approve this deal" (p ~ 0.6)
- Desired margin: e = 0.05 (5 percentage points)
- Confidence: 95% (z = 1.96)
- Estimated ICC: 0.30
- Cluster size: 10 per model-prompt

```
N_independent = (1.96^2 * 0.6 * 0.4) / 0.05^2 = 369
DEFF = 1 + (10-1) * 0.30 = 3.7
N_raw = 369 * 3.7 = 1,365 LLM responses

With 3 models: 1365 / 3 = 455 per model
With 10 prompts per model: 46 temperatures/samples per prompt
```

**Reducing the requirement via diversity strategies:**
If grounding diversity reduces ICC from 0.30 to 0.10:
```
DEFF = 1 + 9 * 0.10 = 1.9
N_raw = 369 * 1.9 = 701 (48% reduction)
```

### 8.3 Power Analysis for Synthetic Surveys

**Two-sample comparison (synthetic survey A vs B):**

To detect a difference `delta` between two conditions:
```
N_per_group = 2 * (z_{alpha/2} + z_beta)^2 * sigma^2 / delta^2 * DEFF
```

For 80% power (z_beta = 0.84), alpha = 0.05 (z_alpha/2 = 1.96):
```
N_per_group = 2 * (1.96 + 0.84)^2 * sigma^2 / delta^2 * DEFF
            = 15.68 * sigma^2 / delta^2 * DEFF
```

**Effect size formulation (Cohen's d = delta/sigma):**
```
N_per_group = 15.68 / d^2 * DEFF
```

| Effect size (d) | DEFF = 1.0 | DEFF = 2.0 | DEFF = 5.0 |
|---|---|---|---|
| Large (0.8) | 25 | 49 | 123 |
| Medium (0.5) | 63 | 126 | 314 |
| Small (0.2) | 392 | 784 | 1,961 |

### 8.4 Confidence Interval Reporting

**For a mean response from N_raw correlated LLM responses:**

```
Point estimate: x_bar = (1/N) * sum(x_i)

Standard error (naive): SE_naive = s / sqrt(N)
Standard error (corrected): SE_corrected = s * sqrt(DEFF) / sqrt(N) = s / sqrt(N_eff)

95% CI: x_bar +/- 1.96 * SE_corrected

Report format:
  "Synthetic crowd estimate: 7.2/10 (95% CI: 6.4 - 8.0)
   Based on N=500 synthetic responses (N_eff=45, ICC=0.28, DEFF=11.1)
   Across 3 models, 15 personas, calibrated against 30 real respondents"
```

**For proportions:**
```
SE_p = sqrt(p_hat * (1 - p_hat) / N_eff)
CI = p_hat +/- 1.96 * SE_p
```

### 8.5 Summary: Key Formulas Reference Card

| Formula | Expression | When to Use |
|---|---|---|
| Effective sample size | `N_eff = N / (1 + (N-1)*rho)` | Always, for any LLM ensemble |
| Design effect | `DEFF = 1 + (k-1)*ICC` | Clustered synthetic responses |
| Spearman-Brown | `rho_k = k*rho_1 / (1+(k-1)*rho_1)` | Predicting reliability vs ensemble size |
| Required k for target reliability | `k = rho_t*(1-rho_1) / (rho_1*(1-rho_t))` | Planning ensemble size |
| Diversity Prediction Theorem | `Err_crowd = Err_avg - Diversity` | Diagnosing ensemble quality |
| Brier Score | `BS = (1/N)*sum(f_t - o_t)^2` | Evaluating probabilistic forecasts |
| JSD | `JSD = 0.5*KL(P\|\|M) + 0.5*KL(Q\|\|M)` | Comparing synthetic vs real distributions |
| Chi-squared (Rao-Scott) | `chi^2_RS = chi^2 / DEFF` | Testing categorical distribution fit |
| Extremized aggregation | `f = logistic(d * mean(logit(f_i)))` | Aggregating probability forecasts |
| Bayesian calibration | `mu_cal = w*mu_synth + (1-w)*x_bar_real` | Combining synthetic + real data |
| Variance inflation | `X_corr = x_bar + (X-x_bar)*sigma_h/sigma_L` | Correcting under-dispersion |
| Raking convergence | Converges to `argmin D_KL(w\|\|w0)` s.t. marginals | Persona weighting |
| Bagging variance | `Var = rho*sigma^2 + (1-rho)*sigma^2/B` | Context-diversified ensembles |
| Copula sampling | `X_d = F_d^{-1}(U_d)`, `U ~ C(R)` | Generating correlated personas |
| Power (two-sample) | `N = 15.68*sigma^2/delta^2 * DEFF` | Planning comparison studies |

---

## References

### Foundational Theory
- Galton, F. (1907). "Vox Populi." Nature, 75, 450-451.
- Condorcet, M. (1785). Essai sur l'application de l'analyse a la probabilite des decisions.
- Page, S.E. (2007). The Difference: How the Power of Diversity Creates Better Groups. Princeton UP.
- Hong, L. & Page, S.E. (2004). "Groups of diverse problem solvers can outperform groups of high-ability problem solvers." PNAS, 101(46), 16385-16389.

### Forecasting & Aggregation
- Tetlock, P.E. & Gardner, D. (2015). Superforecasting: The Art and Science of Prediction.
- Satopaa, V.A. et al. (2014). "Combining Multiple Probability Predictions Using a Simple Logit Model." Int. J. Forecasting, 30(2), 344-356.

### LLM Synthetic Surveys (2024-2025)
- Huang, C., Wu, Y. & Wang, K. (2025). "How Many Human Survey Respondents is a Large Language Model Worth?" arXiv:2502.17773. (ICML 2025)
- "Valid Survey Simulations with Limited Human Data." arXiv:2510.11408.
- "Polypersona: Persona-Grounded LLM for Synthetic Survey Responses." arXiv:2512.14562.
- "Population-Aligned Persona Generation for LLM-based Social Simulation." arXiv:2509.10127.
- "Specializing Large Language Models to Simulate Survey Response Distributions." arXiv:2502.07068.

### Statistical Methods
- Kish, L. (1965). Survey Sampling. Wiley.
- Murphy, A.H. (1973). "A New Vector Partition of the Probability Score." J. Applied Meteorology.
- Ibrahim, J.G. & Chen, M.H. (2000). "Power Prior Distributions for Regression Models." Statistical Science.
- Dempster, A.P. (1967). "Upper and Lower Probabilities Induced by a Multivalued Mapping." Annals of Mathematical Statistics.
- Shafer, G. (1976). A Mathematical Theory of Evidence. Princeton UP.

### Ensemble Methods
- Breiman, L. (1996). "Bagging Predictors." Machine Learning, 24(2), 123-140.
- Raftery, A.E. et al. (2005). "Using Bayesian Model Averaging to Calibrate Forecast Ensembles." Monthly Weather Review.
- Domingos, P. (2000). "A Unified Bias-Variance Decomposition." ICML.

### Information Theory & RAG
- Tishby, N. et al. (2000). "The Information Bottleneck Method." 37th Allerton Conference.
- "Pointwise Mutual Information as a Performance Gauge for RAG." arXiv:2411.07773.
- "An Information-Theoretic Framework for Retrieval-Augmented Generation Systems." Electronics, 14(15), 2925.

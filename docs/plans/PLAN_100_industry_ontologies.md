# PLAN_100: Industry ontologies, starting with healthcare patient/provider (frontier vs open-weight models)

**Status:** DRAFT v2. It applies both critic passes and is waiting for owner approval. No code until it is approved and `/spec` has run (CLAUDE.md, Workflow Step 0/1).
**Date:** 2026-10-04
**Numbering:**
- **Plan.** PLAN_100. The highest file is 098 (`docs/plans/PLAN_098_gleif_lei_identifiers.md`), and 098/099 are kept free for a parallel session.
- **Specs.** These start at **SPEC_162**. The highest file is 161 (`docs/specs/SPEC_161_production_image_gcp_compose.md`). 155-159 are unused but left alone in case a parallel session has claimed them.
- **Alembic.** The next revision is **0020** (`alembic/versions/0019_gleif.py` is the latest). Before writing it, re-check `alembic heads` and `PARALLEL_WORK.md`, because a parallel session may take 0020 first.

**"Sign off FEMA" (the housekeeping part of the request):** this is already done at HEAD.
- Commit `a56510a` "feat: FEMA x3 rights signed off; kaggle_m5 dropped" adds `fema_disaster_declarations`, `fema_hma_projects` and `fema_pa_projects` at `app/catalog/rights_reviewed.py:49-51`.
- The working tree is clean apart from `.claude/scheduled_tasks.lock`.
- This plan takes no further FEMA action. **Question for the owner:** did you want anything beyond `a56510a`, such as a rights evidence file or a log entry?

**What changed from v1:**
- **NPI attach:** the entity-master approach is redesigned. The old one would have attached 0 rows.
- **Output caps:** lowered, with a split-generation mode.
- **Model SQL:** no SQL written by a model is ever executed. Transforms are a closed enum.
- **Data facts:** the CQ labels and the tally are corrected against live data. The duplicate figure is now 1.37x.
- **Scorer scope:** split into an MVP and later work. Human review is limited to finalists and uses paired tests.
- **Cost:** recomputed for the larger output, with no vendor-specific tokenizer factor. The worst case is published, and reservation is per call and atomic.
- **Vendor terms:** treated the same way for every vendor.
- **OMOP licence:** resolved.
- **Local runs:** a laptop CLI posts its results through the API.
- **Spec order:** fixed.

---

## 1. Goal and the questions it answers

**Goal.** Build a repeatable bake-off.
1. Every model gets the same versioned brief and writes a healthcare **provider/organization ontology with a schema-only patient layer**.
2. The outputs are scored deterministically against public standards (FHIR R4, US Core, Plan-Net, OMOP CDM, NUCC).
3. Above all they are scored on **grounding**: how much of our real NPPES/CMS data binds to each ontology through NPI and CCN.
4. A human merges the best outputs into one signed-off, versioned release, which becomes queryable views.

The pipeline is built so later industries can reuse it.

| # | Question | Answered by |
|---|---|---|
| Q1 | On the same brief, how do closed and open-weight models compare on grounding and standard alignment? Is any difference larger than run-to-run noise? | R1 and R3 by model class, paired tests (§6) |
| Q2 | What does each point of quality cost? | Quality vs. $ and latency, Pareto (§6, §9) |
| Q3 | Where do models agree (a consensus core), and where do they diverge (contested design choices)? | R5b cross-model consensus |
| Q4 | How often does each model invent codes, FHIR elements or OMOP fields? | G4 |
| Q5 | How stable is each model from run to run? | R5a |
| Q6 | Is any model that fits the 12 GB laptop GPU close enough for internal iteration at $0 marginal cost? | Local rows in the same tables |
| Q7 | Do models over-claim what our data can answer? | `cq_coverage.answerable` vs. our labels (rule in §6) |
| Q8 | Which competency questions are blocked by our data rather than by any model? | CQ status tally; grounding reported two ways (§7) |

## 2. Scope and non-goals

**In scope (Phase 1, extraction):**
- Each model writes an ontology from the brief.
- The outputs are validated, converted to OWL/Turtle, scored, compared, and merged by a human into a release.
- The release is populated as SQL views over provider data.
- NPI is linked to the entity master for type-2 (organisation) NPIs only, through a new attach path (§7.3).

**Non-goals:**
- **No PHI, ever.** The patient side is schema only and has no bindings (guard in §5.4). NPPES rows are never sent to an external API. Type-1 NPIs are natural persons (`app/catalog/rights.py:287-291`, `pii=personal`). Population and SHACL checks run locally.
- **No model-written SQL is executed.** Binding transforms are a closed enum that our own code implements (§5.2, §5.4).
- **No distillation or fine-tuning in Phase 1.** Phase 2 (§11) proceeds only after legal review of each vendor's terms.
- **No CPT and no SNOMED content** in briefs, prompts, outputs or scoring (§3).
- **No clinical use.** Gemini API terms forbid use "in clinical practice, to provide medical advice" (https://ai.google.dev/gemini-api/terms, modified 2026-03-23). This product stays a data-model and PE-analytics tool.
- **No scheduler entries.** Runs are triggered by the operator only.
- **No vendor recommendation.** §4 sets out trade-offs and the owner chooses (memory: `feedback_no_self_recommendation.md`).

## 3. Reference standards and licences

| Standard | Use | Source / format | Licence | Answer-key extraction |
|---|---|---|---|---|
| **FHIR R4** | Provider/patient structure answer key | https://hl7.org/fhir/R4/downloads.html: `definitions.json.zip`, `fhir.rdf.ttl.zip` | **CC0** (https://hl7.org/fhir/R4/license.html). Does not cover third-party terminologies inside it. | StructureDefinition `snapshot.element[]`. A resource is a concept, an element path is an attribute, and `Reference(targetProfile)` is a typed relation. |
| **US Core 9.0.0** | Weighting for "required core" | https://hl7.org/fhir/us/core/downloads.html | CC0 | `mustSupport=true` gives the weighted-recall subset |
| **Da Vinci PDex Plan-Net 1.2.0** | Gold relation set for the provider side | http://hl7.org/fhir/us/davinci-pdex-plan-net/ | HL7 IG (CC0 family) | PractitionerRole, OrganizationAffiliation, HealthcareService, Endpoint, Network |
| **OMOP CDM** | Relational/FK answer key | https://github.com/OHDSI/CommonDataModel, `inst/csv/OMOP_CDMv5.4_Field_Level.csv` | **Resolved.** Files vendored from the GitHub repo (Field_Level CSV, DDL) are **Apache-2.0** (repo `DESCRIPTION`: "License: Apache License 2.0", https://raw.githubusercontent.com/OHDSI/CommonDataModel/main/DESCRIPTION). Prose on the docs site is **CC BY-SA 4.0** ("all OHDSI content is subject to the Creative Commons CC BY-SA 4.0 license", https://ohdsi.github.io/CommonDataModel/), so no documentation text is copied into releases. Athena vocabularies are separate and some carry EULAs. | Field_Level CSV: table, field, required flag, FK. The docs page lists **v5.5 as current**; the version is owner decision D3. |
| **NUCC taxonomy v26.1** | Specialty is-a tree and value grounding | https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57 | The CSV page says "Copyright 2026 American Medical Association" and "For commercial use, including sales or licensing, a license must be obtained from this web site." The further conditions listed in v1 (royalty-free, no modification, keep notices) **were not found on the CSV page and are unverified**. Nexdata is commercial, so **D9 (request the licence) is required before any external release.** | Grouping > Classification > Specialization |
| **NPPES (NPI)** | Grounding data and provider schema | https://download.cms.gov/nppes/NPI_Files.html (V.2 monthly full file ~1.1 GB zipped; Other Name, Practice Location, Endpoint files; **Deactivated NPI report**) | FOIA-disclosable (`app/catalog/rights.py:287-291`). The taxonomy columns carry NUCC content, which that rights entry does not mention. | Column headers act as a de facto provider schema |
| **CMS PECOS public enrollment** | PractitionerRole (reassignment) | https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment | US government work; terms to confirm | NPI, ENRLMT_ID, PAC ID |
| **CMS Doctors & Clinicians, facility affiliations** | Practitioner→hospital edge | https://data.cms.gov/provider-data/dataset/mj5m-pzi6, `27ea-46a8` | Terms to confirm | NPI↔CCN |
| **CMS Hospital Enrollments** | CCN↔NPI crosswalk | https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments | Terms to confirm | Joins `cms_hospitals` to the NPI graph |
| **ICD-10-CM FY2027** | Patient side, chapter/category level only | https://www.cms.gov/medicare/coding-billing/icd-10-codes | **Not verified as public domain.** WHO holds the ICD-10 copyright (https://cdn.who.int/media/docs/default-source/publishing-policies/copyright/who-faq-licensing-icd-10.pdf). CMS/CDC distribute it free. Confirm before redistributing descriptions. | Chapter/category hierarchy |
| **HCPCS Level II, CMS POS** | Service-line vocabulary | CMS | Public | Code lists |
| **LOINC** | Not used in v1 | loinc.org | Free with attribution; may not be used to build a competing code system (licence page returned 403) | n/a |
| **RxNorm (prescribable subset)** | Optional, for `cms_drug_pricing` | https://www.nlm.nih.gov/research/umls/rxnorm/faq.html | The prescribable subset needs no licence; the full set needs UMLS | Name→RxCUI |
| **SNOMED CT** | **Excluded** | UMLS | Free in the US under UMLS plus an annual affiliate report. UMLS forbids distributing subsets outside an application. | Excluded |
| **CPT (AMA)** | **Excluded (conservative internal policy)** | AMA | Licence required. The per-user fee is **unverified for 2026**. The AMA AI FAQ says "Using the CPT Standard Data File to train, fine-tune, or improve AI models is prohibited" and "Uploading the CPT Standard Data File into public, unrestricted, or open-market AI systems is prohibited" (https://www.ama-assn.org/practice-management/cpt/licensing-cpt-ai-faqs). Those clauses name the *Standard Data File*. Excluding all CPT descriptor text from prompts is **our policy**, which is broader than the literal clause. | Excluded. `cms_medicare_utilization.hcpcs_desc` contains CPT descriptors and must never reach a prompt. Level-I codes are used only as join values. |

## 4. Models to compare (neutral; the owner chooses)

**Provenance of prices.** All figures are standard-tier list prices per 1M tokens, collected by research lenses on 2026-10-04.
- Only these were re-checked at first party during review:
  - the GPT-5.6 Sol promo note;
  - Anthropic's tokenizer note;
  - Gemini's "Output price (including thinking tokens)".
- Everything else is **unverified until it is entered in `pricing.json`**, with `as_of` and a source URL, on the day of the run. **That entry is a hard prerequisite of the D1 roster decision.**
- **(OR)** marks an OpenRouter cross-provider average (https://openrouter.ai/api/v1/models).

This section does not rank or recommend any vendor, Anthropic included.

**Access routes. All three are neutral options for D1.**

**1. Direct keys.**
- `.env` has OPENAI_API_KEY, GEMINI_API_KEY and XAI_API_KEY.
- There is no Anthropic, DeepSeek or OpenRouter key.

**2. OpenRouter.**
- Reaches any model once a key is added.
- Structured-output enforcement depends on which upstream provider serves the request (`require_parameters: true`; https://openrouter.ai/docs/features/structured-outputs).

**3. Vertex AI Model Garden.**
- Becomes convenient after the 2026-10-11 GCP move.
- Serves Gemini plus third-party models (Anthropic, Llama, Mistral, DeepSeek, Qwen, others) on one GCP bill (https://cloud.google.com/model-garden).
- Prices, terms and regional premiums differ from direct access; Anthropic, for example, notes a 10% premium on regional endpoints. They go into `pricing.json` as separate rows.
- GCP budgets **only alert** (§9.3).

### 4.1 Hosted closed models

| Vendor / model | Access today | In / Out | Batch | Context | Structured output | Output-reuse clause (Phase 2 only; all pending legal review, §11) |
|---|---|---|---|---|---|---|
| OpenAI GPT-5.6 Sol | key | $4 / $20. This is **promotional pricing, "available at least through November 21, 2026"**; the post-promo price is not stated. Above 272k: $8/$30. | 50% | ~1.05M | strict JSON schema | OSA §3.3(e) (see §11) |
| OpenAI GPT-5.6 Terra / Luna | key | $2/$12 ; $0.20/$1.20 | 50% | ~1.05M | strict | same |
| OpenAI GPT-5.5 / 5.5 Pro | key | $5/$30 ; $30/$180 | 50% | 1.05M | strict | same |
| Google Gemini 3.1 Pro **Preview** | key | $2/$12 (≤200k) | $1/$6 | 1M | JSON-schema subset | Gemini API terms (see §11); clinical-use ban; free-tier prompts used for training, paid tier not |
| Google Gemini 3.8 Flash / 3.5 Flash | key | $0.75/$3.75 (**doubles 2027-01-01**) ; $1.50/$9 | 50% | 1M | subset | same |
| xAI Grok 4.7 / 4.3 | key | $2/$6 (≤200k) ; $1.25/$2.50 | A Batch API exists for some models. Check each model page for discount and eligibility (https://docs.x.ai/docs/models). | 500k / 1M | "practical subset" | Enterprise ToS (see §11; the page returned 403 on re-check, unverified) |
| Anthropic Claude Fable 5.1 / Opus 5.5 / Sonnet 5.5 | **no key** (direct key, OpenRouter or Vertex) | $10/$50 ; $4/$20 ; $2/$10 | 50% | 1M | yes | Commercial Terms §D.4 (see §11) |

**Tokenizer differences (all vendors).** Every vendor counts the same text differently: OpenAI o200k, Gemini, Grok, Anthropic, DeepSeek, Qwen and others all have their own tokenizers. Anthropic publishes one note: Claude 4.7+ "produces approximately 30% more tokens for the same text" (https://platform.claude.com/docs/en/about-claude/pricing). Others publish none.
- **v1 applied ×1.3 to Anthropic rows only. This is removed.**
- `brief.py` instead counts the hashed brief with each model's own counter: OpenAI `tiktoken`, Gemini `countTokens`, Anthropic `count_tokens`, xAI tokenize endpoint, HF tokenizers for open weights.
- It stores `in_tok` per model, and estimates use those counts.
- Until the brief is measured, **every** row carries a uniform ±30% band (§9.2).

### 4.2 Open-weight models

**Laptop:**
- **RTX 5070 with 12,227 MiB** (nvidia-smi), 31.8 GB RAM, i7-14700F.
- LM Studio and llama.cpp are installed; Ollama is not.
- On disk: `gemma-4-E4B-it-Q4_K_M.gguf`.

**Fit is now judged on input plus max output**, about **45k (lite)** to **60k (full)** tokens of context at the §5.2 caps. v1 counted input only, which was wrong. The KV-cache size at that context is **unmeasured**, and the pilot measures it.

| Model | Licence | Size | Hosted In/Out | Fits 12 GB at 45-60k context? | Output-reuse terms |
|---|---|---|---|---|---|
| DeepSeek V4-Pro / V4.1-Flash | MIT | 1.6T/49B act ; 284B/13B act | $1.32/$3.96 peak ($0.66/$1.98 off-peak) ; $0.30/$1.20 | No | The API ToU is reported to allow distillation (re-verify the wording). Data location: "we directly collect, process and store your Personal Data in People's Republic of China" (https://cdn.deepseek.com/policies/en-US/deepseek-privacy-policy.html) |
| Qwen3.8-2.4T-A95B | Qwen3.8-Max licence (gate above $50M AI revenue; MAU display rule) | 2.4T/95B act | (OR) $2/$6 | No | Gates only |
| Qwen3.8-27B | Apache 2.0 | 27B dense | (OR) $0.425/$2.55 | No | None (self-run) |
| GLM-5.3 | Custom, MIT-like | 753B | (OR) $1.40/$4.40 | No | MaaS gate only |
| Kimi K3 | Custom (attribution, MaaS gates) | 2.8T/104B act | (OR) $0.72/$13 | No | Gates only |
| Mistral Large 3 / Medium 3.5 | Apache 2.0 / modified MIT (revenue cap from secondary sources, **unverified**) | 675B MoE / 128B | (OR) $0.50/$1.50 ; $1.50/$7.50 | No | API terms **unverified** |
| Gemma 4 E4B / 12B / 26B-A4B / 31B | Apache 2.0 (since 2026-04-02) | up to 31B | 31B (OR) $0.09/$0.34 | E4B (5.3 GB): **likely**. 12B Q4 (7.1 GB): **plausible with q8 KV cache; measure**. 26B-A4B: partial (expert offload, slow). 31B: no. | None |
| Qwen3.5-9B | Apache 2.0 | 9B | (OR) $0.10/$0.15 | Q4 (5.7 GB): **plausible; measure** | None |
| gpt-oss-20b / 120b | Apache 2.0 plus usage policy | 21B/3.6B act ; 120B | (OR) 120b $0.037/$0.17 | 20b: **no** at this context without offload | None |
| Llama 4 Maverick | Llama 4 Community Licence | MoE | (OR) $0.188/$0.652 | No | A distributed model trained on its outputs must carry a "Llama" name prefix; 700M MAU gate |
| Meta Muse Glimmer 30B | Apache 2.0 (secondary sources, **unverified**; HF page returned 401) | 29.6B | (OR) $0.35/$1.50 | No | None |
| NVIDIA Nemotron 3 Ultra | NVIDIA licence, **unverified** | 550B/55B act | (OR) $0.50/$2.20 | No | **Unverified** |

**If a model cannot hold the context, or its documented max output is below 64k, it runs in split mode** (§5.2). That fact is recorded in `providers.py` per model, and the model is never sent a single-call request it is bound to truncate.

**Facts that bear on fairness (not rankings):**
- **Local ≠ the large open-weight class.** Only models of about 12B or smaller, plus MoE models with offload, fit. To answer Q1 for the open class, the large open models must run hosted. Local results are reported as a separate class: "fits the laptop".
- **Structured-output enforcement differs** (strict vs. subset; on OpenRouter it depends on the upstream). Appendix A uses the common subset, and every output is re-validated locally.
- **Preview models can change or be withdrawn.** Exact IDs and snapshots are recorded.
- **Hosted open-weight endpoints add the host's own ToS** (OpenRouter, Vertex, Together, Fireworks, Groq: unverified).
- **Processing jurisdiction and retention apply to every vendor and host**: US vendors, OpenRouter upstreams, Vertex regions and CN-hosted APIs alike. The brief contains schema plus non-PII examples only, which lowers the stakes for all of them equally.

**Selection worksheet (owner fills in; D1).** Choose:
- the roster;
- the access route per model;
- whether premium-tier models are included (§9.4);
- the acceptable processing jurisdictions;
- the R2b translator and judge families (§6).

The v1 illustrative mix (**10 hosted + 2 local**) was not produced by a stated rule, so two mechanical rules are offered instead.
- **Rule A: each vendor's newest-generation top non-premium model.** Closed: GPT-5.6 Sol, Gemini 3.1 Pro, Grok 4.7, Opus 5.5 (Fable 5.1 falls in the premium tier). Open, one per lab, the lab's top hosted model: DeepSeek V4-Pro, Qwen3.8-2.4T, GLM-5.3, Kimi K3, Mistral Large 3, Gemma 4 31B. Optionally add Llama 4 Maverick, Nemotron 3 Ultra and gpt-oss-120b.
- **Rule B: each vendor's cheapest current model.** Closed: GPT-5.6 Luna, Gemini 3.8 Flash, Grok 4.3, Sonnet 5.5 (the cheapest listed here). Open: DeepSeek V4.1-Flash, Qwen3.8-27B, gpt-oss-120b, Gemma 4 31B, Llama 4 Maverick, Mistral Large 3.

The §9 costs use Rule A's first 10, plus local Gemma 4 12B and Qwen3.5-9B. This is for sizing only.

## 5. The brief and the output contract

### 5.1 Design rules
- **Same brief, same hash.** The brief `hc-onto-v1.0` is built by script, and its sha256 is recorded.
  - Two variants: **full** (~18-25k tokens) and **lite** (~10k, for local models). **Every** model runs both.
  - The token counts are estimates. Per-model counts are measured when the brief is built (§4.1).
- **Byte-identical schema in the prompt.** The prompt-embedded schema (Appendix A) is the same bytes for every vendor.
  - Some vendors may need an inlined-`$ref` API-level schema. That variant's hash is stored per run as `api_schema_sha256`, next to `prompt_sha256`.
  - So "same brief" means the same prompt bytes. The API-level schema difference is visible in the record, not hidden.
- **The bundle is generated, not handwritten.**
  - `columns.generated.json` has `"columns": []`, `origin: none` for `nppes_providers` and the `cms_*` tables (`:3362-3390`, `:20170-20178`), and `cms_hospitals` is not in `app/catalog/datasets.py`. The generator therefore needs SPEC_162 (cataloguing and dictionary fill) first, and otherwise falls back to `app/sources/nppes/metadata.py:101-133`, `app/sources/cms/metadata.py:18-125` and `app/sources/cms_hospitals/metadata.py:84-107`.
  - Each table gets a `data_state` flag (sample, duplicates, all-NULL).
- **Licence masking.** CPT rows become `[CPT code - licensed, omitted]`. `hcpcs_desc` is never included, and there is no SNOMED. Example values contain no PII.
- **CQs are given without our HOLD/NEED labels** (for Q7).

### 5.2 Output contract (abbreviated; full schema in Appendix A)

**Caps (reduced from v1).** v1's caps (80 classes / 220 properties / 40 relations) implied about **40-60k output tokens**, plus a 25k brief. That exceeds several vendors' max output and any 12 GB local context. The new caps are:
- **≤40 classes;**
- **≤120 properties;**
- **≤30 relations;**
- **≤10 open_questions.**

This gives an estimated **~30-35k output tokens**.
- The figure is checked before the pilot against a hand-written 10-class fragment scaled up, and then against the pilot outputs.
- Costs in §9 use **40k** to stay conservative.

**Generation modes. Both are recorded in the brief and on every run.**
- **single** (default): one call returns the whole `NexdataOntologyRun`.
- **split**: used when a model's context or max output cannot hold a single call.
  - **Call 1** returns `meta`, `classes`, `relations`, `vocabularies`, `cq_coverage`, `open_questions` and `properties: []`.
  - **Call 2** receives call 1's output and returns `properties` only.
  - The runner merges the two **deterministically** before validation. This is a designed protocol, not a repair.
- **Fairness check:** at least two hosted models also run in split mode, to measure the effect of splitting. Split-mode results are reported in their own column.

**Structure:**
- One flat object. Items reference each other by ID, with no recursion.
- Every field is required and `additionalProperties:false`. "Unknown" is an explicit `null`.
- Only the common subset of structured-output features is used (type, enum, array, object, nullable union), with no `oneOf` or `patternProperties`.

```text
NexdataOntologyRun {
  meta{brief_version, domain="healthcare_provider_patient", generation_mode∈{single,split}, model_self_reported, notes}
  classes[≤40]{id ^[A-Z].., label, definition(≤40w), parent|null, layer∈{provider,organization,location,coverage,
     utilization,ownership,quality_sanction,patient_schema,reference}, is_abstract, phi_risk, mappings[], bindings[], evidence, confidence}
  properties[≤120]{id ^[a-z].., label, definition, kind∈{datatype,object}, domain, range, cardinality, value_set|null,
     is_identifier, mappings, bindings, evidence, confidence}
  relations[≤30]{id, label, definition, subject, object, characteristics[], temporal, inverse_of|null, mappings, evidence, confidence}
  vocabularies[]{id, name, steward, license_status, used_by[], example_codes_from_bundle[≤5]}
  cq_coverage[]{cq_id, answerable∈{with_bundle_data,needs_new_data,schema_only,not_modeled}, path[], missing|null}
  open_questions[≤10]
  $defs.mappings[≤4]{standard∈{FHIR_R4,US_CORE,PLAN_NET,OMOP_CDM_5_4}, target, match}
  $defs.bindings[≤6]{table, column (verbatim from bundle §B), transform∈{none,trim,upper,lower,lpad_10,zip5,cast_date,cast_int,cast_numeric,yn_to_bool}}
  $defs.evidence{basis, bundle_refs[]}
}
```

**`transform` is a closed enum, not SQL.**
- Each value is implemented by our own code: a fixed, parameter-free SQL fragment or a Python function.
- Model text is **never** executed as SQL.
- If D3 selects OMOP 5.5, the enum value becomes `OMOP_CDM_5_5`.
- A truncated output, or one that exceeds a cap, is scored as **failed** and is never stitched together.

### 5.3 Competency questions (30; full text and status in Appendix B)

The labels were re-checked against the live tables via `nexdata-api-1` on 2026-10-04.

| Block | CQs | Status today |
|---|---|---|
| A. Provider identity | CQ01-05 | HOLD 02, 05. PARTIAL 01 (all rows `status='A'`; deactivation is NEED). NEED 03, and 04 (**becomes HOLD once SPEC_163 vendors the NUCC CSV**) |
| B. Org hierarchy and affiliations | CQ06-10 | PARTIAL 08. NEED 06, 07, 09, 10 |
| C. Locations | CQ11-14 | HOLD 14. PARTIAL 11 (county/RUCA only for utilization NPIs), 12. NEED 13 |
| D. Payer participation | CQ15-17 | HOLD 15. NEED 16, 17 |
| E. Utilization | CQ18-21 | PARTIAL 18, 19. NEED 20 (all cost-report data columns NULL), 21 |
| F. Ownership and PE roll-ups | CQ22-24 | PARTIAL 23. NEED 22, 24 |
| G. Quality and sanctions | CQ25-27 | HOLD 25. NEED 26, 27 |
| H. Patient schema only | CQ28-30 | SCHEMA |

**Tally today:** HOLD 5, PARTIAL 7, NEED 15, SCHEMA 3 (total 30).
**After SPEC_163** (CQ04 becomes HOLD): HOLD 6, PARTIAL 7, NEED 14, SCHEMA 3.
**Groundable CQs (HOLD + PARTIAL) at round 1: 13.** SPEC_162 re-verifies every label with a SQL probe. The probes become the gold queries (§6, R2).

### 5.4 Deterministic checks on every output (no LLM, no repair)
1. JSON Schema validity.
2. **Referential integrity across one shared ID space**, made of classes ∪ properties ∪ relations, because properties and relations share the Turtle namespace.
   - Every parent, domain, range, subject, object, inverse_of and cq.path ID must resolve.
   - `parent` must name a class.
   - No cycles; no duplicate IDs across all three arrays.
   - This runs **before** conversion.
3. Mapping existence, checked against the FHIR R4 StructureDefinitions and the OMOP Field_Level CSV. This gives the hallucination rate.
4. Binding existence, checked against the bundle manifest. `transform` must be in the enum.
5. **Licence guard.** The run fails on any of these:
   - (a) a string matching `^\d{4}[0-9FTU]$` (CPT Level I/II/III/PLA shape) anywhere except a binding join value or a copied bundle example;
   - (b) any `hcpcs_cd` value whose bundle row was masked, appearing in example codes;
   - (c) fuzzy similarity (rapidfuzz ≥ 90) to any `hcpcs_desc` value. This is **computed locally; `hcpcs_desc` never leaves the database**;
   - (d) SNOMED content, or a `licensed_do_not_enumerate` vocabulary with non-empty example codes.
6. **PHI guard.** The run fails if any binding exists on:
   - a class in the `patient_schema` layer;
   - a property whose `domain` resolves to a `patient_schema` class;
   - a relation whose `subject` or `object` resolves to a `patient_schema` class.
7. **Execution guard.** No model-supplied string is ever interpolated into SQL.
   - Population and value-coverage queries are built by our code from validated identifiers: table and column names come from the manifest allow-list, and transforms come from the enum.
   - They run as a **read-only role** with `statement_timeout` (e.g. 30 s), against a local copy or snapshot of the bundle tables (~120k rows). They never run as the API's database user.

Then a deterministic, sorted Turtle converter produces byte-stable output (Appendix D), with golden-file tests.
- The namespace is `urn:nexdata:onto:hc:{run_id}#`.
- OMOP targets use the local URN `urn:omop:cdm54:` (or `cdm55`), because OMOP has no official IRIs.

## 6. Evaluation metrics

**How runs are ranked:**
- Gates first. Only outputs that pass all gates are ranked.
- There is **no single composite number.** Survivors are ranked by R1, then R2, R3, R5 and H, and plotted against $ and latency.
- Every threshold is a **proposed starting point**, calibrated in the pilot. Until then it is a reporting line, not an automatic pass/fail.

**Scope split.** **MVP (SPEC_166)** is what round 1 needs. **Extended (SPEC_169, Phase 1b, optional)** comes later.

| # | Metric | How computed | Proposed threshold | Spec |
|---|---|---|---|---|
| G1 | Parse validity | JSON Schema, then `rdflib` parse of the Turtle. At most one repair, logged. | 100% after repair; repair count reported | MVP |
| G2 | Logical consistency | ROBOT `reason` with ELK (https://robot.obolibrary.org/reason). HermiT on a sample comes later, because ELK ignores cardinality. | 0 inconsistent, 0 unsatisfiable | ELK: MVP. HermiT: 169 |
| G3 | Hygiene | MVP: ROBOT `report` (same jar). Later: pySHACL meta-shapes. **OOPS! is opt-in only.** It is an external web service that receives the ontology (no PII, but uptime is uncertain). | ROBOT report: 0 ERROR | MVP / 169 |
| G4 | Code/element hallucination | Every asserted code or path is looked up against NUCC, ICD-10-CM, HCPCS L2, FHIR R4 and OMOP. Two rates: *invalid*, and *label-code mismatch*. | Invalid = 0 per item; mismatch ≤2% | MVP |
| **R1** | **Grounding (decisive)** | (a)-(e) below | Column coverage ≥90%; taxonomy row coverage ≥99% | (a)-(c): MVP. (d): 169 |
| R2 | CQ answerability | R2a and R2b below | R2a ≥80% of groundable CQs. R2b per-type thresholds. | R2a: MVP. R2b: 169 |
| R3 | Reference alignment | R3-lite and full R3 below | Core-slice recall ≥0.80, precision ≥0.70; mapping precision ≥0.90 | Lite: MVP. Full: 169 |
| R4 | Unsupported concepts | No reference match, no grounding, and an empty or circular justification. Flagging is automatic. A human gives the verdict on finalists only. | ≤10% flagged | MVP (flag) |
| R5 | Stability and consensus | (a) Within-model concept Jaccard after exact-label plus rapidfuzz alignment, and subClassOf edge Jaccard. (b) Cross-model consensus: a concept proposed by ≥k of m models is a merge prior. Embedding alignment and WL/GED structural similarity come later. | Within-model Jaccard ≥0.70 | MVP / 169 |
| H | Human review | **Finalists only**: the top 3-4 by R1. Blinded and canonicalised, with **50 items each**, stratified and oversampling G4/R4 flags. Rubric: Correct-standard / Correct-non-standard / Wrong / Unjustified, plus a join-corruption flag. One pairwise whole-ontology preference per finalist pair. About **4-6 h total**. | α is a **diagnostic, not a gate**, unless D7 names a second rater | Round |

**R1 detail:**
- **(a) Column coverage.** The share of the column universe bound, scored against the **gold column map**. The map is hand-built in **SPEC_162** and committed with a hash **before any model output exists**.
- **Column universe.** It is stated exactly:
  - live B1-B4 is nppes_providers 30, cms_medicare_utilization 31, cms_hospitals 19 and cms_hospital_cost_reports 24, **104 in total**;
  - ingestion, surrogate-id and timestamp columns are removed;
  - the post-NEED target columns are added per source.
  - Counts are reported per set. v1's figure of "about 150" was wrong.
- **All-NULL tables** (`cms_hospital_cost_reports`) are scored **schema-only** in held-now: they are excluded from the weighted denominator and the exclusion is stated. They are not silently weighted to 0.
- **(b) Value coverage:**
  - row-weighted and distinct-weighted share of `taxonomy_code` values that resolve to NUCC-linked classes;
  - `entity_type` 1/2 → Practitioner/Organization;
  - `hcpcs_cd` → procedure/service class.
- **(c) Join integrity:**
  - NPI is modelled as an identifier on both Practitioner and Organization;
  - share of `rndrng_npi` rows that resolve;
  - CCN for facilities.
- **(d) Instance SHACL** on a stratified local sample of 5k NPIs, under guard 7.
- **(e) Reporting.** Every score is reported **held-now and post-NEED** (§7).

**R2 detail:**
- **R2a, structural (MVP, $0).** Does `cq_coverage.path` resolve through RI-valid classes and properties whose bindings cover the columns the **gold SQL** for that CQ uses? No translator and no LLM spend.
- **R2b, translator (169).** A translator writes SPARQL over each populated graph, and the results are compared with the gold SQL answers:
  - entity lists: set Jaccard ≥0.95;
  - numerics and aggregates (CQ12, CQ18): relative tolerance ±0.5%;
  - ranked lists (CQ19): top-k overlap.
  - The translator comes **from a family not on the roster, or is a local model**, or translators rotate across families with per-translator results reported. The choice is declared in D1.
- The gold SQL and expected-result fixtures for the 13 groundable CQs are written in SPEC_162.
- **Over-claim rule (Q7):**
  - a NEED or SCHEMA CQ claimed `with_bundle_data` is an **over-claim**;
  - a PARTIAL CQ claimed `with_bundle_data` is reported separately as a "partial claim", not an over-claim;
  - a HOLD CQ marked `needs_new_data` or `not_modeled` is an **under-claim**.

**R3 detail:**
- **R3-lite (MVP):** precision and recall of explicit `mappings` against the FHIR/US Core/Plan-Net/OMOP core slice, plus exact and rapidfuzz label matching.
- **Full R3 (169):** embedding cascade (SapBERT or a general encoder) → **cross-family** LLM adjudication for the ambiguous band only → human.
  - Reported as OLLM Literal/Fuzzy/Continuous/Graph F1 plus motif distance (https://arxiv.org/abs/2410.23584).
  - Plus Bio-ML P/R/F1, Hits@1 and MRR (https://krr-oxford.github.io/DeepOnto/bio-ml/).
  - Provider side and patient-schema side are reported separately.

**Statistics:**
- **Primary: pre-registered paired comparisons.** Every model is scored on the same columns and CQs. Per-column binding correctness and per-CQ R2a outcomes are compared pairwise with a **paired permutation test** (or a sign test), with Holm correction.
- **Effect sizes** are reported as the difference in proportion with a 95% CI.
- **Secondary:** mean, range and bootstrap CIs over columns and CQs.
- No winner is declared unless the paired test is significant **and** the effect exceeds the within-model run-to-run spread (R5a).

**Rules on LLM judges and translators (Extended only; the MVP uses none):**
- A model never judges or translates output from its own family.
- Judges are calibrated against the human labels. A judge with κ < 0.6 is used for triage only.
- The existing judge is hard-wired to `gpt-4o-mini` (`app/services/eval_scorer.py:815-844`) and is **not** reused.

**Sampling policy (fixed, applies to every run):**
- The three samples use **temperature 0.7 where the API accepts it**. Otherwise the vendor default is used and recorded (reasoning models).
- Temperature-0 determinism is reported separately, from the pilot only.
- So R5 measures sampling stability, not vendor nondeterminism at temperature 0.

## 7. Grounding: our tables and the NPI provider graph

### 7.1 What we hold (live, 2026-10-04 via `nexdata-api-1`)

| Table | Rows / cols | Maps to FHIR / OMOP | Key | Problems |
|---|---|---|---|---|
| `nppes_providers` | 39,817 NPIs (28,792 type 1, 11,025 type 2); 337 taxonomy codes; 588 rows without a description; 30 cols | Type 1 → Practitioner / PROVIDER. Type 2 → Organization / CARE_SITE. practice_* → Location. taxonomy → PractitionerRole.specialty | NPI | Targeted Registry-API sample (`app/sources/nppes/client.py:31`). **Every row has `status='A'`** because the Registry API returns active NPIs only. **Primary taxonomy only** (`metadata.py:248-282`). One practice address. **No county FIPS or RUCA.** |
| `cms_medicare_utilization` | 40,054 rows; 3,216 NPIs; 1,631 HCPCS; 79 provider types; 31 cols | rndrng_npi → Practitioner. Aggregates → EOB.item / MeasureReport (closest) | NPI | **1.37x duplication**: 29,189 distinct (rndrng_npi, hcpcs_cd, place_of_srvc), so 10,865 excess rows, from **3 ingestion dates** (2026-02-20 to 2026-09-25), with **no data-year column** (BUG-CMS-IDEMPOTENT). The catalog text is stale (`datasets.py:885-896`). Rights restricted/agreement_required. `hcpcs_desc` = CPT. Has `rndrng_prvdr_state_fips` and RUCA for its own 3,216 NPIs. |
| `cms_hospitals` | 5,426; 19 cols | facility_id → Organization + Location; emergency → HealthcareService; stars → MeasureReport | CCN | **No NPI. Not catalogued.** |
| `cms_hospital_cost_reports` | 10; 24 cols | Organization financials | CCN, npi | **All data columns NULL** (schema-only in held-now) |
| `cms_drug_pricing` | 71,595 | Medication / DRUG | name | Keyed by drug, not by prescriber |
| `core.identifier` | cik 138,338; ein 91,777; crd 24,557; lei 429 | n/a | n/a | **No NPI.** `npi` exists as a semantic type (`app/catalog/identifiers.py:48`). There are no types for CCN, PAC ID, NUCC, HCPCS or ICD-10-CM. |

There is **no HUD ZIP→county crosswalk table**; the only HUD table is `realestate_hud_permits`.

**Headline gap:** only **233 of 3,216 utilization NPIs (7.2%) appear in `nppes_providers`**. A grounding score computed today would mostly measure our coverage gaps, so data readiness (SPEC_162) comes first.

### 7.2 The NPI provider graph

| Edge | Source today | Groundable now? |
|---|---|---|
| `hasSpecialty` NUCC (primary) | nppes.taxonomy_code | Yes (sample) |
| `hasAllSpecialties` (up to 15) | NPPES V.2 bulk | NEED |
| `practicesAt` Location | nppes practice_* | Yes (one address) |
| Location `inCounty` / RUCA | utilization FIPS/RUCA (3,216 NPIs only); HUD crosswalk | Partial / NEED |
| `secondaryPracticeAt` | NPPES Practice Location file / PECOS | NEED |
| Provider `deactivated` (date) | NPPES Deactivated NPI report | NEED |
| Organization `subpartOf` parent | nppes.organization_subpart (flag only) | Partial |
| Practitioner `reassignsTo` Group | PECOS REASSIGNMENT | NEED |
| Practitioner `affiliatedWith` Hospital | DAC facility affiliations | NEED |
| Hospital(CCN) `sameAs` Organization(NPI) | Hospital Enrollments | NEED |
| Provider `billedService` (HCPCS × POS) | cms_medicare_utilization (deduped) | Partial (7.2% overlap) |
| Facility `ownedBy` Owner | CMS All Owners | NEED |
| Organization `sameLegalEntity` / EIN → PE portfolio | PECOS / AHRQ Compendium; core.entity weak match | NEED / partial |

**Reporting.** R1 is reported two ways (D5; proposed: both):
- **held-now**, against today's columns;
- **post-NEED**, against the target column set.

The gold column map covers both sets.

### 7.3 Entity master: NPI link (redesigned)

**Why v1 would have attached 0 rows (verified):**
- `_attach_feed(name, table, sql)` hard-codes `attach_only=True` and takes no keyword (`app/entities/feeds.py:492-493`).
- An attach-only row with no website domain is dropped: `if feed.attach_only and not domain: return None` (`feeds.py:368`).
- NPPES has no domain column, so nothing would attach.
- The native-key path (`feeds.py:92`, `:360-361`, `_NATIVE_CANON[feed.native_key]`) is what SPEC_154 used for `lei` (`:294`) and `uei` (`:318`).

**SPEC_168 chooses between two designs (owner sees both):**
- **A. NPI native-key feed.** Add a canonicalizer for `npi`: 10 digits, Luhn check with the `80840` prefix. Allow attach-only resolution when `native_key` is set, so the domain check at `:368` is skipped for native-key feeds. The feed attaches only to an existing entity and never creates or merges one.
- **B. Name+address review path.** Type-2 NPI rows go through the 0016 weak-match review tier (`0016_entity_weak_match.py`) and are attached only after review.

**Invariants and tests, either way:**
- A type-2 NPI row **without a domain** is attached (A), or queued for review (B).
- Type-1 NPIs never enter `core.entity`.
- `npi` is never added to `STRONG_KEYS` (`app/entities/resolve_core.py:124`). One organisation holds many type-2 NPIs, and NPPES publishes no EIN.

## 8. Architecture and spec breakdown

### 8.1 Modules: new `app/ontology/` package, separate from `app/agentic/llm_client.py`

**Why separate:**
- `LLMClient` supports only `openai` and `anthropic` (`app/agentic/llm_client.py:119-154`).
- It has no `base_url` or JSON-schema support.
- Its default `max_tokens=500` (`:102`) would truncate an ontology.
- It retries 3x on any exception, timeouts included (`:195-237`), which can triple the bill.
- The people and PE pipelines depend on it, so it is not widened.

```text
app/ontology/
  providers.py   registry: name -> {adapter: openai_compat|anthropic, base_url, key setting, max_output_tokens,
                 reasoning_control (param name + bounded? y/n), usage_reasoning_field, generation_mode,
                 terms_url, terms_checked_at, jurisdiction}
                 one AsyncOpenAI(base_url=...) adapter: OpenAI, Gemini (OpenAI-compat), xAI, OpenRouter, Vertex (where
                 OpenAI-compat), LM Studio (:1234/v1), llama.cpp llama-server; Anthropic SDK when a key exists
  pricing.json   per-model in/out/cached/reasoning $/M, long-context tiers, promo_end, premium flag (derived), as_of, source URL
  budget.py      preflight, atomic reservation ledger, caps (§9.3); refuses unknown prices and unbounded reasoning
  standards/     vendored FHIR R4, US Core, Plan-Net, OMOP Field_Level CSV, NUCC CSV, ICD-10-CM chapters, HCPCS L2, POS
  schema.py      Pydantic -> Appendix A JSON Schema (+ inlined API variant; both hashed)
  brief.py       bundle generator, full/lite, sha256, per-model token counts
  runner.py      model x sample x variant matrix under asyncio.Semaphore; single/split modes; ≤1 repair; no retry on timeout
  local_run.py   laptop CLI (python -m app.ontology.local_run): calls LM Studio/llama.cpp, POSTs results to the API
  guards.py      licence, PHI, execution guards (§5.4)
  convert.py     deterministic JSON -> Turtle (Appendix D)
  scorer.py      MVP metrics (SPEC_166); extended metrics behind flags (SPEC_169)
  compare.py     metric matrix, paired tests, Pareto, consensus/merge candidates
  store.py       persistence + release export
  populate.py    release bindings -> onto_health.* views via enum transforms only (no LLM, no model SQL)
  gold/healthcare_provider/  column_map.json, cq_gold.sql, cq_fixtures/ (SPEC_162, committed + hashed)
app/api/v1/ontology.py   router; register + OpenAPI tag in app/main.py; admin-only per 0012_access_lockdown.py
```

**Cost-tracker use (corrected).**
- `LLMCostTracker.record()` already accepts `provider=` and `cost_usd=` overrides (`app/core/llm_cost_tracker.py:79-80`, `:96-100`).
- The runner passes both from `providers.py` and `pricing.json`, so `_detect_provider` (`:40-45`) needs **no change**.
- **Reasoning tokens, `cost_basis` (priced/unpriced/local) and the vendor usage fields** are stored **in `onto.run` only**. The shared `llm_usage` table (`app/core/models.py:3683-3717`: id, created_at, model, provider, input/output tokens, `cost_usd`, source, company_id, job_id, prompt_chars) is **not migrated**. *Critics differed here (add to `llm_usage` vs. keep in `onto.run`). `onto.run` was chosen to avoid a migration on a shared table; D12 can revisit.*
- **D12 shrinks to one change:** `_calculate_cost` returns $0 for unknown models with no warning (`llm_cost_tracker.py:50`). Add a warning log there. `llm_client.py:160` has the same pattern and is optional to fix.

**Reused as-is:**
- the `llm_usage` ledger (8,858 rows; gpt-4o is $86.07 to date);
- eval regression detection (`app/services/eval_runner.py:281-358`);
- the `_SCORERS` registry (`eval_scorer.py:1011`), with new assertion types `ontology_schema_valid`, `ontology_grounding_pct` and `ontology_standard_ref_valid_pct` (`eval_suites` has 0 rows today);
- catalog vocabulary: `llm_extracted`, `reference` and `derived_mart` (`app/catalog/spec.py:35-40`);
- lineage computed from `inputs=` (`app/catalog/lineage.py`).

**Eval tooling** goes in a **separate eval environment or image**, not the API image, because Java adds ~200 MB to the SPEC_161 production image.
- MVP: rdflib, ROBOT jar + JRE, rapidfuzz.
- Extended: pyshacl, owlready2, sentence-transformers, networkx, krippendorff.

### 8.2 Tables (Alembic `0020_ontology.py`, schema `onto`)

| Table | Columns |
|---|---|
| `onto.brief` | id, domain, version, variant, generation_mode, sha256, body, prompt_schema_sha256, standards_versions jsonb, gold_sha256, in_tok_by_model jsonb, created_at |
| `onto.run` | id, brief_id, provider, route (direct/openrouter/vertex/local), model, model_snapshot, model_version_reported, prompt_sha256, api_schema_sha256, generation_mode, params jsonb (temperature, max_output_tokens, reasoning_effort/budget, seed), sample_idx, local_only, status, in/out/reasoning/cached tokens, est_cost_usd, reserved_usd, cost_usd, cost_basis, budget_id, job_id, vendor_request_id(s), system_fingerprint, response_headers jsonb, latency_ms, error, timestamps |
| `onto.output` | run_id, raw_text (per call in split mode), parsed jsonb, schema_valid, validation_errors, guard_violations, turtle_sha256 |
| `onto.score` | run_id, metric, value, ci_low, ci_high, details jsonb, scorer_version |
| `onto.binding` | run_id, table, column, class, attribute, transform, exists, value_match_pct |
| `onto.budget` | id, scope (pilot/round/month), cap_usd, reserved_usd, spent_usd, status, created_by |
| `onto.release` | domain, version, source_run_ids, file_path, file_sha256, approved_by, approved_at |

**Where outputs live:**
- Raw outputs live only in the database.
- The merged release is committed at `app/ontology/releases/healthcare_provider/v1.json` through a PR and **signed off by hash** (the `rights_reviewed.py` pattern).
- It is registered as a `DatasetSpec` (kind `reference`, origin `llm_extracted`, `confidence='llm_extracted'` until a human verifies it, per CLAUDE.md).

### 8.3 Endpoints (`/api/v1/ontology`, admin-only)

**Briefs and estimates:**
- `POST /briefs`, `GET /briefs/{id}`.
- `POST /runs/estimate`: a dry run. Returns expected and worst-case cost per model and in total, and flags any model that is unpriced or has unbounded reasoning. Makes **no** model calls; token counting endpoints are free and carry no PII.

**Runs:**
- `POST /runs` {models[], samples, variants[], budget_id, allow_premium?, confirm_worst_case?}:
  - refuses unpriced models, models with unbounded reasoning, and models over the premium threshold without `allow_premium`;
  - warns, and requires `confirm_worst_case`, when the whole-round worst case exceeds the cap;
  - enqueues a worker job (WORKER_MODE=1) for hosted models.
- **Local runs.** `local_only` runs create run rows but **no worker job**.
  - The laptop CLI `python -m app.ontology.local_run --run-id ...` calls LM Studio or llama.cpp.
  - It posts each output to `POST /runs/{id}/ingest-output` with an admin token, so it needs no Cloud SQL access after the move.
  - **Test:** a VM worker never claims or executes a `local_only` run.
- `GET /runs/{id}`, `GET /runs/{id}/output`, `POST /runs/{id}/cancel`.

**Comparison, budget and releases:**
- `GET /compare?brief_id=`: matrix, paired tests and Pareto. Can be rendered as an HTML report with the build-report skill.
- `GET /budget`: spent, reserved and cap for the round and the month.
- `POST /releases`, `GET /releases/{domain}/{version}/coverage`.

### 8.4 Specs, effort and order

Each spec goes through `/spec`, with tests written first.

| Order | Spec | Scope | Effort | Depends on |
|---|---|---|---|---|
| 1 | **SPEC_162 healthcare data readiness + gold** | Add `data_year` to `cms_medicare_utilization` (from the source dataset ID/URL). Unique key `(data_year, rndrng_npi, hcpcs_cd, place_of_srvc)`. Keep the latest ingestion per key. Fix the `datasets.py:885-896` text. Catalog `cms_hospitals`. Fill dictionary columns for nppes and cms. Add semantic types ccn, nucc_taxonomy, hcpcs, icd10cm, pac_id. Close the NPI gap: Registry backfill of ~3k NPIs (30-60 min at 1-2 req/s) **or** the NPPES V.2 bulk load (D4). Rights review of nppes (NUCC note) and cms_medicare_utilization. **SQL probe per CQ label. Hand-built gold column map. Gold SQL and fixtures for the 13 groundable CQs. All committed with a hash before any model run.** | M-L, 4-6 d (+1-2 d if bulk) | none |
| 1 | **SPEC_163 standards vendoring + rights** | FHIR R4, US Core 9.0.0, Plan-Net, OMOP (D3 version; Apache-2.0 repo files only), NUCC 26.1, ICD-10-CM chapters, HCPCS L2, POS. NUCC and ICD terms. | S-M, 1-2 d | none |
| 1 | **SPEC_164 provider registry, pricing, budget guard** | OpenAI-compat adapter. `pricing.json`. `budget.py` with atomic reservation. Per-model max_output, reasoning control and usage fields. Terms record. D12 warning. | M, 2-3 d | none |
| 2 | **SPEC_165 schema, brief, runner, store, Alembic 0020, endpoints, local CLI** | Single/split modes. Guards 5-7. Per-model token counts. Ingest-output endpoint. Smoke tests **only against a mock or a local model** (no hosted outputs before gold is frozen). | L, 4-5 d | 162, 163, 164 |
| 3 | **SPEC_166 scorer MVP + compare** | G1, G2 (ELK), G3 (ROBOT report), G4, R1(a-c), R2a, R3-lite, R4 flags, R5a/b (exact + rapidfuzz), paired tests, converter with golden tests, eval assertion types, compare report | L, 4-5 d | 162, 165 |
| 4 | **Pilot** (not a spec) | 2 owner-picked models × 2 runs × lite+full, plus one split-mode check. Measure output size, context fit, max-output behaviour, and reasoning-token bounding per adapter (§9.3). Calibrate thresholds. Reconcile billed tokens with vendor dashboards. | 1 d + review | 166 |
| 5 | **Round 1 + finalist review** | Owner roster, 3 samples × 2 variants, blinded review of 3-4 finalists | 1 d compute + 4-6 h review | pilot |
| 6 | **SPEC_167 release + populate** | Merge, committed release with hash sign-off, `onto_health.*` views (`provider`, `provider_specialty`, `location`, `provider_location`, `service_line`, `facility`) as derived_mart, via enum transforms under a read-only role | M-L, 2-3 d | round |
| 7 | **SPEC_168 NPI link to entity master** | Design A or B (§7.3) and its tests | M, 2-3 d | 162 |
| 1b | **SPEC_169 extended eval (optional)** | HermiT, pySHACL meta-shapes and instance SHACL (R1d), R2b translator, full R3 cascade (embeddings, OLLM, Bio-ML), WL/GED, judge panel, OOPS! opt-in | L, 6-10 d | 166 |

**Total effort:**
- **About 21-30 engineering days** for Phase 1 (SPECs 162-168, pilot, round), plus owner review time.
- SPEC_169 adds 6-10 d.
- v1's 16-21 d estimate understated the scorer.

**Timing.** The GCP VM move is about 2026-10-11 (~$62/mo; no GPU).
- SPECs 162-164 can land on either side of it.
- The first paid round runs **after** the move, once worker location and Cloud SQL disk are settled.
- The NPPES bulk file, if chosen, needs **several GB** (estimate; measure it). Size the disk before loading.

## 9. Cost, with a hard budget cap

### 9.1 Assumptions

**Per model per round:**
- 3 samples × 2 variants = **6 runs**.
- Input = 3 × (25k + 10k) = **105k**.
- Output ≈ **40k per run (240k)** at the new caps.
- **Reasoning-heavy:** ≈ 100k per run (40k output + ~60k reasoning, billed as output) = **600k**.
- At v1's caps the output would have been ~50-60k per run.

**Formula:**
```
cost = Σ_models Σ_runs (in_tok × p_in + (out_tok + reasoning_tok) × p_out)
```
- **Worst case per run** = `in_tok × p_in + max_output_tokens × p_out`, with `max_output_tokens` = **64k** (non-reasoning) or **128k** (reasoning, including reasoning tokens where the vendor counts them that way). Worst-case figures below use 128k.

**Tokenizer band:** ±30% on every row until per-model counts are measured (§4.1). This replaces v1's Anthropic-only ×1.3.

**Sensitivity:** a 60k brief roughly doubles input cost. Output still dominates.

### 9.2 Per-model round cost

These are list prices. Prompt caching and batch, where a vendor offers them (§4.1), are optional discounts that apply equally across vendors.

| Model | Expected (base to heavy) | Worst case (128k cap) |
|---|---|---|
| GPT-5.6 Sol (promo) | $5-12 | ~$16 |
| GPT-5.6 Terra | $3-7 | ~$9 |
| GPT-5.5 | $8-19 | ~$24 |
| GPT-5.5 Pro *(premium)* | $46-111 | ~$141 |
| Gemini 3.1 Pro Preview | $3-7 | ~$9 |
| Gemini 3.8 Flash (to 2026-12-31) | $1-2 | ~$3 |
| Grok 4.7 | $2-4 | ~$5 |
| Grok 4.3 | $0.7-1.6 | ~$2 |
| Claude Fable 5.1 *(premium)* | $13-31 | ~$39 |
| Claude Opus 5.5 | $5-12 | ~$16 |
| Claude Sonnet 5.5 | $3-6 | ~$8 |
| DeepSeek V4-Pro (peak) | $1.1-2.5 | ~$3 |
| DeepSeek V4.1-Flash | $0.3-0.8 | ~$1 |
| Qwen3.8-2.4T (OR) | $2-4 | ~$5 |
| GLM-5.3 (OR) | $1.2-2.8 | ~$3.5 |
| Kimi K3 (OR) | $3-8 | ~$10 |
| Mistral Large 3 (OR, terms unverified) | $0.4-1 | ~$1 |
| Qwen3.8-27B (OR) | $0.7-1.6 | ~$2 |
| Llama 4 Maverick (OR) | $0.2-0.4 | ~$0.5 |
| Muse Glimmer 30B (OR, licence unverified) | $0.4-0.9 | ~$1 |
| Gemma 4 31B (OR) | $0.1-0.2 | ~$0.3 |
| gpt-oss-120b (OR) | <$0.1 | ~$0.1 |
| **Local** (Gemma 4 E4B/12B, Qwen3.5-9B) | **$0 marginal** (electricity; minutes to tens of minutes per run, unmeasured) | $0 |

**Illustrative round (Rule A's 10 hosted + 2 local; sizing only, not a recommendation):**
- Expected **~$23-54**; with the ±30% tokenizer band, **~$16-70**.
- **Worst case ~$69** (up to ~$90 with the band).
- Adding the two premium-tier models adds **$59-142** expected and up to **~$181** worst case.

**Evaluation spend:**
- **MVP:** **$0** LLM spend. R2a is structural and R3-lite is deterministic.
- **Extended (SPEC_169):**
  - **R3 adjudication** (~222 ambiguous-band calls per ontology, ~1k in / 200 out, at a $0.20/$1.20 tier):
    - on all 60-72 ontologies: ~$6-7 per judge, **~$17-21 for a 3-judge panel**;
    - on **one median run per model (default)**: ~$1.2 per judge, **~$3.5 for the panel**.
  - **R2b translator.** One median run per model × 13 CQs ≈ 156 calls × ~40k tokens ≈ 6.2M input. That is **~$1.5 at a cheap tier and ~$14 at $2/M**, uncached. The second-family re-check adds ~30%. Prefix caching lowers it. Total **~$2-18**.
- All eval calls are charged to the **same round budget** as generation.

**Expected all-in, first full round:**
- **MVP:** ~$16-70.
- **With SPEC_169 eval:** ~$22-95.
- **With both premium-tier models:** up to ~$240 expected and ~$270 worst case.
- Human review (4-6 h for finalists) is the larger real cost.

### 9.3 Hard budget cap mechanism (the owner asked for cost warnings)

1. **Price gate.** Every model needs a `pricing.json` entry with `as_of`, a source URL and, where relevant, `promo_end` (Sol: at least 2026-11-21). A run after `promo_end` requires re-pricing. Unpriced models are **refused** and are never recorded as $0.
2. **Reasoning bound gate.** Each adapter declares how reasoning is bounded and where usage reports it:
   - OpenAI: `max_output_tokens` includes reasoning, plus `reasoning.effort`; usage in `completion_tokens_details.reasoning_tokens`.
   - Gemini: thinking is billed as output, but `thinkingBudget`/`thinkingLevel` is a separate parameter; usage in `thoughtsTokenCount`. Reporting via the OpenAI-compat endpoint is **unverified**.
   - Anthropic: the thinking budget counts in `output_tokens`.
   - xAI: `reasoning_effort`; the cap behaviour is **unverified**.
   - OpenRouter upstreams: **unverified**.
   - The pilot verifies, per adapter, that the output cap plus the reasoning budget bounds billed tokens. If it does not, the model is **refused** (as if unpriced) or gets a model-specific reservation ceiling.
3. **Preflight.** `/runs/estimate` and `/runs` compute the expected cost and the **whole-round worst case** (the 128k/64k cap; long-context tier if a prompt crosses 200k for Gemini/Grok or 272k for OpenAI).
   - A whole-round worst case above the cap is a **warning** that needs `confirm_worst_case=true`.
   - It is not a refusal, because step 4 enforces the cap per call.
4. **Atomic per-call reservation.** Before each call (generation or eval):
   ```sql
   UPDATE onto.budget SET reserved_usd = reserved_usd + :r
    WHERE id = :id AND spent_usd + reserved_usd + :r <= cap_usd
   RETURNING reserved_usd
   ```
   - Zero rows updated means the remaining calls are cancelled and the job ends `budget_exhausted`.
   - After the call, the reservation is settled to actual usage in one transaction.
   - This is safe with concurrent workers (SKIP LOCKED, multiple replicas).
5. **Per-call limits:**
   - explicit `max_output_tokens` and reasoning setting;
   - at most 1 repair;
   - **no retry on timeout**; a truncated run is a failure.
6. **Monthly ceiling.** `ONTOLOGY_MONTHLY_BUDGET_USD` is checked against:
   ```sql
   SELECT SUM(cost_usd) FROM llm_usage
    WHERE source LIKE 'ontology:%' AND created_at >= date_trunc('month', now())
   ```
   plus the in-flight `reserved_usd` from `onto.budget`.
7. **Vendor-side backstops.** The owner sets these up, and the behaviour is verified on setup day. **Alert-only backstops are never relied on as caps.** The in-app ledger is the primary control.

   | Backstop | Behaviour |
   |---|---|
   | OpenRouter prepaid credit / per-key credit limit | **Hard** (prepaid) |
   | Google Cloud billing budget (Gemini API, Vertex incl. third-party models) | **Alert only** |
   | OpenAI project budget | Verify: hard or alert |
   | xAI spend limit | Verify |
   | Anthropic Console workspace spend limit | Verify |

8. **Proposed defaults (D2):** pilot **$20**, round **$100** (generation + eval), month **$200**.
   - These replace v1's $10/$75/$150. Under v1, the illustrative round's worst case (~$69 before the band) nearly filled the round cap before any eval spend.
   - Premium-tier models need `allow_premium=true`.

### 9.4 Cost warnings

**Reasoning tokens are billed as output** and are the main way runs overrun. The pilot checks how each vendor bounds them (§9.3 step 2).

**Premium tier, by a vendor-neutral rule (output price ≥ $40/M):**
- This currently captures **GPT-5.5 Pro ($180)** and **Claude Fable 5.1 ($50)**.
- One full round on GPT-5.5 Pro (~$46-111) costs about **5x the whole hosted open-weight field** (~$9-22).

**Prices move:**
- Gemini 3.x Flash doubles on 2027-01-01.
- GPT-5.6 Sol's promo runs "at least through November 21, 2026", and the post-promo price is not stated.
- DeepSeek peak is 2x off-peak.
- OpenRouter prices are cross-provider averages; first-party prices win where they differ. The v1 OR figure for Sol ($2/$10) is dropped.
- Re-enter all prices on run day.

**Free tiers** (Gemini free, OpenRouter `:free`) may train on prompts. Use paid tiers only.

**Billing of failed calls:**
- A call that returns a refusal or truncated JSON is billed in full.
- A pre-inference 400 schema rejection is generally not billed; verify per vendor in the pilot.
- **Run the cheapest model first.**

**Storage:** the NPPES bulk load adds several GB. Transparency in Coverage data (CQ16) is TB scale and out of scope for Phase 1.

## 10. Risks and mitigations

| Risk | Mitigation |
|---|---|
| **Vendor ToS** on output reuse (OpenAI, Google, xAI, Anthropic, DeepSeek, open-weight licences and hosts). Gemini clinical-use ban. Processing jurisdiction for all vendors. | Phase 1 uses outputs only as design drafts. A verbatim terms table with `terms_checked_at` lives in `providers.py`. All closed vendors share one Phase-2 state: pending legal review (§11). The product stays non-clinical. Briefs contain no PHI and no row data. |
| **Untrusted model output reaching SQL** | Closed-enum transforms. Identifiers allow-listed from the manifest. Read-only role, `statement_timeout`, and a local snapshot (§5.4 guard 7). |
| **Output too large → truncation, local infeasibility** | Lower caps. Split mode. Per-model `max_output_tokens` in `providers.py`. Measured in the pilot. |
| **Licensing** (CPT in `hcpcs_desc`; SNOMED/UMLS; NUCC is AMA copyright; ICD-10 is WHO copyright; OMOP docs are CC BY-SA) | Licence guard with a defined detector. Masking. Apache-2.0 OMOP files only. SPEC_163 terms work. D9 before external release. |
| **PII** | No rows sent to vendors. Local population. Type-1 NPIs out of `core.entity`. PHI guard covers classes, properties and relations. |
| **NPI attach yields 0 rows** (v1 design) | §7.3, design A or B, with an explicit test. |
| **Judge/translator bias** | MVP uses no LLM judges. Extended uses cross-family, κ-calibrated judges and an off-roster or rotating translator. Blinded human review. |
| **Hallucinated codes and elements** | G4 lookups. Wrong mappings count as errors; missing mappings do not. |
| **Grounding measures our gaps** (7.2% overlap, 1.37x duplicates, NULL cost reports, active-only NPIs) | SPEC_162 first. Held-now vs. post-NEED. All-NULL tables schema-only. Gold frozen before any output. |
| **Reproducibility** | prompt and api-schema hashes, gold hash, standards versions, snapshot, params, seed, request IDs, system_fingerprint, headers, raw output, scorer_version. Byte-stable Turtle with golden tests. |
| **Vendor drift and preview models** | Snapshots recorded. Manual re-runs only. Release pinned by hash. |
| **Unfair local comparison** | Both variants for every model. Large open models run hosted. Local reported as its own class. Split-mode effect measured. |
| **Low statistical power** (n=3, 13 CQs) | Paired permutation/sign tests on shared items. Effect sizes. Option of 5 samples (D8). |
| **Cost overrun / budget race** | §9.3: price and reasoning gates, atomic reservation, per-call caps, monthly ceiling, hard-cap backstops only. |
| **Human-review bottleneck** | Finalists only, 50 items each. α is diagnostic without a second rater. |
| **Scorer scope creep** | MVP/Extended split (SPEC_166/169). OOPS! is opt-in. |
| **Local runner after the VM move** | Laptop CLI posts via the admin API. Workers never claim `local_only` runs. |
| **Thresholds without a literature basis** | Calibrated in the pilot. Reporting lines until then. |
| **Tooling friction** (Java on Windows, NP-hard GED) | Separate eval image. ROBOT jar. Approximate graph metrics with timeouts (SPEC_169). |

## 11. Phase 2 options (not approved; for discussion)

**1. Narrow distillation, after legal review.** Fine-tune a small Apache-licensed model as a **column→ontology binder or classifier**, not a general ontology writer. Candidates: Gemma 4 12B or Qwen3.5-9B. QLoRA on the 12 GB GPU is plausible but unmeasured.

Every source sits in one review state until counsel classifies it against the stated use. **No vendor is excluded or pre-approved by default.**

| Source | Clause (verbatim where verified) | State |
|---|---|---|
| OpenAI | OSA §3.3(e): no models that compete. Its Permitted Exception covers models "primarily intended to categorize, classify, or organize data … if these models are not distributed or made commercially available" (https://cdn.openai.com/osa/openai-services-agreement.pdf). OpenAI also allows fine-tuning through its own fine-tuning service. | Pending legal review |
| Google (Gemini API) | "may not use the Services to develop models that compete" ("compete" is not defined) (https://ai.google.dev/gemini-api/terms) | Pending legal review |
| xAI | Enterprise ToS (2026-08-14): research lenses reported a restriction on training AI systems on Output without an Order Form. **The page returned 403 on re-check; unverified.** | Pending legal review |
| Anthropic | Commercial Terms §D.4: no competing models. The support article lists permitted uses including "Information extraction tools" and "Content categorization systems", and **states no not-distributed condition** (https://support.claude.com/en/articles/12326764) | Pending legal review |
| DeepSeek API | ToU reported to allow distillation; re-verify the wording | Pending legal review |
| Self-run Apache/MIT open weights | Licence only | Pending (licence check) |
| Llama 4 | A distributed model trained on its outputs must carry a "Llama" name prefix | Pending |

**Always excluded:** any CPT- or SNOMED-derived content (AMA policy, UMLS).

**Training data:** the human-approved release, plus outputs only from sources counsel clears.

**2. Second industry: finance against FIBO** (EDM Council; licence believed to be MIT, **verify**). Grounding should be stronger than in healthcare: `core.entity` and `core.identifier` hold CIK 138k, EIN 92k, CRD 25k and LEI 429, and the PE marts exist. The pipeline is reused with a new brief and standards pack (FIBO, GLEIF LEI CC0, SEC XBRL).

**3. Load the NEED sources** (PECOS, DAC affiliations, Hospital Enrollments, All Owners, NPPES Deactivated and Practice Location files, HUD ZIP-county crosswalk, LEIE, Open Payments) and re-score post-NEED.

**4.** Expose `onto_health.*` through the GraphQL layer.

**5.** Re-run the bake-off quarterly, triggered manually.

## 12. Open decisions for the owner

| # | Decision | Options |
|---|---|---|
| D1 | **Roster, access and jurisdiction** | Models per vendor and class (Rule A, Rule B, or custom), entered in `pricing.json` first. Route per model: direct key, OpenRouter or Vertex. Acceptable processing jurisdictions and retention, as one criterion for **all** vendors and hosts. Premium-tier inclusion. The R2b translator and judge families: off-roster, local, or rotating. |
| D2 | **Budget caps** | Pilot / round / month (proposed $20 / $100 / $150-200). Whether `allow_premium` is ever permitted. |
| D3 | **OMOP version** | 5.4 (as requested) or 5.5 (current) |
| D4 | **Data readiness depth** | Registry backfill of ~3k NPIs, or the NPPES V.2 bulk file (several GB). Load PECOS, DAC, Hospital Enrollments and the Deactivation file before or after round 1. |
| D5 | **Grounding headline** | Held-now, post-NEED, or both (proposed: both) |
| D6 | **Local runtime** | LM Studio or llama.cpp (installed), or Ollama |
| D7 | **Human review** | A second rater, which makes α a gate. Or owner only, with α as a diagnostic and a ≥1-week re-blinded re-code of 20%. Finalists only (proposed: top 3-4). |
| D8 | **Samples and sampling** | 3 or 5 samples. Temperature 0.7 (proposed) or vendor default. |
| D9 | **NUCC licence** | **Required before any external release** (commercial use). Request now or at release time. |
| D10 | **Release distribution** | Internal-only or published (affects NUCC, ICD and OMOP terms) |
| D11 | **Timing** | Start specs now; first paid round after the 2026-10-11 VM move (proposed) |
| D12 | **Shared-code change** | Approve the $0-fallback warning at `llm_cost_tracker.py:50`. Optionally add `reasoning_tokens` and `cost_basis` to `llm_usage` (proposed: no, keep them in `onto.run`). |
| D13 | **Entity link design** | SPEC_168 design A (NPI native-key attach) or B (weak-match review) |
| D14 | **Extended eval** | Whether, and when, to fund SPEC_169 (6-10 d) |

---

## Appendix A: Full output JSON Schema (`NexdataOntologyRun`, hc-onto-v1.0)

```json
{
 "$schema":"https://json-schema.org/draft/2020-12/schema",
 "title":"NexdataOntologyRun","type":"object","additionalProperties":false,
 "required":["meta","classes","properties","relations","vocabularies","cq_coverage","open_questions"],
 "properties":{
  "meta":{"type":"object","additionalProperties":false,
    "required":["brief_version","domain","generation_mode","model_self_reported","notes"],
    "properties":{"brief_version":{"type":"string"},"domain":{"type":"string","enum":["healthcare_provider_patient"]},
      "generation_mode":{"type":"string","enum":["single","split"]},
      "model_self_reported":{"type":["string","null"]},"notes":{"type":["string","null"]}}},
  "classes":{"type":"array","maxItems":40,"items":{"type":"object","additionalProperties":false,
    "required":["id","label","definition","parent","layer","is_abstract","phi_risk","mappings","bindings","evidence","confidence"],
    "properties":{
      "id":{"type":"string","pattern":"^[A-Z][A-Za-z0-9]{1,63}$"},
      "label":{"type":"string"},"definition":{"type":"string"},
      "parent":{"type":["string","null"]},
      "layer":{"type":"string","enum":["provider","organization","location","coverage","utilization","ownership","quality_sanction","patient_schema","reference"]},
      "is_abstract":{"type":"boolean"},
      "phi_risk":{"type":"string","enum":["none","quasi_identifier","phi_if_populated"]},
      "mappings":{"$ref":"#/$defs/mappings"},
      "bindings":{"$ref":"#/$defs/bindings"},
      "evidence":{"$ref":"#/$defs/evidence"},
      "confidence":{"type":"string","enum":["high","medium","low"]}}}},
  "properties":{"type":"array","maxItems":120,"items":{"type":"object","additionalProperties":false,
    "required":["id","label","definition","kind","domain","range","cardinality","value_set","is_identifier","mappings","bindings","evidence","confidence"],
    "properties":{
      "id":{"type":"string","pattern":"^[a-z][A-Za-z0-9]{1,63}$"},
      "label":{"type":"string"},"definition":{"type":"string"},
      "kind":{"type":"string","enum":["datatype","object"]},
      "domain":{"type":"string"},
      "range":{"type":"string"},
      "cardinality":{"type":"string","enum":["0..1","1..1","0..*","1..*"]},
      "value_set":{"type":["string","null"]},
      "is_identifier":{"type":"boolean"},
      "mappings":{"$ref":"#/$defs/mappings"},"bindings":{"$ref":"#/$defs/bindings"},
      "evidence":{"$ref":"#/$defs/evidence"},"confidence":{"type":"string","enum":["high","medium","low"]}}}},
  "relations":{"type":"array","maxItems":30,"items":{"type":"object","additionalProperties":false,
    "required":["id","label","definition","subject","object","characteristics","temporal","inverse_of","mappings","evidence","confidence"],
    "properties":{"id":{"type":"string","pattern":"^[a-z][A-Za-z0-9]{1,63}$"},"label":{"type":"string"},"definition":{"type":"string"},
      "subject":{"type":"string"},"object":{"type":"string"},
      "characteristics":{"type":"array","items":{"type":"string","enum":["functional","inverse_functional","transitive","symmetric","asymmetric","irreflexive"]}},
      "temporal":{"type":"boolean"},
      "inverse_of":{"type":["string","null"]},
      "mappings":{"$ref":"#/$defs/mappings"},"evidence":{"$ref":"#/$defs/evidence"},"confidence":{"type":"string","enum":["high","medium","low"]}}}},
  "vocabularies":{"type":"array","items":{"type":"object","additionalProperties":false,
    "required":["id","name","steward","license_status","used_by","example_codes_from_bundle"],
    "properties":{"id":{"type":"string","enum":["NUCC_TAXONOMY","ICD10CM","HCPCS_L2","CPT","SNOMED_CT","CMS_POS","CMS_HOSPITAL_TYPE","CMS_OWNERSHIP","NPPES_ENTITY_TYPE","US_STATE","FIPS_COUNTY","RUCA","LOCAL_PROPOSED"]},
      "name":{"type":"string"},"steward":{"type":"string"},
      "license_status":{"type":"string","enum":["public","free_with_terms","licensed_do_not_enumerate","local"]},
      "used_by":{"type":"array","items":{"type":"string"}},
      "example_codes_from_bundle":{"type":"array","maxItems":5,"items":{"type":"string"}}}}},
  "cq_coverage":{"type":"array","items":{"type":"object","additionalProperties":false,
    "required":["cq_id","answerable","path","missing"],
    "properties":{"cq_id":{"type":"string","pattern":"^CQ[0-9]{2}$"},
      "answerable":{"type":"string","enum":["with_bundle_data","needs_new_data","schema_only","not_modeled"]},
      "path":{"type":"array","items":{"type":"string"}},
      "missing":{"type":["string","null"]}}}},
  "open_questions":{"type":"array","maxItems":10,"items":{"type":"string"}}
 },
 "$defs":{
  "mappings":{"type":"array","maxItems":4,"items":{"type":"object","additionalProperties":false,
    "required":["standard","target","match"],
    "properties":{"standard":{"type":"string","enum":["FHIR_R4","US_CORE","PLAN_NET","OMOP_CDM_5_4"]},
      "target":{"type":"string"},
      "match":{"type":"string","enum":["exact","close","broad","narrow","related"]}}}},
  "bindings":{"type":"array","maxItems":6,"items":{"type":"object","additionalProperties":false,
    "required":["table","column","transform"],
    "properties":{"table":{"type":"string"},"column":{"type":"string"},
      "transform":{"type":"string","enum":["none","trim","upper","lower","lpad_10","zip5","cast_date","cast_int","cast_numeric","yn_to_bool"]}}}},
  "evidence":{"type":"object","additionalProperties":false,"required":["basis","bundle_refs"],
    "properties":{"basis":{"type":"string","enum":["bundle","public_standard","prior_knowledge","inferred"]},
      "bundle_refs":{"type":"array","items":{"type":"string"}}}}
 }
}
```

**Field conventions:**
- `range` is a class ID for object properties. For datatype properties it is one of `xsd:string|date|dateTime|decimal|integer|boolean`, or `code`. With `code`, `value_set` must be a vocabulary ID.
- FHIR targets are written `Resource` or `Resource.element`. OMOP targets are written `TABLE` or `TABLE.field`.
- `table` and `column` in bindings must be verbatim from bundle §B. `transform` names one of our own implementations.
- `temporal=true` means the relation needs valid_from and valid_to.
- **In split mode**, call 1 returns `properties: []` and call 2 returns an object whose only populated array is `properties`. The runner merges the two before validation.
- **Validate against each vendor API before the pilot**: limits on `pattern`, `$ref`, enum size and nesting depth. Any inlined variant is hashed as `api_schema_sha256`.

## Appendix B: Competency questions and status (labels not shown to models)

| CQ | Question | Status (2026-10-04) | Source if NEED |
|---|---|---|---|
| 01 | Given an NPI: individual or organization, active or deactivated, enumeration and last-update dates, DBA name | **PARTIAL.** All rows are `status='A'`, so deactivation is NEED. Other/former names are NEED. | NPPES Deactivated NPI report; NPPES V.2 |
| 02 | A practitioner's credentials, primary NUCC specialty, and licence state for that taxonomy | HOLD (primary only) | n/a |
| 03 | All taxonomies a provider holds, and which is primary | NEED | NPPES V.2 (15 slots) |
| 04 | NUCC grouping, classification and specialization a taxonomy code rolls up to | NEED → **HOLD after SPEC_163** | NUCC CSV |
| 05 | Individual NPIs sharing a practice address or phone with an organization NPI | HOLD (derived, low confidence) | n/a |
| 06 | Group practices a practitioner reassigns billing to (org PAC ID) | NEED | DAC national file / PECOS |
| 07 | Hospitals a practitioner is affiliated with | NEED | DAC facility affiliations `27ea-46a8` |
| 08 | Whether an organization NPI is a subpart, and its parent | PARTIAL | NPPES V.2 parent LBN |
| 09 | Organization NPIs belonging to the same legal entity (EIN/TIN) or health system | NEED | PECOS; AHRQ Compendium |
| 10 | CCN ↔ NPI ↔ Medicare enrollment ID crosswalk for a hospital | NEED | Hospital Enrollments |
| 11 | Where a provider practises (address, ZIP, state, county FIPS, RUCA rural flag) | **PARTIAL.** Address, ZIP and state are HOLD. County/RUCA only for the 3,216 utilization NPIs. **No HUD crosswalk table exists.** | HUD ZIP-county crosswalk |
| 12 | Providers of taxonomy X per ZIP or county, and per capita | PARTIAL (biased by the sample) | NPPES full + Census |
| 13 | A provider's secondary practice locations | NEED | NPPES Practice Location file |
| 14 | A hospital's type, emergency capability and county | HOLD | n/a |
| 15 | Whether a provider takes Medicare assignment | HOLD (sampled NPIs) | n/a |
| 16 | Commercial or MA networks that include a provider, and negotiated rates | NEED | TiC MRFs; Plan-Net APIs (TB scale) |
| 17 | A provider's enrollment in a state Medicaid program | NEED | State files |
| 18 | For an NPI: HCPCS billed, volumes, average allowed and paid | PARTIAL (sample; duplicates and year fixed in SPEC_162) | n/a |
| 19 | Top HCPCS by place of service for a specialty in a state | PARTIAL | n/a |
| 20 | A hospital's beds, total costs, net income | **NEED** (all data columns NULL) | HCRIS reload |
| 21 | Part D prescribing by provider | NEED | Part D Prescribers by Provider |
| 22 | Owners of a hospital, SNF, HHA or hospice (%, PE/REIT/holding flags) | NEED (coarse category HOLD) | CMS All Owners |
| 23 | Practices that are PE portfolio companies, and the NPI/location footprint per platform | PARTIAL | NPI↔portfolio link (SPEC_168) |
| 24 | When a provider's owner or affiliation changed | NEED | CMS CHOW files, snapshots |
| 25 | A hospital's overall and domain star ratings | HOLD | n/a |
| 26 | Whether a provider or entity is excluded from federal programs | NEED | OIG LEIE; SAM exclusions |
| 27 | Industry payments a provider received | NEED | Open Payments |
| 28 | How patient demographics are represented (birth-year band, sex, race/ethnicity, ZIP3), aligned with FHIR Patient and OMOP PERSON | SCHEMA | n/a |
| 29 | How an Encounter links practitioner, organization, location, ICD-10-CM diagnosis and HCPCS service | SCHEMA | n/a |
| 30 | How coverage and plan enrollment periods are represented (FHIR Coverage / OMOP PAYER_PLAN_PERIOD) | SCHEMA | n/a |

**Tally:** today HOLD 5 / PARTIAL 7 / NEED 15 / SCHEMA 3. After SPEC_163: HOLD 6 / PARTIAL 7 / NEED 14 / SCHEMA 3. **13 groundable.** SPEC_162 re-verifies each label with a SQL probe.

## Appendix C: The brief (identical for every model; versioned and hashed)

**System prompt**
```
You are an ontology engineer producing a schema-level ontology for a US healthcare data business.
Output: ONE JSON object that validates against the provided JSON Schema. No prose, no markdown, no comments.
Rules:
1. Ground, don't invent. Reuse public standards (HL7 FHIR R4 / US Core / Da Vinci Plan-Net, OMOP CDM v5.4, NUCC taxonomy, ICD-10-CM, HCPCS Level II, CMS place-of-service). Only emit a mapping if you are confident the target resource/element/table/field exists in that standard version; otherwise emit no mapping. Wrong mappings are scored as errors; missing mappings are not.
2. Bindings: only use table and column names that appear verbatim in CONTEXT section B. Never guess a column. "transform" must be one of the listed names; never write SQL. If a concept has no column, leave bindings empty and set evidence.basis accordingly.
3. Codes: never output CPT or SNOMED CT codes or descriptions (licensed). You may name those vocabularies with license_status "licensed_do_not_enumerate". Example codes may only be copied from CONTEXT section C.
4. Patient-side concepts (demographics, encounters, coverage) are schema-only: model them in layer "patient_schema", mark phi_risk, and give them NO bindings - and give NO bindings to any property or relation whose domain, subject or object is a patient_schema class. Do not include identifiers that would identify a person beyond the de-identified fields listed in the brief.
5. Prefer fewer, well-defined classes over many thin ones. Definitions ≤40 words, genus-differentia form ("A <parent> that ..."). Use time-bounded relations (temporal=true) for affiliations, ownership and enrollment.
6. Every competency question CQ01–CQ30 must appear once in cq_coverage. If the model cannot answer it, say not_modeled. If it needs data not in CONTEXT B, say needs_new_data and name the missing concept in "missing" (not a specific vendor product).
7. Confidence: high = taken from a standard or the bundle; medium = standard practice; low = your own design choice. evidence.bundle_refs must cite section ids from CONTEXT.
8. Respect caps: ≤40 classes, ≤120 properties, ≤30 relations, ≤10 open_questions. IDs must be unique across classes, properties and relations. If you would exceed a cap, merge or drop the lowest-value concepts.
```

**User prompt template**
```
brief_version: hc-onto-v1.0  variant: {full|lite}  generation_mode: {single|split:1|split:2}  domain: healthcare_provider_patient
TASK: Build the provider/organization/location/coverage/utilization/ownership/quality ontology plus a schema-only patient layer, such that CQ01–CQ30 are answerable by traversing it, and the CMS/NPPES columns in section B bind to it with NPI (and CCN for facilities) as join keys.
[split:1] Return meta, classes, relations, vocabularies, cq_coverage, open_questions; return properties as [].
[split:2] Given PART 1 below, return only properties (other arrays empty, meta copied).
CONTEXT
A. Competency questions CQ01–CQ30 (text only, no status labels)
B. Data dictionary: B1.nppes_providers, B2.cms_medicare_utilization, B3.cms_hospitals, B4.cms_hospital_cost_reports,
   B5.core_identifier_types (npi, ein, lei, uei, zip5, fips_county)
   per column: table | column | type | description | example (non-PII) | data_state
C. Reference excerpts
   C1 NUCC: ~30 rows (code | grouping | classification | specialization) covering bundle taxonomies
   C2 HCPCS Level II: ~20 codes present in B2; CPT rows -> "[CPT code - licensed, omitted]"
   C3 CMS POS F/O; NPPES entity type 1/2; status A/D; distinct hospital_type & ownership values from B3
   C4 ICD-10-CM: 5 chapter-level examples
   C5 FHIR R4 resources: Patient, Practitioner, PractitionerRole, Organization, OrganizationAffiliation, Location,
      HealthcareService, Endpoint, InsurancePlan, Coverage, Encounter, Claim, ExplanationOfBenefit (name + 1-line purpose)
   C6 OMOP CDM tables: PERSON, PROVIDER, CARE_SITE, LOCATION, VISIT_OCCURRENCE, VISIT_DETAIL, PAYER_PLAN_PERIOD, COST,
      CONDITION_OCCURRENCE, PROCEDURE_OCCURRENCE (name + key fields)
D. JSON Schema (Appendix A verbatim, byte-identical for all vendors)
[split:2] PART 1: <call-1 JSON>
Return only the JSON object.
```

**Lite variant:** drop the B examples and trim C1/C2.

**Run protocol:**
- Same `prompt_sha256` for every model in a given mode. Any `api_schema_sha256` difference is recorded.
- Native structured-output or grammar mode. Local models use llama.cpp `json_schema` or LM Studio structured output.
- **Temperature 0.7 where accepted, otherwise the vendor default.** The actual settings and seed are recorded.
- 3 samples per model per variant.
- Explicit `max_output_tokens` (64k, or 128k with reasoning) and an explicit reasoning setting.
- No retry on timeout.
- Every call is recorded through `llm_cost_tracker.record(provider=..., cost_usd=..., source="ontology:<brief>")` and in `onto.run`, including reasoning tokens, request IDs and the fingerprint.

## Appendix D: Deterministic Turtle converter (sketch, for SPEC_166)

Preconditions:
- §5.4 checks 1-7 have passed.
- In particular, referential integrity has run across the shared ID space of classes ∪ properties ∪ relations, so there are no dangling `parent`, domain or range references and no ID collisions.

```python
FHIR="http://hl7.org/fhir/"; OMOP="urn:omop:cdm54:"   # cdm55 if D3 picks 5.5; OMOP has no official IRIs
XSD={"xsd:string","xsd:date","xsd:dateTime","xsd:decimal","xsd:integer","xsd:boolean"}
CARD={"1..1":[("owl:qualifiedCardinality",1)],"0..1":[("owl:maxQualifiedCardinality",1)],
      "1..*":[("owl:minQualifiedCardinality",1)],"0..*":[]}
SKOS={"exact":"skos:exactMatch","close":"skos:closeMatch","broad":"skos:broadMatch",
      "narrow":"skos:narrowMatch","related":"skos:relatedMatch"}
CH={"functional":"owl:FunctionalProperty","inverse_functional":"owl:InverseFunctionalProperty",
    "transitive":"owl:TransitiveProperty","symmetric":"owl:SymmetricProperty",
    "asymmetric":"owl:AsymmetricProperty","irreflexive":"owl:IrreflexiveProperty"}
_ESC={'\\':'\\\\','"':'\\"','\n':'\\n','\r':'\\r','\t':'\\t','\b':'\\b','\f':'\\f'}
def lit(s):
    out=[]
    for ch in s:
        if ch in _ESC: out.append(_ESC[ch])
        elif ord(ch)<0x20 or ord(ch)==0x7f: out.append('\\u%04X'%ord(ch))   # all other control chars
        else: out.append(ch)
    return '"'+''.join(out)+'"'
def tgt(m):
    if m["standard"] in ("FHIR_R4","US_CORE","PLAN_NET"):
        return f"<{FHIR}{m['target'].replace('.', '#',1)}>"
    return f"<{OMOP}{m['target']}>"
def to_turtle(run, run_id):
    out=["@prefix : <urn:nexdata:onto:hc:%s#> ."%run_id, "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
         "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .","@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
         "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .","@prefix nx: <urn:nexdata:meta#> .","",
         "<urn:nexdata:onto:hc:%s> a owl:Ontology ."%run_id,""]
    def common(x):
        t=[f"rdfs:label {lit(x['label'])}", f"skos:definition {lit(x['definition'])}",
           f"nx:confidence {lit(x['confidence'])}", f"nx:evidenceBasis {lit(x['evidence']['basis'])}"]
        t+=[f"{SKOS[m['match']]} {tgt(m)}" for m in sorted(x.get("mappings",[]),key=lambda m:(m['standard'],m['target'],m['match']))]
        t+=[f"nx:binding {lit(b['table']+'.'+b['column']+'|'+b['transform'])}"
            for b in sorted(x.get("bindings",[]),key=lambda b:(b['table'],b['column'],b['transform']))]
        return t
    for c in sorted(run["classes"],key=lambda c:c["id"]):
        t=["a owl:Class"]+([f"rdfs:subClassOf :{c['parent']}"] if c["parent"] else [])+common(c)
        t.append(f"nx:layer {lit(c['layer'])}")
        out.append(f":{c['id']} "+" ;\n    ".join(t)+" .\n")
    for p in sorted(run["properties"],key=lambda p:p["id"]):
        dt=p["kind"]=="datatype"
        rng="xsd:string" if p["range"]=="code" else (p["range"] if p["range"] in XSD else f":{p['range']}")
        t=[f"a {'owl:DatatypeProperty' if dt else 'owl:ObjectProperty'}", f"rdfs:domain :{p['domain']}", f"rdfs:range {rng}"]
        if p["value_set"]: t.append(f"nx:valueSet {lit(p['value_set'])}")   # code ranges keep their vocabulary here
        if p["is_identifier"]: t.append("nx:isIdentifier true")
        out.append(f":{p['id']} "+" ;\n    ".join(t+common(p))+" .\n")
        for pred,n in CARD[p["cardinality"]]:
            q="owl:onDataRange" if dt else "owl:onClass"
            out.append(f":{p['domain']} rdfs:subClassOf [ a owl:Restriction ; owl:onProperty :{p['id']} ; "
                       f"{pred} \"{n}\"^^xsd:nonNegativeInteger ; {q} {rng} ] .\n")
    for r in sorted(run["relations"],key=lambda r:r["id"]):
        t=["a owl:ObjectProperty"]+[f"a {CH[c]}" for c in sorted(set(r["characteristics"]))]
        t+=[f"rdfs:domain :{r['subject']}", f"rdfs:range :{r['object']}"]
        if r["inverse_of"]: t.append(f"owl:inverseOf :{r['inverse_of']}")
        if r["temporal"]: t.append("nx:temporal true")
        out.append(f":{r['id']} "+" ;\n    ".join(t+common(r))+" .\n")
    return "\n".join(out)
```

**Tests:**
- **Golden-file tests** (byte-identical output for fixed inputs, including strings that contain `\r`, `\t` and other control characters).
- An `rdflib` round-trip parse.
- A fallback, if the golden-file tests ever fail: serialise with rdflib `Literal`s to sorted N-Triples, then convert.

**Validation chain:**
- **MVP:**
  1. `rdflib` parse.
  2. ROBOT `reason` with ELK.
  3. ROBOT `report`.
- **Extended:** HermiT on a sample, and pySHACL shapes (for example, every provider/organization-layer class has an `isIdentifier` property bound to `npi`).

**Cross-model alignment:**
1. Pivot on shared `exact` mapping targets.
2. Label and definition similarity (exact plus rapidfuzz in MVP; embeddings in Extended).
3. A human reviews the rest.

## Appendix E: Sources

**Repo (file:line):**
- `app/core/llm_cost_tracker.py:40-50,70-128` (`record()` provider/cost_usd overrides at `:79-80`, `:96-100`)
- `app/core/models.py:3683-3717` (`LLMUsage`, `cost_usd` at `:3698`)
- `app/agentic/llm_client.py:102,119-160,195-266`
- `app/services/eval_scorer.py:815-844,1011`
- `app/services/eval_runner.py:281-358`
- `app/catalog/rights.py:287-291`
- `app/catalog/rights_reviewed.py:49-51`
- `app/catalog/identifiers.py:48`
- `app/catalog/spec.py:35-40`
- `app/catalog/columns.py:135`
- `app/catalog/datasets.py:885-926,494-505,1442-1448,2197-2237`
- `app/catalog/columns.generated.json:5-60,224-255,3362-3390,20170-20178`
- `app/sources/nppes/metadata.py:101-133,248-282`
- `app/sources/nppes/client.py:31`
- `app/sources/cms/metadata.py:18-125,242-244`
- `app/sources/cms_hospitals/metadata.py:84-107`
- `app/sources/cms_hospitals/client.py:33`
- `app/entities/feeds.py:90,92,294,318,360-361,368,492-493`
- `app/entities/resolve_core.py:124-130,208`
- `alembic/versions/0008_entity_master.py:51`
- `0012_access_lockdown.py`
- `0016_entity_weak_match.py`
- `0019_gleif.py`
- `app/core/config.py:693-696,961-963`
- `app/catalog/evidence/verification_2026-09-25.json:1636`
- Commit `a56510a`
- Live queries via `nexdata-api-1`, 2026-10-04: nppes status distribution; utilization distinct-key and ingestion-date counts; table column counts; HUD table list.

**Standards:**
- https://hl7.org/fhir/R4/license.html
- https://hl7.org/fhir/R4/downloads.html
- https://hl7.org/fhir/us/core/downloads.html
- http://hl7.org/fhir/us/davinci-pdex-plan-net/
- https://ohdsi.github.io/CommonDataModel/
- https://github.com/OHDSI/CommonDataModel
- https://raw.githubusercontent.com/OHDSI/CommonDataModel/main/DESCRIPTION
- https://www.nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40/csv-mainmenu-57
- https://download.cms.gov/nppes/NPI_Files.html
- https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment
- https://data.cms.gov/provider-data/dataset/mj5m-pzi6
- https://data.cms.gov/provider-data/dataset/27ea-46a8
- https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments
- https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-all-owners
- https://www.cms.gov/medicare/coding-billing/icd-10-codes
- https://cdn.who.int/media/docs/default-source/publishing-policies/copyright/who-faq-licensing-icd-10.pdf
- https://www.nlm.nih.gov/healthit/snomedct/snomed_licensing.html
- https://www.nlm.nih.gov/research/umls/rxnorm/faq.html
- https://loinc.org/get-started/getting-loinc/
- https://www.ama-assn.org/practice-management/cpt/licensing-cpt-ai-faqs
- https://oig.hhs.gov/exclusions/exclusions_list.asp
- https://openpaymentsdata.cms.gov/

**Models, prices and terms:**
- https://developers.openai.com/api/docs/pricing
- https://cdn.openai.com/osa/openai-services-agreement.pdf
- https://ai.google.dev/gemini-api/docs/pricing
- https://ai.google.dev/gemini-api/terms
- https://ai.google.dev/gemini-api/docs/structured-output
- https://ai.google.dev/gemini-api/docs/openai
- https://docs.x.ai/docs/models
- https://docs.x.ai/docs/guides/structured-outputs
- https://x.ai/legal/terms-of-service-enterprise (403 on re-check)
- https://platform.claude.com/docs/en/about-claude/pricing
- https://www.anthropic.com/legal/commercial-terms
- https://support.claude.com/en/articles/12326764
- https://api-docs.deepseek.com/quick_start/pricing
- https://cdn.deepseek.com/policies/en-US/deepseek-terms-of-use.html
- https://cdn.deepseek.com/policies/en-US/deepseek-privacy-policy.html
- https://huggingface.co/Qwen/Qwen3.8-2.4T-A95B/blob/main/LICENSE
- https://huggingface.co/zai-org/GLM-5.3/blob/main/LICENSE
- https://huggingface.co/moonshotai/Kimi-K3/blob/main/LICENSE
- https://huggingface.co/google/gemma-4-31b-it
- https://dev.meta.ai/llama/llama4/license/
- https://docs.mistral.ai/getting-started/models/
- https://openrouter.ai/api/v1/models
- https://openrouter.ai/docs/features/structured-outputs
- https://cloud.google.com/model-garden

**Evaluation:**
- https://arxiv.org/abs/2410.23584
- https://github.com/andylolu2/ollm
- https://krr-oxford.github.io/DeepOnto/bio-ml/
- https://arxiv.org/abs/2307.03067
- https://arxiv.org/abs/2507.14552
- https://arxiv.org/abs/2609.26029v1
- https://arxiv.org/abs/2503.05388
- https://arxiv.org/abs/2606.24619
- https://arxiv.org/pdf/2503.21813
- https://arxiv.org/abs/2307.16648
- https://arxiv.org/abs/2409.10146
- https://www.sciencedirect.com/science/article/pii/S0306457324004011
- https://robot.obolibrary.org/reason
- https://github.com/RDFLib/pySHACL
- https://owlready2.readthedocs.io/en/latest/reasoning.html
- https://oeg.fi.upm.es/index.php/en/technologies/292-oops/index.html
- https://github.com/sciknoworg/OntoLearner/

**Critic notes where v2 departs from a suggestion:**
- **`reasoning_tokens` / `cost_basis`:** kept in `onto.run` instead of migrating the shared `llm_usage` table (critics disagreed; D12 can revisit).
- **Output-size measurement:** uses a scaled hand-written 10-class fragment plus the pilot outputs, not a full hand-written reference ontology, because that would take days of work for one number.
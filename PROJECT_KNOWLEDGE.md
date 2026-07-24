# MDM Medallion Architecture Demo — Project Knowledge

Portfolio project for Balram's job search (targeting Senior MDM Architect / Director-level
data governance roles). Working demo, not production software — built to be a genuine
interview talking piece with runnable code behind it.

## Purpose & positioning

Demonstrates hands-on MDM architecture skills: metadata-driven data quality, match/merge
and survivorship, data stewardship workflows with AI assistance, lineage/impact analysis,
and access-controlled data products — the kinds of capabilities a Senior MDM Architect or
data governance Director is expected to have designed firsthand, not just managed.

## Architecture

Medallion architecture (bronze/silver/gold) with two synthetic heterogeneous source
systems (a CRM and an ERP), so schema standardization is a real problem, not a formality.

- **Bronze** — raw CRM + ERP extracts landed untouched, tagged with source_system/source_record_id.
- **Silver** — canonical, validated records only. Cleansing/standardization/validation is
  **metadata-driven**: rules live in `bus_rules.column_rules` (rule_id, source column,
  rule_type, severity) — a DB-native table maintained via the Data Governance > Rules
  Configuration screen's maker-checker workflow, not a dbt seed. Records failing validation
  route to an exception queue instead of silver.
- **Gold (Data Hub)** — hybrid match/merge across sources, and **fully metadata-driven**:
  tier definitions/thresholds live in `bus_rules.matching_thresholds` and the
  fields/columns each tier operates on live in `bus_rules.matching_rules` (a
  child table keyed by tier_id) — nothing about *what* matches is hardcoded in pipeline
  code, mirroring the same pattern `bus_rules.column_rules` already established for silver
  validation. Today's configuration has two tiers: tier 1 is deterministic exact match on
  normalized email OR phone (confidence = that tier's `auto_merge_threshold`, 1.00); tier 2
  is an embedding-similarity fuzzy match (TF-IDF character n-gram cosine similarity over
  name/address text, blocked by state_code) that catches near-duplicates tier 1 misses.
  High-confidence fuzzy matches auto-merge with their similarity score as confidence;
  borderline ones surface in a **Match Review queue** for a data steward to confirm or
  reject rather than auto-merging. A third, non-tier row (`is_match_tier=false`)
  holds the 0.50 "provisional" baseline confidence assigned to a single-source golden
  record with no corroborating match at all — read by both the batch gold layer and
  real-time reprocessing, so they can't disagree on it. Computed in
  `scripts/generate_matches.py`, which runs as a Python step between silver and gold
  (dbt-duckdb can't do the embedding math in SQL) and writes to a `gold_prep` schema that
  the gold dbt models consume as a source — same "Python loads, dbt treats it as a source"
  pattern also used for bronze and for `ref`/`bus_rules` themselves. **Survivorship is
  attribute-level**, not record-level: each gold column is independently won by whichever
  contributing source best satisfies that column's own `bus_rules.survivorship_rules` rule
  (`most_common` / `most_complete` / `oldest` / `newest` / `pattern_match`), with
  `source_modified_date` desc (then CRM preferred) as the universal, non-configurable
  tie-break whenever a rule doesn't produce a clean winner — see
  `dbt_project/models/gold/gold_survivorship_winners.sql`. A **crosswalk table**
  (`gold_crosswalk`) preserves the relationship between every golden record and its
  contributing source records, with a graduated match confidence score (not hardcoded
  literals) plus a `winning_columns` list per source showing exactly which gold columns
  that source won (`is_survivor_record` is now simply "won at least one column").

## Tech stack

- **dbt-core + DuckDB** for the batch pipeline — genuinely runs locally, zero cloud cost,
  no Databricks account needed. This was a deliberate choice over Talend (which the user
  has production experience with) because Talend's MDM Server product and Open Studio are
  both end-of-life/discontinued as of 2024, and Databricks/dbt-style lakehouse patterns are
  what current job postings ask for.
- **scikit-learn** (TF-IDF + cosine similarity) for the fuzzy-matching tier — chosen over
  a neural sentence-embedding model (e.g. sentence-transformers) to keep the demo's setup
  fast and fully offline (no model download, no torch, no GPU); a defensible "real
  embedding-similarity technique" story for an interview without the install weight.
- **Faker** generates the ~100-row-per-source synthetic population (see Key data model
  notes below for how single-source, exact-duplicate, and fuzzy-duplicate cases are seeded).
- **FastAPI** backend for all apps and the REST API, with bcrypt-hashed password auth
  (from-scratch bearer-token sessions, 8-hour expiry).
- Plain HTML/JS frontends (no build step, no Node.js needed) for both apps.
- **Windows-first**: every file path is computed via `pathlib` relative to file location —
  no hardcoded absolute paths anywhere. The user runs this on Windows (PowerShell), the
  dev/test environment used to build it is Linux, so this portability was hard-won and
  tested by literally extracting into a space-containing path (`MDM Pipeline`) and running
  the full pipeline before shipping each zip.

## Applications

1. **Data Stewardship console** (`/app/`) — three tabs.
   - **Exception Queue** — reviews the exception queue, gets an
     AI-assisted remediation suggestion (real Claude API call via `ANTHROPIC_API_KEY`, with a
     transparent heuristic fallback when no key is set — every suggestion response includes
     `source: "ai"` or `"heuristic_fallback"`), and resolves/rejects corrections. **Resolving
     or rejecting now submits into the maker-checker workflow** (`stewardship_remediation`,
     1 Data Owner approver, different from the steward) instead of applying immediately —
     see the Maker-Checker Workflow Engine section below. Only once approved does
     resolving actually **trigger real-time reprocessing**: the corrected record is upserted
     into silver, matched against the current gold set using the same tier-1 exact_match_field
     rules from `matching_rules.csv` the batch pipeline reads (exact tier only, see known
     simplifications), and survivorship is re-run to either update the matched golden
     record or create a new one; a rejected approval returns the exception to the open queue.
     This mirrors the batch dbt logic so both paths stay consistent, though golden ID numbering
     can diverge between them (documented limitation).
   - **Match Review** — borderline fuzzy-match candidates from the gold layer's
     embedding-similarity tier (similarity between the review and auto-merge thresholds),
     shown side by side, for the steward to confirm or reject. Confirm/reject now also
     submits into a maker-checker workflow (`match_review_confirmation`, 2 sequential
     levels: Data Owner, then Admin) rather than recording immediately. Once approved,
     confirm/reject still does NOT reprocess in real time (re-clustering is a global
     union-find recompute, not a local update) — it writes to
     `stewardship.match_review_overrides` and takes effect on the next
     `python scripts/generate_matches.py && dbt run --select gold.*`.
   - **Approvals** — lists workflow instances awaiting this user's decision ("Awaiting my
     decision") or previously submitted by them ("My submissions"), for any of the
     workflow types whose current step matches their role. Backed by the same generic
     `GET /api/v1/workflows/pending` / `GET /api/v1/workflows/mine` /
     `POST /api/v1/workflows/{instance_id}/decide` endpoints the Portal's Approvals view
     uses.

2. **MDM Data Hub Portal** (`/portal/`) — login-gated, SSO'd with the Stewardship
   app. Roles are `admin` / `dataSteward` / `dataOwner` / `businessUser`; gold
   access is `read_write` / `read` / `none`. `admin` is a distinct governance
   function (user administration only) and is deliberately **not** a superset of
   stewardship rights — only `dataSteward`/`dataOwner` can reach the Stewardship
   console or its API endpoints (enforced both server-side in `api/auth.py` and
   independently in the Stewardship frontend). Has:
   - **Browse Customers** — searchable/paginated gold-layer browser with live (debounced)
     search-as-you-type, click-through detail view with the source crosswalk, inline edit
     if the user's account has `read_write` gold access, and an **Audit Trail** button
     opening a read-only history panel for that golden record (see below). **An inline
     edit now submits into the `gold_record_edit` maker-checker workflow** (1 level, a
     quorum of 2 *different* Data Owners) instead of applying immediately — the response
     is `{status: "pending_approval", workflow_instance_id, steps}` rather than the
     updated record, and the audit-trail entry (once it does apply) still attributes the
     edit to the maker, not the approvers.
   - **Approvals** — a nav item visible to every signed-in role (not just
     dataSteward/dataOwner), since Admins — who approve Match Review's second level and
     User Administration changes — don't have access to the Stewardship Console. Same
     "awaiting my decision" / "my submissions" split as the console's Approvals tab.
   - **Audit Trail** — append-only history of every golden record: creation, every edit
     (manual portal edit, steward real-time reprocessing, or batch pipeline recompute),
     and logical deletes (a golden_id no longer produced by a pipeline rebuild). Opens
     from a button on the customer detail modal as a separate panel showing the record's
     key identifiers up top and a newest-first timeline below, grouped into one entry per
     logical change with expandable old→new field diffs. Viewable by anyone with gold
     `read` or `read_write` access (`GET /api/v1/customers/{golden_id}/audit-trail`); there
     is no write/delete endpoint for it by design — no code path anywhere updates or
     deletes an audit row once written, which is the actual enforcement mechanism (DuckDB
     has no per-table grants to lean on here, same app-level-only security model as
     `gold_access` itself). See `api/audit.py` and Key data model notes below.
   - **Data Governance** (nav dropdown, four items; the first two are role-gated, the
     last two are visible to every signed-in user since they're read-only outside
     dataSteward/dataOwner — the dropdown itself is therefore always shown now):
     - **Data Stewardship** — visible only to `dataSteward`/`dataOwner` (same role gate
       as the console). Opens `/app/` via `window.open('/app/', 'mdmStewardshipTab')`;
       clicking again while that named tab is still open re-focuses it instead of
       spawning a duplicate — the "launch if not open, else shift control to it"
       behavior, implemented with the browser's native named-window targeting rather
       than any cross-tab messaging. Only recognizes tabs opened via this button.
     - **Lineage and Impact Analysis** — visible only with gold `read`/`read_write`
       access; this gates the nav item only — `/api/v1/lineage/*` stays open to any
       authenticated user regardless of `gold_access` (deliberate, pre-existing:
       pipeline metadata, not gold customer data). A full pannable/zoomable **network
       diagram** of the entire lineage metadata graph (bronze→silver→gold→stewardship).
       Click any node to highlight its upstream lineage (amber) and downstream impact
       (green) directly on the graph, with a side detail panel (description, direct
       in/out rules, upstream/downstream counts). Clicking a gold node adds a Golden ID
       lookup tool.
     - **Reference Data Maintenance** — Country Codes and State Codes (`ref.ref_country_codes`
       / `ref.ref_state_codes`, DB-native, each with a human-readable name/label column and
       an `is_active` flag; no hard delete, same "deactivate" convention as everywhere else
       in this app). Visible to everyone; create/edit/deactivate forms are only shown to
       `dataSteward`/`dataOwner` (everyone else gets a read-only table) and submit into the
       `reference_data_change` maker-checker workflow (1 level, 1 Data Owner). dbt's silver
       staging models (`stg_crm_customers.sql`, `stg_erp_customers.sql`) read these two
       tables as a `{{ source('ref', ...) }}` instead of a seed, so an approved edit here
       takes effect on the next pipeline rebuild.
     - **Rules Configuration** — Column Rules, Matching Rules (plus their tier definitions,
       "Matching Tiers"), and the new **Survivorship Rules** (`bus_rules.column_rules` /
       `matching_rules` / `matching_thresholds` / `survivorship_rules`, all DB-native).
       Same visibility/gating pattern as Reference Data Maintenance, but submissions go
       into the higher-risk `rules_config_change` workflow (1 level, quorum of 2 *different*
       Data Owners) since these rules govern what the batch pipeline rejects, matches, and
       survives. One workflow_type covers all four sub-tables; `entity_type` on the
       workflow instance distinguishes which one a given submission targets. dbt's
       `gold_crosswalk.sql` and `gold_survivorship_winners.sql` read `bus_rules.*` as
       sources; `scripts/generate_matches.py` and `api/reprocessing.py` query them directly.
   - **Administration → User Administration** — admin-only. Create/edit/deactivate users,
     assign role (`admin`/`dataSteward`/`dataOwner`/`businessUser`) and gold access
     (`read_write`/`read`/`none`), reset passwords. The seed admin account is `mdm_admin`, created via
     `scripts/create_admin_user.py`, which prints a one-time random password to the
     terminal — never stored in plaintext, never baked into the shipped zip. **Creating a
     user, or changing `role`/`gold_access`/`is_active` on an existing one, now submits
     into the `user_admin_change` maker-checker workflow** (1 level, 1 Admin approver who
     must be a different admin than the requester) — the new-user's one-time password is
     generated only at approval time and shown only to the approver, not the requester.
     `full_name`-only edits and password resets are not gated (they don't change what a
     user is authorized to do) and still apply immediately.

3. **Maker-Checker Approval Workflow Engine** (`api/workflow_engine.py`, `governance`
   schema) — generic across all three apps, added so a maker can never be the sole
   authority on a state-changing action. A `workflow_type` is an ordered list of steps
   (`governance.workflow_definitions`, seeded once on first boot and left alone
   thereafter so a future Rules Configuration screen can edit it at runtime); each step
   names a `required_role` and an `approvals_required` count (1 for a simple single
   approver, >1 for a same-level quorum of *different* people). The engine itself
   enforces, independent of any caller: a maker can never decide on their own
   submission, the same approver can never cast two decisions on one instance, and
   rejection at any step is terminal. Six workflows are configured today:
   `stewardship_remediation` (1 level, 1 Data Owner), `gold_record_edit` (1 level,
   quorum of 2 Data Owners), `match_review_confirmation` (2 sequential levels: Data
   Owner, then Admin), `user_admin_change` (1 level, 1 Admin), `reference_data_change`
   (1 level, 1 Data Owner — Reference Data Maintenance), and `rules_config_change`
   (1 level, quorum of 2 Data Owners — Rules Configuration). Generic endpoints —
   `GET /api/v1/workflows/pending`, `GET /api/v1/workflows/mine`,
   `GET /api/v1/workflows/{instance_id}`, `POST /api/v1/workflows/{instance_id}/decide`
   — serve an **Approvals** view in both the Stewardship Console and the Portal (the
   Portal's copy is visible to every role, since Admins can't reach the console).
   `scripts/create_demo_governance_users.py` seeds the extra `dataOwner`/`admin`
   accounts (`mdm_dataowner2`, `mdm_dataowner3`, `mdm_admin2`) needed to actually clear
   a quorum or a "different approver" requirement in a fresh install, which otherwise
   ships with only one account per role.

4. **REST API** — exposes the gold layer, crosswalk, lineage graph, and stewardship queue
   for downstream consumption. Auto-docs at `/docs`.

## Key data model notes

- `bus_rules.column_rules` — the single source of truth for cleansing/standardization/
  validation rules, referenced by rule ID (R001, R002...) throughout the pipeline, the
  exception queue's reject_reasons, and the stewardship UI. DB-native (schema `bus_rules`),
  maintained via the Rules Configuration screen's maker-checker workflow — migrated off
  `dbt_project/seeds/column_rules.csv` when that screen was built.
- `dbt_project/seeds/lineage_edges.csv` — column-level lineage metadata (from_layer/table/
  column → to_layer/table/column, transform_rule_id, description). Powers both the
  Data Governance network diagram and the `/api/v1/lineage/*` endpoints. Still a dbt seed
  (out of scope for the Rules Configuration migration). Note: wildcard nodes (e.g.
  `gold.gold_match_candidates.*`) must be explicitly linked to specific columns or
  impact-analysis chains silently break — this bit us once already, and again when the
  match/merge step moved into `gold_prep` (matching layer is now `gold_prep`, edges
  E013/E014/E018-E026 cover it; re-verify with `/api/v1/lineage/impact` and `/trace` after
  touching this file, don't just eyeball it).
- `bus_rules.matching_thresholds` — one row per matching tier (tier_id, tier_order,
  tier_name, match_method, is_match_tier, auto_merge_threshold, review_lower_threshold,
  active, description). DB-native, maintained via Rules Configuration's "Matching Tiers"
  tab (migrated off `dbt_project/seeds/matching_thresholds.csv`). `match_method` is
  `'exact'` or `'fuzzy_tfidf_cosine'` today; a non-tier row (`is_match_tier=false`,
  `match_method='no_match_baseline'`) holds the 0.50 provisional-confidence value.
  Adding a tier means adding a row here (plus its rules below) — nothing in
  `scripts/generate_matches.py` or `api/reprocessing.py` needs to change to add another
  exact or fuzzy tier, only to add a genuinely new match_method.
- `bus_rules.matching_rules` — child table keyed by `tier_id`, one row per field/column a
  tier operates on (rule_role is `exact_match_field`, `similarity_text_field`, or
  `blocking_key`; `transform_function` is `none`/`normalize_email`/`normalize_phone`,
  applied identically in both Python (`scripts/generate_matches.py`, `api/reprocessing.py`)
  and SQL (`gold_crosswalk.sql`'s tier lookups) via matching name-keyed registries). DB-native,
  maintained via Rules Configuration's "Matching Rules" tab (migrated off
  `dbt_project/seeds/matching_rules.csv`). Multiple `blocking_key` rows for one tier compose
  into a multi-column blocking key; `rule_order` controls concatenation order for
  `similarity_text_field` rows.
- `bus_rules.survivorship_rules` — new: one active rule per gold column (`target_column`),
  driving attribute-level survivorship. `rule_type` is `most_common` (the non-blank value
  appearing in the most contributing records), `most_complete` (prefers non-blank over
  blank), `oldest`/`newest` (by `source_modified_date`), or `pattern_match` (prefers a value
  matching `rule_param`, a regex). Exactly one primary rule per column; whenever it doesn't
  produce a clean winner, `source_modified_date` desc (then CRM preferred) is the universal,
  non-configurable tie-break — not itself a further stack of configurable rules. Defaults
  (seeded by `api/db.py`/`scripts/ensure_governed_schemas.py`) are all `newest`, which
  reproduces the previous record-level behavior exactly until a governance user tunes a
  specific column. Evaluated by `dbt_project/models/gold/gold_survivorship_winners.sql`
  (batch) and `api/reprocessing.py`'s `_pick_column_winners()` (real-time) — the two must
  stay in lockstep.
- `ref.ref_country_codes` / `ref.ref_state_codes` — DB-native reference data (schema `ref`),
  maintained via the Reference Data Maintenance screen's maker-checker workflow. Each has a
  human-readable `country_name`/`state_name` label alongside the code, plus `is_active`
  (migrated off `dbt_project/seeds/ref_country_codes.csv` / `ref_state_codes.csv`, which
  only had the bare code). `api/validation.py` queries these live (no caching) since, unlike
  the old static seed, they can change at runtime.
- `gold_prep.match_groups` / `match_edges` / `match_review_candidates` — written by
  `scripts/generate_matches.py` (not dbt-built), same "Python loads, dbt treats it as a
  source" pattern as bronze. `match_edges` is the audit trail dbt joins to compute
  `gold_crosswalk.match_confidence_score`.
- `auth.users` / `auth.sessions` — bcrypt password hashes, bearer tokens, 8hr TTL.
- `stewardship.remediation_log` / `remediated_records` / `exception_status_overrides` —
  audit trail and status tracking layered on top of the dbt-built `exceptions_queue` table
  (which itself is a static point-in-time snapshot; live status lives in the overrides
  table, not the snapshot).
- `stewardship.match_review_overrides` / `match_review_log` — same live-status-overlay
  pattern, for `gold_match_review_queue` instead of `exceptions_queue`. A 'confirmed'
  override is honored by `generate_matches.py` as a forced union-find merge on the next
  run; a 'rejected' override permanently excludes that pair from ever resurfacing.
- `audit.audit_trail` — append-only gold-layer audit trail (`api/audit.py`). One row per
  changed field per logical operation, grouped by `change_batch_id` (all field rows from
  one edit share a batch id so the UI renders one timeline entry per edit). Three writers:
  `api/main.py`'s `update_customer()` (`event_source='portal_manual_edit'`),
  `api/reprocessing.py` (`'steward_reprocessing'`), and `scripts/audit_pipeline_diff.py`
  (`'pipeline_batch'`, run as the final step of `scripts/build_pipeline.py`). No code
  anywhere issues an UPDATE/DELETE against this table.
- `governance.workflow_definitions` / `workflow_instances` / `workflow_decisions` — the
  maker-checker engine's own tables (`api/workflow_engine.py`). Definitions are seeded
  per `workflow_type` (an existing type is never re-seeded, but a newly-added one — like
  `reference_data_change`/`rules_config_change` when Rules Configuration shipped — is
  backfilled into an already-running DB on next app start) so runtime edits persist across
  restarts; instances carry a JSON `payload`
  (whatever the completion callback needs to apply the change) and, once
  approved/rejected, a JSON `result`; decisions are one row per approver per step, which
  is how the engine enforces "no double-deciding" and quorum counts.
- `audit.gold_customers_snapshot` — a copy of `main_gold.gold_customers`'s tracked columns
  as of the last batch pipeline run, kept in the `audit` schema specifically because `dbt
  run` never touches that schema (it CREATE OR REPLACEs `main_gold.gold_customers` every
  run). `scripts/audit_pipeline_diff.py` diffs the fresh gold table against this snapshot
  to detect batch-driven creates/updates/logical-deletes, then refreshes it.

## Known simplifications (documented on purpose, not oversights)

- Fuzzy matching only runs in the batch pipeline (`scripts/generate_matches.py`).
  Real-time reprocessing (steward resolves an exception → immediate re-match) still only
  evaluates the `match_method='exact'` tier -- fitting a TF-IDF vectorizer per API request
  was judged not worth the latency for a demo-scoped feature. Both paths read the same
  `exact_match_field` rules from `bus_rules.matching_rules`, so they can't drift on *which*
  fields count as an exact match, even though only the batch step can also run a fuzzy
  tier. A confirmed/rejected Match Review decision similarly only takes effect on the
  next batch rebuild, not instantly.
- The fuzzy tier is TF-IDF character n-gram cosine similarity, not a neural sentence
  embedding -- a deliberate lightweight choice (see Tech stack); the vectorizer's shape
  (char_wb analyzer, 2-4 char n-grams) is a fixed code constant in `generate_matches.py`,
  not rule metadata. Thresholds (0.80 auto-merge, 0.35 review floor today) live in
  `bus_rules.matching_thresholds`'s `auto_merge_threshold`/`review_lower_threshold` columns
  for the fuzzy tier row, not hardcoded constants -- edit the row (via Rules Configuration,
  maker-checker approved) and rerun to recalibrate, no code edit needed. Original
  calibration was empirical, against this project's synthetic data generator's 12 seeded
  fuzzy-duplicate pairs vs. every same-state non-duplicate pair; re-calibrate if the
  generator's population changes materially.
- Attribute-level survivorship's `most_common`/`most_complete`/`pattern_match` rule types
  are evaluated independently per column across whatever sources contributed to that match
  group -- there's no cross-column consistency check (e.g. a golden record could in theory
  draw `city` from one source and `state_code` from another even if that pairing wouldn't
  occur together in any single real source record). Accepted for a demo-scoped feature;
  a production system would likely want a "prefer values from the same winning source when
  reasonably possible" refinement.
- Steward corrections ARE re-validated against the reject-severity metadata rules
  (`api/validation.py`, mirroring `column_rules.csv`) before reprocessing runs — a
  correction that still fails validation is rejected with an alert and the record
  stays in the queue (status remains `open`). Only a genuinely valid correction is
  treated as authoritative and flows into match/merge; correct-severity
  standardization rules (proper-casing, phone formatting) are not re-checked, since
  they don't gate silver eligibility in the batch pipeline either.
- Real-time reprocessing's match step compares against each golden record's *current*
  survivor values, not every historical non-survivor source in that group.
- New golden IDs from real-time reprocessing increment the current max, which can diverge
  from what a full `dbt run` would assign from scratch. The audit trail inherits this: if
  a reprocessing-created golden_id gets renumbered by the next full rebuild,
  `audit_pipeline_diff.py` logs the old number as `logically_deleted` and the new number
  as `created`, even though it's the same underlying customer — not a bug in the diff
  logic, an accepted consequence of the numbering scheme itself.
- The audit trail only captures a manual `dbt run --select gold.*` if
  `scripts/audit_pipeline_diff.py` is run immediately afterward (this happens
  automatically inside `build_pipeline.py`, but not if you run the gold rebuild command
  by itself, e.g. after a Match Review confirm/reject).
- Auth is sized for a demo: no MFA, no refresh tokens, single local DuckDB file (not a
  concurrent multi-user warehouse).
- If the domain action itself fails after every required
  approval is already recorded (e.g. a downstream write error), the instance is still
  marked `approved` — the human decision stands — with the failure captured in its
  `result` field rather than silently rolled back; there is no automatic retry. A
  quorum or multi-level chain also needs enough distinct people in the required role to
  ever clear, since a maker can't approve their own submission and the same approver
  can't decide twice on one instance — hence `scripts/create_demo_governance_users.py`.

## Repo structure

```
data/                  synthetic CRM+ERP source data generator (Faker-based, ~100 rows/source)
scripts/                load_bronze.py, generate_matches.py (fuzzy match/merge, Python step
                        between silver and gold), build_pipeline.py (runs the 6-stage build,
                        step 0 = ensure_governed_schemas.py), ensure_governed_schemas.py
                        (provisions ref.*/bus_rules.* before dbt runs -- see its docstring),
                        audit_pipeline_diff.py (gold-layer audit trail diff, final build
                        step), create_admin_user.py, reset_admin_password.py,
                        create_demo_governance_users.py (seeds extra dataOwner/admin
                        accounts needed to test maker-checker quorums/multi-level chains)
dbt_project/            seeds (only lineage_edges.csv now -- column_rules.csv,
                        matching_thresholds.csv, matching_rules.csv, ref_state_codes.csv,
                        and ref_country_codes.csv migrated to DB-native bus_rules/ref
                        schemas when Rules Configuration/Reference Data Maintenance
                        shipped), models (silver, gold); gold reads
                        scripts/generate_matches.py's output via the gold_prep source and
                        bus_rules.*/ref.* via their own sources
api/                    FastAPI app: main.py, db.py, auth.py, reprocessing.py,
                        lineage.py, ai_remediation.py, audit.py (gold-layer audit trail),
                        workflow_engine.py (generic maker-checker workflow engine)
stewardship_app/frontend/   exception queue + match review console (plain HTML/JS), plus
                        an Approvals tab over the maker-checker workflow engine
portal_app/frontend/        login-gated portal: browse, governance graph, admin, and an
                        Approvals view (plain HTML/JS)
requirements.txt        pinned deps (duckdb==1.5.4 -- NOT the same version number as the
                        dbt-duckdb adapter, a mistake made once already, worth double-checking)
README.md               Windows-first setup instructions (PowerShell), plus macOS/Linux
```

## Outstanding / not yet built

- Attribute-level survivorship's cross-column consistency (see Known simplifications) is
  an accepted gap, not a planned build item.
- CCA-F (Claude Certified Architect – Foundations) certification is a separate, unrelated
  thread the user is pursuing — exam access is gated behind the Claude Partner Network with
  no individual registration path found so far; using Skilljar course-completion
  certificates as an interim credential.

## How to help in this project going forward

- Treat the working app as the source of truth; if asked to change behavior, check the
  actual current code/schema first rather than assuming from this summary alone (this
  document may lag recent changes).
- This is a demo built under real time constraints — prefer pragmatic, tested fixes over
  large rewrites unless explicitly asked to redesign something.
- Always test changes end-to-end (rebuild the pipeline, hit the actual API, don't just
  review code) before presenting a fix as done — this project's history includes more than
  one case where an untested "fix" (a bad dependency pin, a missing `pandas` requirement,
  a broken lineage chain) shipped and had to be caught and corrected afterward.
- Windows portability matters for every file-path change — no hardcoded absolute paths.
- **Every time a fix or enhancement is successfully tested, update documentation and push
  to GitHub as part of finishing the task, not as a separate follow-up:** update this file
  (`PROJECT_KNOWLEDGE.md`), `README.md`, and the relevant `.docx` deliverables (at minimum
  `Design_Document.docx`; also `Enterprise_Readiness_Assessment.docx` and the
  `Installation_Guide_*.docx` files if the change affects them) so they describe the
  change, then `git add` / `git commit` / `git push` to `origin/main`
  (`https://github.com/balramvv-gh/MDM-Medallion-Arch-Demo-V2.git`). Check `git status`
  and `git log main..origin/main` first — this repo has previously accumulated uncommitted
  local changes and un-pulled remote commits from other sessions/machines; reconcile
  (`git fetch`, `git merge --ff-only origin/main` or resolve conflicts) before adding new
  work on top, rather than committing blind. Network access to github.com from a sandboxed
  session has been intermittent (occasional `502`/`403` from the proxy) but has succeeded
  on retry every time so far — retry a few times before concluding the push isn't possible.

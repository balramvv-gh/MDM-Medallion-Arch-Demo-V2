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
  **metadata-driven**: rules live in `dbt_project/seeds/column_rules.csv` (rule_id, source
  column, rule_type, severity), not hardcoded in pipeline code. Records failing validation
  route to an exception queue instead of silver.
- **Gold (Data Hub)** — deterministic match/merge (exact match on normalized email OR
  phone) across sources, record-level survivorship (most-recently-modified source wins,
  CRM preferred as tiebreak), and a **crosswalk table** preserving the relationship between
  every golden record and its contributing source records with match confidence scores.

## Tech stack

- **dbt-core + DuckDB** for the batch pipeline — genuinely runs locally, zero cloud cost,
  no Databricks account needed. This was a deliberate choice over Talend (which the user
  has production experience with) because Talend's MDM Server product and Open Studio are
  both end-of-life/discontinued as of 2024, and Databricks/dbt-style lakehouse patterns are
  what current job postings ask for.
- **FastAPI** backend for all apps and the REST API, with bcrypt-hashed password auth
  (from-scratch bearer-token sessions, 8-hour expiry).
- Plain HTML/JS frontends (no build step, no Node.js needed) for both apps.
- **Windows-first**: every file path is computed via `pathlib` relative to file location —
  no hardcoded absolute paths anywhere. The user runs this on Windows (PowerShell), the
  dev/test environment used to build it is Linux, so this portability was hard-won and
  tested by literally extracting into a space-containing path (`MDM Pipeline`) and running
  the full pipeline before shipping each zip.

## Applications

1. **Data Stewardship console** (`/app/`) — reviews the exception queue, gets an
   AI-assisted remediation suggestion (real Claude API call via `ANTHROPIC_API_KEY`, with a
   transparent heuristic fallback when no key is set — every suggestion response includes
   `source: "ai"` or `"heuristic_fallback"`), and approves/rejects corrections. **Approving
   a correction triggers real-time reprocessing**: the corrected record is upserted into
   silver, matched against the current gold set, and survivorship is re-run to either update
   the matched golden record or create a new one. This mirrors the batch dbt logic so both
   paths stay consistent, though golden ID numbering can diverge between them (documented
   limitation).

2. **MDM Data Hub Portal** (`/portal/`) — login-gated. Has:
   - **Browse Customers** — searchable/paginated gold-layer browser with live (debounced)
     search-as-you-type, click-through detail view with the source crosswalk, inline edit
     if the user's account has `read_write` gold access.
   - **Data Governance** — a full pannable/zoomable **network diagram** of the entire
     lineage metadata graph (bronze→silver→gold→stewardship). Click any node to highlight
     its upstream lineage (amber) and downstream impact (green) directly on the graph, with
     a side detail panel (description, direct in/out rules, upstream/downstream counts).
     Clicking a gold node adds a Golden ID lookup tool. This replaced an earlier two-mode
     "pick a column, see a static chain" version entirely, by design.
   - **Administration → User Administration** — admin-only. Create/edit/deactivate users,
     assign role (`admin`/`steward`/`viewer`) and gold access (`read_write`/`read`/`none`),
     reset passwords. The seed admin account is `mdm_admin`, created via
     `scripts/create_admin_user.py`, which prints a one-time random password to the
     terminal — never stored in plaintext, never baked into the shipped zip.

3. **REST API** — exposes the gold layer, crosswalk, lineage graph, and stewardship queue
   for downstream consumption. Auto-docs at `/docs`.

## Key data model notes

- `dbt_project/seeds/column_rules.csv` — the single source of truth for cleansing/
  standardization/validation rules, referenced by rule ID (R001, R002...) throughout the
  pipeline, the exception queue's reject_reasons, and the stewardship UI.
- `dbt_project/seeds/lineage_edges.csv` — column-level lineage metadata (from_layer/table/
  column → to_layer/table/column, transform_rule_id, description). Powers both the
  Data Governance network diagram and the `/api/v1/lineage/*` endpoints. Note: wildcard
  nodes (e.g. `gold.gold_match_candidates.*`) must be explicitly linked to specific match-
  key columns or impact-analysis chains silently break — this bit us once already (fixed
  in edges E018/E019).
- `auth.users` / `auth.sessions` — bcrypt password hashes, bearer tokens, 8hr TTL.
- `stewardship.remediation_log` / `remediated_records` / `exception_status_overrides` —
  audit trail and status tracking layered on top of the dbt-built `exceptions_queue` table
  (which itself is a static point-in-time snapshot; live status lives in the overrides
  table, not the snapshot).

## Known simplifications (documented on purpose, not oversights)

- Matching is deterministic (exact email/phone match), not fuzzy/probabilistic.
- Survivorship is record-level (whole record from one winning source), not attribute-level.
- Steward corrections skip re-validation against the metadata rules — steward approval is
  treated as authoritative (human-in-the-loop override).
- Real-time reprocessing's match step compares against each golden record's *current*
  survivor values, not every historical non-survivor source in that group.
- New golden IDs from real-time reprocessing increment the current max, which can diverge
  from what a full `dbt run` would assign from scratch.
- Auth is sized for a demo: no MFA, no refresh tokens, single local DuckDB file (not a
  concurrent multi-user warehouse).

## Repo structure

```
data/                  synthetic CRM+ERP source data generator
scripts/                load_bronze.py, create_admin_user.py, reset_admin_password.py
dbt_project/            seeds (rules/reference/lineage metadata), models (silver, gold)
api/                    FastAPI app: main.py, db.py, auth.py, reprocessing.py,
                        lineage.py, ai_remediation.py
stewardship_app/frontend/   exception queue console (plain HTML/JS)
portal_app/frontend/        login-gated portal: browse, governance graph, admin (plain HTML/JS)
requirements.txt        pinned deps (duckdb==1.5.4 -- NOT the same version number as the
                        dbt-duckdb adapter, a mistake made once already, worth double-checking)
README.md               Windows-first setup instructions (PowerShell), plus macOS/Linux
```

## Outstanding / not yet built

- Professional Design Document and Technical Reference Document (Word/.docx deliverables),
  matching the pattern already established on the user's earlier AI Product Recommendation
  Engine portfolio project: working demo first, polished documentation second.
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

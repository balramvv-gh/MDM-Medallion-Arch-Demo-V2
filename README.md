# MDM Medallion Architecture Demo

A working demonstration of a metadata-driven Master Data Management pipeline built on
a bronze/silver/gold (medallion) architecture, with an AI-assisted data stewardship
application, a login-gated data hub portal, and a REST API over the gold layer.

## What this demonstrates

- **Bronze layer** — raw customer extracts from two heterogeneous source systems (a
  CRM and an ERP), landed untouched.
- **Silver layer** — cleansing, standardization, and validation driven entirely by
  **metadata** (`dbt_project/seeds/column_rules.csv`), not hardcoded logic. Records
  that fail validation are routed to an exception queue instead of the pipeline.
- **Gold layer (Data Hub)** — hybrid match/merge across source systems:
  deterministic exact matching on normalized email/phone, plus an
  **embedding-similarity (fuzzy) tier** that catches near-duplicates exact
  matching misses (typos, nicknames, reformatted addresses) via TF-IDF
  character n-gram cosine similarity, computed in `scripts/generate_matches.py`
  and clustered with union-find. High-confidence fuzzy matches auto-merge;
  borderline ones go to a **Match Review queue** for a data steward to confirm
  or reject. Record-level survivorship and a **crosswalk table** preserve the
  link between every golden record and its contributing source records, with
  a graduated match confidence score (1.00 for exact, the actual similarity
  score for fuzzy, 0.50 "provisional" for uncorroborated single-source records).
- **Data Stewardship app** (`/app/`) — restricted to `dataSteward` / `dataOwner`
  accounts, two tabs: an **Exception Queue** console for reviewing rejected records,
  getting an AI-assisted remediation suggestion (Claude, with a heuristic fallback
  if no API key is configured), and approving/rejecting corrections. Approving a
  correction triggers real-time **reprocessing**: the corrected record is upserted
  into the silver layer, matched against the current set of golden records, and
  survivorship is re-run to either update the matched golden record (recomputing
  the survivor across the whole contributing group) or create a brand-new golden
  record if no match is found. The console shows you which happened and which
  `golden_id` was affected. And a **Match Review** tab: borderline fuzzy-match
  candidates from the gold layer's embedding-similarity tier, shown side by
  side, for the steward to confirm ("same customer, merge them") or reject
  ("coincidence, keep separate"). Unlike exception resolution, this doesn't
  reprocess in real time — the decision takes effect on the next pipeline
  rebuild (see `scripts/generate_matches.py`).
- **MDM Data Hub Portal** (`/portal/`) — a **login-gated** application for browsing
  the gold layer (search, paginate, view a record's full source crosswalk, and edit
  it if your account has write access), plus an **Administration → User
  Administration** screen for managing portal accounts: creating users, assigning
  roles (`admin` / `dataSteward` / `dataOwner` / `businessUser`) and gold-layer access
  (`read_write` / `read` / `none`), deactivating accounts, and resetting passwords.
  Passwords are stored as bcrypt hashes — never in plaintext. Note: `admin` is a
  distinct governance function (user administration) and is deliberately **not** a
  superset of stewardship rights — an admin account cannot access the Data
  Stewardship console or its API endpoints; only `dataSteward` / `dataOwner` can.
- **Data Governance** (`/portal/`, "Data Governance" tab) — an explorable **network
  diagram** of the entire lineage metadata graph (bronze → silver → gold →
  stewardship), pannable and zoomable. Click any node to highlight its full
  upstream lineage (amber) and downstream impact (green) directly on the graph,
  and see details — description, direct incoming/outgoing rules, and
  upstream/downstream node counts — in a side panel. Clicking a gold-layer node
  also offers a quick lookup of any specific Golden ID's contributing source
  records.
- **REST API** — exposes the gold layer (and its crosswalk) for downstream system
  consumption.

## Stack

- **dbt-core + DuckDB** for the pipeline (genuinely "dbt-style" — runs locally with
  zero cloud cost, no Databricks account needed)
- **scikit-learn** (TF-IDF + cosine similarity) for the fuzzy-matching tier — a
  lightweight, fully-local embedding technique (no model download, no GPU, no
  cloud API) chosen over a neural sentence-embedding model to keep setup fast
  and the demo runnable fully offline
- **Faker** to generate a larger, more realistic synthetic source population
- **FastAPI** for the app backends and REST API, with bcrypt-hashed password auth
- Plain HTML/JS frontends for both apps (no build step, no Node.js needed)

---

## Windows setup (PowerShell)

These steps assume you've extracted the zip into a folder such as:
`C:\Balram\Personal\Learning\AI\Claude\MDM Pipeline`

Open **PowerShell**, then:

```powershell
cd "C:\Balram\Personal\Learning\AI\Claude\MDM Pipeline"

python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

> If `python` isn't recognized, try `py` instead (the Python launcher), e.g. `py -m venv venv`.
>
> If PowerShell blocks the activation script with an execution-policy error, run
> `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` first, then retry.

### Build the pipeline

```powershell
# 1. Generate synthetic source data (CRM + ERP CSV extracts, ~100 rows each)
python data\generate_source_data.py

# 2. Land it in the bronze layer
python scripts\load_bronze.py

# 3. Build the rest of the pipeline (seed -> silver -> match/merge -> gold)
python scripts\build_pipeline.py
```

`build_pipeline.py` runs four steps in order and stops if any of them fail:

```powershell
cd dbt_project
$env:DBT_PROFILES_DIR = "."
dbt --no-partial-parse seed
dbt --no-partial-parse run --exclude gold.*
cd ..
python scripts\generate_matches.py     # embedding-based match/merge -- needs silver, produces gold's input
cd dbt_project
dbt --no-partial-parse run --select gold.*
cd ..
```

`--no-partial-parse` forces a full reparse instead of trusting dbt's
`target/partial_parse.msgpack` cache. Without it, a manifest cache left over
from a previous run (different machine, different OS, or just a moved/copied
project folder) can go stale and raise a `KeyError` looking up a macro file
that doesn't match the current parse. `build_pipeline.py` already passes this
flag; include it if you run the commands manually too.

The gold layer now depends on a Python step (`scripts/generate_matches.py`,
TF-IDF embedding similarity via scikit-learn) that has to run strictly between
silver and gold, so a single `dbt run` can no longer build the whole pipeline
end to end — run `build_pipeline.py`, or the four commands above, instead.
If you only changed something silver-or-earlier and don't need to re-match,
`dbt run --exclude gold.*` alone is enough; if you only changed a steward's
match-review decision, `python scripts\generate_matches.py` followed by
`dbt run --select gold.*` is enough.

This produces `mdm_demo.duckdb` in the project root — a single-file database
containing every layer (bronze, silver, gold, rules metadata, lineage metadata,
plus the portal's user/session tables once you complete the next step).

### Create the admin user

```powershell
python scripts\create_admin_user.py
```

This prints a **one-time password** for the `mdm_admin` account to your terminal —
copy it now, it is never shown again (and never stored in plaintext; only its
bcrypt hash is saved). If you ever get locked out, run
`python scripts\reset_admin_password.py` to generate a new one.

### Run the app + API

```powershell
cd api
uvicorn main:app --reload --port 8000
```

Then open in your browser:
- **MDM Data Hub Portal (login required):** http://localhost:8000/portal/
  — sign in with `mdm_admin` and the password from the step above.
- **Data Stewardship console:** http://localhost:8000/app/
- **REST API docs (auto-generated):** http://localhost:8000/docs
- **Health check:** http://localhost:8000/health

### Enabling real AI-assisted remediation (optional)

```powershell
$env:ANTHROPIC_API_KEY = "your_key_here"
```
Set this before starting `uvicorn`. Without it, the stewardship app uses a
transparent heuristic fallback so the demo runs fully offline — every suggestion
response includes a `source` field (`"ai"` or `"heuristic_fallback"`) so it's always
clear which path produced it.

---

## macOS / Linux setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 data/generate_source_data.py
python3 scripts/load_bronze.py
python3 scripts/build_pipeline.py      # seed -> silver -> match/merge -> gold

python3 scripts/create_admin_user.py   # copy the printed one-time password

cd api
uvicorn main:app --reload --port 8000
```
Enable AI remediation with `export ANTHROPIC_API_KEY=your_key_here` before starting `uvicorn`.

---

## Key REST API endpoints

| Endpoint | Purpose |
|---|---|
| `POST /api/v1/auth/login` | Authenticate, returns a session token |
| `GET /api/v1/auth/me` | Current user's identity/role/access |
| `GET /api/v1/customers` | List golden customer records (auth required; supports `q=` search) |
| `GET /api/v1/customers/{golden_id}` | Get one golden record |
| `PUT /api/v1/customers/{golden_id}` | Edit a golden record (requires `read_write` gold access) |
| `GET /api/v1/customers/{golden_id}/sources` | Crosswalk: contributing source records |
| `GET /api/v1/lineage/impact?layer=&table=&column=` | Forward impact analysis |
| `GET /api/v1/lineage/trace?layer=&table=&column=` | Backward lineage trace |
| `GET /api/v1/lineage/record/{golden_id}` | Full lineage for one golden record |
| `GET /api/v1/admin/users` | List portal users (admin role required) |
| `POST /api/v1/admin/users` | Create a user (admin role required) |
| `PUT /api/v1/admin/users/{user_id}` | Update role/access/status (admin role required) |
| `POST /api/v1/admin/users/{user_id}/reset_password` | Reset a user's password (admin role required) |
| `GET /api/v1/stewardship/queue?status=open` | Exception queue |
| `POST /api/v1/stewardship/queue/{id}/suggest` | Get AI/heuristic remediation suggestion |
| `POST /api/v1/stewardship/queue/{id}/resolve` | Approve a correction |
| `POST /api/v1/stewardship/queue/{id}/reject` | Reject a record |
| `GET /api/v1/stewardship/match-review?status=pending` | Borderline fuzzy-match candidates (dataSteward/dataOwner role required) |
| `GET /api/v1/stewardship/match-review/{pair_id}` | One match-review candidate pair, full side-by-side detail |
| `POST /api/v1/stewardship/match-review/{pair_id}/confirm` | Confirm a pair is the same customer (merges on the next pipeline rebuild) |
| `POST /api/v1/stewardship/match-review/{pair_id}/reject` | Reject a pair as coincidental (permanently excluded going forward) |

All `/api/v1/customers*`, `/api/v1/admin/*` and `/api/v1/auth/me` endpoints require an
`Authorization: Bearer <token>` header from `/api/v1/auth/login`.

## Design notes / known simplifications (by design, for demo scope)

- **Matching** is a hybrid of deterministic exact matching (normalized email or
  phone, confidence 1.00) and an embedding-similarity fuzzy tier (TF-IDF
  character n-gram cosine similarity over name/address text, blocked by
  state_code). Fuzzy matches above a calibrated high threshold auto-merge with
  their similarity score as confidence; borderline ones go to a **Match
  Review** queue for a steward to confirm or reject rather than auto-merging.
  This is a real embedding-similarity technique used in production dedup
  systems, deliberately kept lightweight (no model download, fully offline,
  no GPU) rather than a neural sentence-embedding model — a reasonable next
  iteration if higher recall on more subtle near-duplicates is needed. See
  `scripts/generate_matches.py` for the full algorithm and calibration notes.
  **Known gap:** the real-time reprocessing path (steward resolves a
  validation exception → immediate re-match) still only does exact matching;
  the fuzzy tier only runs in the batch pipeline. Fitting a TF-IDF vectorizer
  per API request was judged not worth the added request latency for a
  demo-scoped feature.
- **Survivorship** is record-level (most-recently-modified source wins for the
  whole record), not attribute-level — a natural next iteration.
- Rules are documented as metadata (`column_rules.csv`) and referenced by rule ID
  throughout the pipeline and stewardship app; the current implementation applies
  them via explicit SQL rather than fully dynamic rule interpretation, trading a
  bit of "purity" for time-boxed reliability.
- **Auth** is a from-scratch bcrypt + bearer-token implementation sized for a demo
  (sessions expire after 8 hours, no refresh tokens, no MFA). A production system
  would typically delegate this to an identity provider (Okta, Azure AD, etc.).
- The database (`mdm_demo.duckdb`) is a single local file — fine for a demo, not a
  substitute for a real concurrent multi-user warehouse.
- **Steward-corrected records ARE re-run through the reject-severity validation
  rules** (`api/validation.py`, mirroring `column_rules.csv`) before reprocessing.
  If the correction still fails validation, the steward sees an alert explaining
  what's still wrong and the record stays in the exception queue — it is not
  marked resolved and does not reach match/merge. Only once a correction
  genuinely passes validation does it flow through, at which point the "modified"
  timestamp is stamped as the moment of correction, so under the recency-wins
  survivorship rule a fresh correction will typically become the new survivor.
- The real-time reprocessing match step compares against each existing golden
  record's *current* (survivor) email/phone, not every historical non-survivor
  source in that group — consistent with the record-level (not attribute-level)
  survivorship design used elsewhere in this demo.
- Golden IDs created via real-time reprocessing are numbered by incrementing the
  current max, which can diverge from what a full `dbt run` from scratch would
  assign (dbt's match-group numbering is order-dependent on the complete silver
  set). Both paths stay internally consistent; the ID numbers themselves aren't
  guaranteed portable between a live-reprocessed record and a full pipeline rerun.

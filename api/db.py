import duckdb
import threading
from pathlib import Path

# Portable: resolves to <project_root>/mdm_demo.duckdb regardless of OS or
# where the project was checked out / extracted.
DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

_lock = threading.Lock()
_con = None


def get_con():
    global _con
    if _con is None:
        _con = duckdb.connect(DB_PATH)
        _ensure_stewardship_tables(_con)
        _ensure_match_review_tables(_con)
        _ensure_auth_tables(_con)
        _ensure_audit_tables(_con)
        _ensure_governance_tables(_con)
    return _con


def _ensure_auth_tables(con):
    """Creates the app-level auth tables (users + sessions) for the UX portal."""
    con.execute("CREATE SCHEMA IF NOT EXISTS auth;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS auth.users (
            user_id VARCHAR PRIMARY KEY,
            username VARCHAR UNIQUE,
            password_hash VARCHAR,
            full_name VARCHAR,
            role VARCHAR,           -- 'admin' | 'dataSteward' | 'dataOwner' | 'businessUser' -- drives app menu visibility
            gold_access VARCHAR,    -- 'read_write' | 'read' | 'none' -- drives gold-layer data permission
            is_active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            last_login_ts TIMESTAMP
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS auth.sessions (
            token VARCHAR PRIMARY KEY,
            user_id VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            expires_ts TIMESTAMP
        );
    """)


def _ensure_stewardship_tables(con):
    """Creates the stewardship-side tables if this is a fresh DB file."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.remediation_log (
            log_id VARCHAR,
            exception_id VARCHAR,
            action VARCHAR,             -- ai_suggested | steward_resolved | steward_rejected
            suggested_fields VARCHAR,   -- JSON string
            rationale VARCHAR,
            suggestion_source VARCHAR,  -- ai | heuristic_fallback
            applied_fields VARCHAR,     -- JSON string, null until resolved
            steward_note VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.exception_status_overrides (
            exception_id VARCHAR PRIMARY KEY,
            remediation_status VARCHAR,  -- open | in_review (submitted, awaiting maker-checker approval) | resolved | rejected
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    # Records that a steward has corrected and approved land here, then flow
    # back into silver_customers on the next pipeline run (re-validated).
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.remediated_records (
            exception_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            source_record_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            address_line1 VARCHAR,
            address_line2 VARCHAR,
            city VARCHAR,
            state_code VARCHAR,
            postal_code VARCHAR,
            country_code VARCHAR,
            resolved_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)


def _ensure_match_review_tables(con):
    """Live status overlay for gold_match_review_queue (the dbt-built static
    snapshot of borderline fuzzy-match pairs) -- same pattern as
    exception_status_overrides for exceptions_queue. Also created by
    scripts/generate_matches.py; whichever of the two runs first wins, both
    definitions must stay identical."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_overrides (
            pair_id VARCHAR PRIMARY KEY,
            status VARCHAR,             -- 'pending' | 'in_review' (submitted, awaiting maker-checker approval) | 'confirmed' | 'rejected'
            steward_note VARCHAR,
            updated_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_log (
            log_id VARCHAR,
            pair_id VARCHAR,
            action VARCHAR,             -- 'confirmed' | 'rejected'
            steward_note VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)


def _ensure_audit_tables(con):
    """Append-only audit trail for the gold layer (creation, edits -- manual or
    systemic, and logical deletes). No function anywhere in this codebase issues
    an UPDATE or DELETE against audit.audit_trail, and there is no API endpoint
    that could -- that omission is the enforcement mechanism (DuckDB has no
    per-table grants to lean on here, so "not editable by any role" is an
    application-level guarantee, consistent with how gold_access read/read_write
    is already enforced in api/auth.py rather than via DB permissions).

    gold_customers_snapshot lives in this same schema specifically so it survives
    every `dbt run`'s CREATE OR REPLACE of main_gold.gold_customers -- it's how
    scripts/audit_pipeline_diff.py detects what changed across a batch rebuild."""
    con.execute("CREATE SCHEMA IF NOT EXISTS audit;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.audit_trail (
            audit_id VARCHAR PRIMARY KEY,
            golden_id VARCHAR,
            change_batch_id VARCHAR,     -- groups the field-level rows from one logical operation
            event_ts TIMESTAMP,
            event_type VARCHAR,          -- 'created' | 'updated' | 'logically_deleted'
            event_source VARCHAR,        -- 'pipeline_batch' | 'portal_manual_edit' | 'steward_reprocessing'
            changed_by VARCHAR,          -- user_id, or 'system:dbt_pipeline'
            changed_by_label VARCHAR,    -- display name shown in the UI
            field_name VARCHAR,          -- null for record-level events (logical delete)
            old_value VARCHAR,
            new_value VARCHAR,
            change_reason VARCHAR,
            related_exception_id VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.gold_customers_snapshot (
            golden_id VARCHAR PRIMARY KEY,
            first_name VARCHAR, last_name VARCHAR, email VARCHAR, phone VARCHAR,
            address_line1 VARCHAR, address_line2 VARCHAR, city VARCHAR, state_code VARCHAR,
            postal_code VARCHAR, country_code VARCHAR, source_system_count INTEGER,
            survivor_source_system VARCHAR, survivor_source_record_id VARCHAR
        );
    """)


def _ensure_governance_tables(con):
    """Generic maker-checker workflow engine tables (api/workflow_engine.py).

    workflow_definitions is deliberately a plain DB table, not a dbt seed CSV
    like column_rules.csv/matching_rules.csv -- those are batch-pipeline
    metadata refreshed by `dbt seed`, but workflow definitions need to be
    editable at runtime by governance users (the planned Rules Configuration
    screen under Data Governance), so they're seeded here once and then left
    alone on every subsequent app start -- only the first boot (empty table)
    populates DEFAULT_WORKFLOW_DEFINITIONS below.

    A workflow_type is an ordered list of steps (step_order). Each step names
    the role allowed to decide at that step and how many distinct approvers
    of that role are required before the step is satisfied (approvals_required
    > 1 means a same-level quorum, e.g. 2 different Data Owners, rather than
    sequential levels). A maker can never decide on their own submission, and
    the same approver can never cast two decisions on one instance (no
    double-counting toward a quorum) -- both enforced in workflow_engine.py,
    not here."""
    con.execute("CREATE SCHEMA IF NOT EXISTS governance;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.workflow_definitions (
            workflow_type VARCHAR,
            step_order INTEGER,
            step_name VARCHAR,
            required_role VARCHAR,       -- 'admin' | 'dataSteward' | 'dataOwner' | 'businessUser'
            approvals_required INTEGER,  -- distinct approvers of required_role needed to clear this step
            is_active BOOLEAN DEFAULT true,
            PRIMARY KEY (workflow_type, step_order)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.workflow_instances (
            instance_id VARCHAR PRIMARY KEY,
            workflow_type VARCHAR,
            entity_type VARCHAR,     -- 'exception' | 'gold_customer' | 'match_pair' | 'user'
            entity_id VARCHAR,
            action_type VARCHAR,     -- 'resolve' | 'reject' | 'update' | 'confirm' | 'create' ...
            payload VARCHAR,         -- JSON: whatever the executor needs to apply the change on approval
            maker_user_id VARCHAR,
            maker_label VARCHAR,
            status VARCHAR,          -- 'pending' | 'approved' | 'rejected'
            current_step INTEGER,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            completed_ts TIMESTAMP,
            result VARCHAR           -- JSON: outcome of the executor once the workflow completes (or an error note)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS governance.workflow_decisions (
            decision_id VARCHAR PRIMARY KEY,
            instance_id VARCHAR,
            step_order INTEGER,
            actor_user_id VARCHAR,
            actor_label VARCHAR,
            decision VARCHAR,        -- 'approved' | 'rejected'
            comment VARCHAR,
            decided_ts TIMESTAMP DEFAULT current_timestamp
        );
    """)

    existing = con.execute("SELECT COUNT(*) FROM governance.workflow_definitions").fetchone()[0]
    if existing == 0:
        for row in DEFAULT_WORKFLOW_DEFINITIONS:
            con.execute("""
                INSERT INTO governance.workflow_definitions
                    (workflow_type, step_order, step_name, required_role, approvals_required, is_active)
                VALUES (?, ?, ?, ?, ?, true)
            """, row)


# Seeded once, on first boot with an empty workflow_definitions table. See
# _ensure_governance_tables' docstring for why this isn't a dbt seed CSV.
DEFAULT_WORKFLOW_DEFINITIONS = [
    # Data Stewardship: exception-queue resolve/reject. 1 level, 1 Data Owner.
    ("stewardship_remediation", 1, "Data Owner review", "dataOwner", 1),

    # Customer Portal: inline gold-record edit. 1 level, quorum of 2 Data Owners
    # (two different Data Owners must both sign off; not sequential levels).
    ("gold_record_edit", 1, "Data Owner quorum (2 of 2)", "dataOwner", 2),

    # Data Stewardship: Match Review confirm/reject. 2 sequential levels.
    ("match_review_confirmation", 1, "Data Owner review", "dataOwner", 1),
    ("match_review_confirmation", 2, "Admin sign-off", "admin", 1),

    # User Administration: create user, or update role/gold_access/is_active.
    # 1 level, 1 Admin (must be a different admin than whoever made the request).
    ("user_admin_change", 1, "Admin sign-off", "admin", 1),
]


def run_query(sql, params=None):
    with _lock:
        con = get_con()
        if params:
            return con.execute(sql, params).fetchdf()
        return con.execute(sql).fetchdf()


def to_records(df):
    """Converts a DataFrame to a list of plain-Python dicts, safely handling
    numpy arrays (from DuckDB LIST columns) and NaN/NaT values, which FastAPI's
    default JSON encoder cannot serialize on its own."""
    import numpy as np
    import pandas as pd
    records = df.to_dict(orient="records")
    for rec in records:
        for k, v in rec.items():
            if isinstance(v, np.ndarray):
                rec[k] = v.tolist()
            elif isinstance(v, float) and np.isnan(v):
                rec[k] = None
            elif v is pd.NaT:
                rec[k] = None
    return records


def run_write(sql, params=None):
    with _lock:
        con = get_con()
        if params:
            con.execute(sql, params)
        else:
            con.execute(sql)

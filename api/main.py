import json
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from db import run_query, run_write, to_records
from ai_remediation import suggest_remediation
import lineage as lineage_mod
import auth as auth_mod
from reprocessing import reprocess_corrected_record
from validation import validate_record

BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(
    title="MDM Demo API",
    description="Gold-layer REST API, lineage/impact analysis, and data stewardship endpoints "
                "for the MDM medallion architecture demo.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/api/v1/auth/login")
def login(body: LoginRequest):
    result = auth_mod.authenticate(body.username, body.password)
    if result is None:
        raise HTTPException(401, "Invalid username or password")
    return result


@app.post("/api/v1/auth/logout")
def logout_endpoint(authorization: str = Header(None)):
    if authorization and authorization.startswith("Bearer "):
        auth_mod.logout(authorization.removeprefix("Bearer ").strip())
    return {"status": "logged_out"}


@app.get("/api/v1/auth/me")
def me(user: dict = Depends(auth_mod.get_current_user)):
    return user


# ---------------------------------------------------------------------------
# (e) REST API over the Gold layer, for downstream system consumption
# ---------------------------------------------------------------------------

@app.get("/api/v1/customers")
def list_customers(
    limit: int = 50, offset: int = 0, state_code: Optional[str] = None,
    q: Optional[str] = None,
    user: dict = Depends(auth_mod.require_gold_read),
):
    clauses = []
    if state_code:
        clauses.append(f"state_code = '{state_code}'")
    if q:
        q_safe = q.replace("'", "''")
        clauses.append(
            f"(lower(first_name) LIKE lower('%{q_safe}%') OR lower(last_name) LIKE lower('%{q_safe}%') "
            f"OR lower(email) LIKE lower('%{q_safe}%') OR golden_id LIKE '%{q_safe}%')"
        )
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    df = run_query(
        f"SELECT * FROM main_gold.gold_customers {where} ORDER BY golden_id LIMIT {limit} OFFSET {offset}"
    )
    total = run_query(f"SELECT COUNT(*) AS n FROM main_gold.gold_customers {where}").iloc[0]["n"]
    return {"total": int(total), "limit": limit, "offset": offset, "customers": to_records(df)}


@app.get("/api/v1/customers/{golden_id}")
def get_customer(golden_id: str, user: dict = Depends(auth_mod.require_gold_read)):
    df = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if df.empty:
        raise HTTPException(404, f"No golden record found for {golden_id}")
    return to_records(df)[0]


@app.get("/api/v1/customers/{golden_id}/sources")
def get_customer_sources(golden_id: str, user: dict = Depends(auth_mod.require_gold_read)):
    df = run_query("SELECT * FROM main_gold.gold_crosswalk WHERE golden_id = ?", [golden_id])
    if df.empty:
        raise HTTPException(404, f"No crosswalk entries found for {golden_id}")
    return {"golden_id": golden_id, "sources": to_records(df)}


class CustomerUpdateRequest(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state_code: Optional[str] = None
    postal_code: Optional[str] = None
    country_code: Optional[str] = None


@app.put("/api/v1/customers/{golden_id}")
def update_customer(golden_id: str, body: CustomerUpdateRequest, user: dict = Depends(auth_mod.require_gold_write)):
    df = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if df.empty:
        raise HTTPException(404, f"No golden record found for {golden_id}")

    updates = {k: v for k, v in body.dict().items() if v is not None}
    if not updates:
        return to_records(df)[0]

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(
        f"UPDATE main_gold.gold_customers SET {set_clause} WHERE golden_id = ?",
        list(updates.values()) + [golden_id],
    )
    updated = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    return to_records(updated)[0]


# ---------------------------------------------------------------------------
# (d) Lineage and impact analysis
# ---------------------------------------------------------------------------

@app.get("/api/v1/lineage/impact")
def lineage_impact(layer: str, table: str, column: str, user: dict = Depends(auth_mod.get_current_user)):
    """Forward trace: what downstream tables/columns are affected by this one?"""
    return {"origin": f"{layer}.{table}.{column}", "downstream": lineage_mod.impact_analysis(layer, table, column)}


@app.get("/api/v1/lineage/trace")
def lineage_trace(layer: str, table: str, column: str, user: dict = Depends(auth_mod.get_current_user)):
    """Backward trace: where did this column's data come from?"""
    return {"target": f"{layer}.{table}.{column}", "upstream": lineage_mod.lineage_trace(layer, table, column)}


@app.get("/api/v1/lineage/record/{golden_id}")
def lineage_for_record(golden_id: str, user: dict = Depends(auth_mod.get_current_user)):
    """Full lineage for one golden record: every contributing source record."""
    result = lineage_mod.trace_golden_record(golden_id)
    if result is None:
        raise HTTPException(404, f"No golden record found for {golden_id}")
    return result


@app.get("/api/v1/lineage/graph")
def lineage_graph(user: dict = Depends(auth_mod.get_current_user)):
    """Full lineage metadata graph (all edges) for the Data Governance network diagram.
    Available to any authenticated user regardless of gold_access -- this is
    governance/metadata, not customer data."""
    df = run_query("SELECT from_layer, from_table, from_column, to_layer, to_table, to_column, "
                    "transform_rule_id, transform_description FROM main_rules.lineage_edges")
    edges = [
        {
            "from": f"{r.from_layer}.{r.from_table}.{r.from_column}",
            "to": f"{r.to_layer}.{r.to_table}.{r.to_column}",
            "rule_id": r.transform_rule_id,
            "description": r.transform_description,
        }
        for r in df.itertuples()
    ]
    return {"edges": edges}


# ---------------------------------------------------------------------------
# (c) Data Stewardship app backend, with AI-assisted remediation
# ---------------------------------------------------------------------------

@app.get("/api/v1/stewardship/queue")
def get_queue(status: str = "open", user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("""
        SELECT eq.*,
               COALESCE(o.remediation_status, eq.remediation_status) AS effective_status
        FROM main_silver.exceptions_queue eq
        LEFT JOIN stewardship.exception_status_overrides o
          ON o.exception_id = eq.exception_id
        WHERE COALESCE(o.remediation_status, eq.remediation_status) = ?
        ORDER BY eq.queued_ts
    """, [status])
    return {"status_filter": status, "count": len(df), "items": to_records(df)}


@app.get("/api/v1/stewardship/queue/{exception_id}")
def get_queue_item(exception_id: str, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")
    return to_records(df)[0]


@app.post("/api/v1/stewardship/queue/{exception_id}/suggest")
def suggest(exception_id: str, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")
    record = to_records(df)[0]
    reject_reasons = list(record.get("reject_reasons") or [])

    suggestion = suggest_remediation(record, reject_reasons)

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, suggested_fields, rationale, suggestion_source)
        VALUES (?, ?, 'ai_suggested', ?, ?, ?)
    """, [str(uuid.uuid4()), exception_id, json.dumps(suggestion["suggested_fields"]),
          suggestion["rationale"], suggestion["source"]])

    return suggestion


class ResolveRequest(BaseModel):
    applied_fields: dict
    steward_note: Optional[str] = None


@app.post("/api/v1/stewardship/queue/{exception_id}/resolve")
def resolve(exception_id: str, body: ResolveRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")
    record = to_records(df)[0]
    merged = {**record, **body.applied_fields}

    # Re-check the corrected record against the same reject-severity rules that
    # routed it here in the first place (column_rules.csv). Only a record that
    # now genuinely passes validation is allowed to flow into reprocessing --
    # otherwise it stays in the queue (still 'open') and the steward is told
    # exactly what's still failing, instead of silently trusting the click.
    still_failing = validate_record(merged)
    if still_failing:
        run_write("""
            INSERT INTO stewardship.remediation_log
                (log_id, exception_id, action, applied_fields, rationale, steward_note, suggestion_source)
            VALUES (?, ?, 'steward_resolve_blocked', ?, ?, ?, 'steward')
        """, [str(uuid.uuid4()), exception_id, json.dumps(body.applied_fields),
              "Still failing: " + "; ".join(still_failing), body.steward_note])
        raise HTTPException(422, detail={
            "message": "This record still fails validation and was not resolved. "
                       "It remains in the exception queue.",
            "still_failing": still_failing,
        })

    run_write("""
        INSERT OR REPLACE INTO stewardship.remediated_records
            (exception_id, source_system, source_record_id, first_name, last_name, email, phone,
             address_line1, address_line2, city, state_code, postal_code, country_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [exception_id, record["source_system"], record["source_record_id"],
          merged.get("first_name"), merged.get("last_name"), merged.get("email"), merged.get("phone"),
          merged.get("address_line1"), merged.get("address_line2"), merged.get("city"),
          merged.get("state_code"), merged.get("postal_code"), merged.get("country_code")])

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, applied_fields, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_resolved', ?, ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, json.dumps(body.applied_fields), body.steward_note])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'resolved')
    """, [exception_id])

    reprocess_result = reprocess_corrected_record(merged)

    return {
        "exception_id": exception_id,
        "status": "resolved",
        "applied_fields": body.applied_fields,
        "reprocessing": reprocess_result,
    }


class RejectRequest(BaseModel):
    steward_note: Optional[str] = None


@app.post("/api/v1/stewardship/queue/{exception_id}/reject")
def reject(exception_id: str, body: RejectRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_rejected', ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, body.steward_note])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'rejected')
    """, [exception_id])

    return {"exception_id": exception_id, "status": "rejected"}


# ---------------------------------------------------------------------------
# (d) Match Review queue -- borderline fuzzy-match pairs from the gold layer's
# embedding-similarity tier (scripts/generate_matches.py) that were not
# auto-merged. A data steward confirms ("same customer, merge them") or
# rejects ("coincidence, keep separate"). Same role gating as the exception
# queue: dataSteward/dataOwner only, admin deliberately excluded.
#
# Known simplification: confirm/reject only writes the steward's decision.
# It does NOT trigger real-time reprocessing -- re-clustering is a global
# recompute (union-find over the whole silver set), not a local update like
# a single corrected record, so it takes effect on the next full pipeline
# rebuild (`python scripts/generate_matches.py && dbt run --select gold.*`,
# or `python scripts/build_pipeline.py`). The response says so explicitly.
# ---------------------------------------------------------------------------

@app.get("/api/v1/stewardship/match-review")
def get_match_review_queue(status: str = "pending", user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("""
        SELECT q.*,
               COALESCE(o.status, q.review_status) AS effective_status
        FROM main_gold.gold_match_review_queue q
        LEFT JOIN stewardship.match_review_overrides o
          ON o.pair_id = q.pair_id
        WHERE COALESCE(o.status, q.review_status) = ?
        ORDER BY q.queued_ts
    """, [status])
    return {"status_filter": status, "count": len(df), "items": to_records(df)}


@app.get("/api/v1/stewardship/match-review/{pair_id}")
def get_match_review_item(pair_id: str, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_gold.gold_match_review_queue WHERE pair_id = ?", [pair_id])
    if df.empty:
        raise HTTPException(404, "Match review candidate not found")
    return to_records(df)[0]


class MatchReviewDecisionRequest(BaseModel):
    steward_note: Optional[str] = None


@app.post("/api/v1/stewardship/match-review/{pair_id}/confirm")
def confirm_match(pair_id: str, body: MatchReviewDecisionRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_gold.gold_match_review_queue WHERE pair_id = ?", [pair_id])
    if df.empty:
        raise HTTPException(404, "Match review candidate not found")

    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, 'confirmed', ?)
    """, [pair_id, body.steward_note])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, 'confirmed', ?)
    """, [str(uuid.uuid4()), pair_id, body.steward_note])

    return {
        "pair_id": pair_id,
        "status": "confirmed",
        "note": "Recorded. This pair will merge into a single golden record on the next "
                "pipeline rebuild (python scripts/generate_matches.py && dbt run --select gold.*).",
    }


@app.post("/api/v1/stewardship/match-review/{pair_id}/reject")
def reject_match(pair_id: str, body: MatchReviewDecisionRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    df = run_query("SELECT * FROM main_gold.gold_match_review_queue WHERE pair_id = ?", [pair_id])
    if df.empty:
        raise HTTPException(404, "Match review candidate not found")

    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, 'rejected', ?)
    """, [pair_id, body.steward_note])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, 'rejected', ?)
    """, [str(uuid.uuid4()), pair_id, body.steward_note])

    return {
        "pair_id": pair_id,
        "status": "rejected",
        "note": "Recorded. This pair will be permanently excluded from future matching runs "
                "and will not resurface in the review queue.",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Administration -> User Administration (admin role required)
# ---------------------------------------------------------------------------

@app.get("/api/v1/admin/users")
def list_users(admin: dict = Depends(auth_mod.require_admin)):
    df = run_query("""
        SELECT user_id, username, full_name, role, gold_access, is_active, created_ts, last_login_ts
        FROM auth.users ORDER BY created_ts
    """)
    return {"users": to_records(df)}


VALID_ROLES = ("admin", "dataSteward", "dataOwner", "businessUser")


class CreateUserRequest(BaseModel):
    username: str
    full_name: str
    role: str          # 'admin' | 'dataSteward' | 'dataOwner' | 'businessUser'
    gold_access: str   # 'read_write' | 'read' | 'none'


@app.post("/api/v1/admin/users")
def create_user_endpoint(body: CreateUserRequest, admin: dict = Depends(auth_mod.require_admin)):
    if body.role not in VALID_ROLES:
        raise HTTPException(400, f"role must be one of: {', '.join(VALID_ROLES)}")
    if body.gold_access not in ("read_write", "read", "none"):
        raise HTTPException(400, "gold_access must be one of: read_write, read, none")
    try:
        result = auth_mod.create_user(body.username, body.full_name, body.role, body.gold_access)
    except ValueError as e:
        raise HTTPException(409, str(e))
    return result  # includes the one-time temp_password


class UpdateUserRequest(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None
    gold_access: Optional[str] = None
    is_active: Optional[bool] = None


@app.put("/api/v1/admin/users/{user_id}")
def update_user(user_id: str, body: UpdateUserRequest, admin: dict = Depends(auth_mod.require_admin)):
    existing = run_query("SELECT * FROM auth.users WHERE user_id = ?", [user_id])
    if existing.empty:
        raise HTTPException(404, "User not found")

    if body.role is not None and body.role not in VALID_ROLES:
        raise HTTPException(400, f"role must be one of: {', '.join(VALID_ROLES)}")

    updates = {k: v for k, v in body.dict().items() if v is not None}
    if not updates:
        return to_records(existing)[0]

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(f"UPDATE auth.users SET {set_clause} WHERE user_id = ?", list(updates.values()) + [user_id])
    updated = run_query("""
        SELECT user_id, username, full_name, role, gold_access, is_active, created_ts, last_login_ts
        FROM auth.users WHERE user_id = ?
    """, [user_id])
    return to_records(updated)[0]


@app.post("/api/v1/admin/users/{user_id}/reset_password")
def reset_password(user_id: str, admin: dict = Depends(auth_mod.require_admin)):
    df = run_query("SELECT username FROM auth.users WHERE user_id = ?", [user_id])
    if df.empty:
        raise HTTPException(404, "User not found")
    import secrets as _secrets
    new_password = _secrets.token_urlsafe(9)
    run_write("UPDATE auth.users SET password_hash = ? WHERE user_id = ?",
              [auth_mod.hash_password(new_password), user_id])
    return {"user_id": user_id, "username": df.iloc[0]["username"], "temp_password": new_password}


app.mount("/app", StaticFiles(directory=str(BASE_DIR / "stewardship_app" / "frontend"), html=True), name="stewardship_app")
app.mount("/portal", StaticFiles(directory=str(BASE_DIR / "portal_app" / "frontend"), html=True), name="portal_app")

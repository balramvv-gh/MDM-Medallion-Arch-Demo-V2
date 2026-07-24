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
import audit as audit_mod
import workflow_engine as wf_mod
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
# Generic maker-checker workflow endpoints (api/workflow_engine.py).
#
# Data Stewardship remediation, Match Review, Portal gold-record edits, and
# User Administration create/update all submit into this same engine rather
# than applying immediately -- see the workflow_type-specific "submit" calls
# further down and their _execute_*/_rollback_* callbacks. These three
# endpoints are the one place any authenticated user goes to see what's
# awaiting their decision, see their own past submissions, and decide.
# ---------------------------------------------------------------------------

def _wf_error(e: wf_mod.WorkflowError) -> HTTPException:
    if isinstance(e, wf_mod.WorkflowNotFoundError):
        return HTTPException(404, str(e))
    if isinstance(e, wf_mod.WorkflowPermissionError):
        return HTTPException(403, str(e))
    if isinstance(e, wf_mod.WorkflowStateError):
        return HTTPException(409, str(e))
    return HTTPException(400, str(e))


@app.get("/api/v1/workflows/pending")
def list_pending_approvals(user: dict = Depends(auth_mod.get_current_user)):
    return {"items": wf_mod.list_pending_for_user(user)}


@app.get("/api/v1/workflows/mine")
def list_my_submissions(user: dict = Depends(auth_mod.get_current_user)):
    return {"items": wf_mod.list_mine(user["user_id"])}


@app.get("/api/v1/workflows/{instance_id}")
def get_workflow_instance(instance_id: str, user: dict = Depends(auth_mod.get_current_user)):
    try:
        return wf_mod.get_instance(instance_id)
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)


class WorkflowDecisionRequest(BaseModel):
    decision: str  # 'approved' | 'rejected'
    comment: Optional[str] = None


@app.post("/api/v1/workflows/{instance_id}/decide")
def decide_workflow_instance(instance_id: str, body: WorkflowDecisionRequest,
                              user: dict = Depends(auth_mod.get_current_user)):
    if body.decision not in ("approved", "rejected"):
        raise HTTPException(400, "decision must be 'approved' or 'rejected'")
    try:
        instance = wf_mod.get_instance(instance_id)
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    on_approved, on_rejected = _WORKFLOW_EXECUTORS.get(instance["workflow_type"], (None, None))
    try:
        return wf_mod.decide(instance_id, actor=user, decision=body.decision, comment=body.comment,
                              on_approved=on_approved, on_rejected=on_rejected)
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)


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


@app.get("/api/v1/customers/{golden_id}/audit-trail")
def get_customer_audit_trail(golden_id: str, user: dict = Depends(auth_mod.require_gold_read)):
    """Read-only, append-only history of every creation, edit (manual or
    systemic), and logical delete recorded for one golden record. Gated the
    same way as viewing the record itself (any gold_access of 'read' or
    'read_write') -- there is no corresponding write/delete endpoint, by design.
    Falls back to a reconstruction from the audit log itself if the record has
    been logically deleted (no longer present in main_gold.gold_customers)."""
    events = audit_mod.get_audit_trail(golden_id)
    if not events:
        raise HTTPException(404, f"No audit history found for {golden_id}")

    current_df = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if not current_df.empty:
        current = to_records(current_df)[0]
        is_active = True
    else:
        current = audit_mod.reconstruct_last_known_state(golden_id)
        is_active = False

    return {"golden_id": golden_id, "is_active": is_active, "current": current, "events": events}


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
    """Submits a gold-record edit into the maker-checker workflow instead of
    applying it directly. It only takes effect once 2 different Data Owners
    (neither of them this maker) have both approved -- see
    _execute_gold_edit below and the 'gold_record_edit' workflow_type
    (governance.workflow_definitions: 1 step, approvals_required=2)."""
    df = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if df.empty:
        raise HTTPException(404, f"No golden record found for {golden_id}")

    updates = {k: v for k, v in body.dict().items() if v is not None}
    if not updates:
        return {"golden_id": golden_id, "status": "no_changes"}

    try:
        instance = wf_mod.start_workflow(
            "gold_record_edit", entity_type="gold_customer", entity_id=golden_id, maker=user,
            action_type="update", payload={"updates": updates, "old_snapshot": to_records(df)[0]},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "golden_id": golden_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_gold_edit(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'gold_record_edit'. Applies the update the
    maker proposed, attributing the audit-trail entry to the maker (not the
    approvers) -- the maker made the edit; the Data Owners authorized it."""
    golden_id = instance["entity_id"]
    updates = instance["payload"]["updates"]
    old_snapshot = instance["payload"]["old_snapshot"]

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(f"UPDATE main_gold.gold_customers SET {set_clause} WHERE golden_id = ?",
              list(updates.values()) + [golden_id])
    updated = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    if updated.empty:
        return {"error": f"golden_id {golden_id} no longer exists (logically deleted since submission)"}

    audit_mod.log_update(
        golden_id, old_record=old_snapshot, new_record=to_records(updated)[0],
        event_source="portal_manual_edit",
        changed_by=instance["maker_user_id"], changed_by_label=instance["maker_label"],
        change_reason="Manual edit via MDM Data Hub Portal, approved by 2 Data Owners",
    )
    return {"action": "applied", "golden_id": golden_id, "updated_record": to_records(updated)[0]}


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
    """Re-validates and, if the corrected record now passes, submits it into
    the 'stewardship_remediation' maker-checker workflow (1 Data Owner,
    different from whoever resolved this) instead of reprocessing it
    immediately. The actual silver upsert + match/merge only happens once
    that Data Owner approves -- see _execute_stewardship_resolve below."""
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")
    record = to_records(df)[0]
    merged = {**record, **body.applied_fields}

    # Re-check the corrected record against the same reject-severity rules that
    # routed it here in the first place (column_rules.csv). Only a record that
    # now genuinely passes validation is allowed to be submitted for approval --
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
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, applied_fields, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_resolve_submitted', ?, ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, json.dumps(body.applied_fields), body.steward_note])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'in_review')
    """, [exception_id])

    try:
        instance = wf_mod.start_workflow(
            "stewardship_remediation", entity_type="exception", entity_id=exception_id, maker=user,
            action_type="resolve", payload={"merged": merged, "applied_fields": body.applied_fields},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "exception_id": exception_id,
        "status": "pending_approval",
        "applied_fields": body.applied_fields,
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_stewardship_resolve(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'stewardship_remediation' (action_type='resolve').
    Everything resolve() used to do immediately after validation, now deferred
    to here. Attributes the resulting reprocessing/audit entries to the
    original maker (the steward who actually made the correction), not the
    Data Owner who approved it."""
    exception_id = instance["entity_id"]
    merged = instance["payload"]["merged"]
    applied_fields = instance["payload"]["applied_fields"]
    maker = {"user_id": instance["maker_user_id"], "full_name": instance["maker_label"]}

    run_write("""
        INSERT OR REPLACE INTO stewardship.remediated_records
            (exception_id, source_system, source_record_id, first_name, last_name, email, phone,
             address_line1, address_line2, city, state_code, postal_code, country_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [exception_id, merged["source_system"], merged["source_record_id"],
          merged.get("first_name"), merged.get("last_name"), merged.get("email"), merged.get("phone"),
          merged.get("address_line1"), merged.get("address_line2"), merged.get("city"),
          merged.get("state_code"), merged.get("postal_code"), merged.get("country_code")])

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, applied_fields, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_resolved', ?, 'Approved by Data Owner', 'steward')
    """, [str(uuid.uuid4()), exception_id, json.dumps(applied_fields)])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'resolved')
    """, [exception_id])

    reprocess_result = reprocess_corrected_record(merged, actor=maker)
    return {"action": "resolved", "reprocessing": reprocess_result}


def _rollback_stewardship_submission(instance: dict, actor: dict) -> dict:
    """on_rejected callback shared by both 'resolve' and 'reject' submissions
    to 'stewardship_remediation': a Data Owner rejecting the steward's
    proposed action puts the exception back in the open queue so it can be
    reworked, rather than silently disappearing."""
    exception_id = instance["entity_id"]
    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, steward_note, suggestion_source)
        VALUES (?, ?, 'approval_rejected', ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, f"Rejected by Data Owner: {actor.get('full_name', 'unknown')}"])
    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'open')
    """, [exception_id])
    return {"action": "returned_to_open_queue"}


class RejectRequest(BaseModel):
    steward_note: Optional[str] = None


@app.post("/api/v1/stewardship/queue/{exception_id}/reject")
def reject(exception_id: str, body: RejectRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a proposal to permanently reject this exception into the same
    'stewardship_remediation' workflow as resolve() (action_type='reject')
    -- a Data Owner has to sign off on excluding a record from the pipeline
    too, not just on correcting one."""
    df = run_query("SELECT * FROM main_silver.exceptions_queue WHERE exception_id = ?", [exception_id])
    if df.empty:
        raise HTTPException(404, "Exception not found")

    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_reject_submitted', ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, body.steward_note])

    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'in_review')
    """, [exception_id])

    try:
        instance = wf_mod.start_workflow(
            "stewardship_remediation", entity_type="exception", entity_id=exception_id, maker=user,
            action_type="reject", payload={"steward_note": body.steward_note},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "exception_id": exception_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_stewardship_reject(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'stewardship_remediation' (action_type='reject')."""
    exception_id = instance["entity_id"]
    steward_note = instance["payload"].get("steward_note")
    run_write("""
        INSERT INTO stewardship.remediation_log
            (log_id, exception_id, action, steward_note, suggestion_source)
        VALUES (?, ?, 'steward_rejected', ?, 'steward')
    """, [str(uuid.uuid4()), exception_id, steward_note])
    run_write("""
        INSERT OR REPLACE INTO stewardship.exception_status_overrides (exception_id, remediation_status)
        VALUES (?, 'rejected')
    """, [exception_id])
    return {"action": "rejected"}


def _dispatch_stewardship_remediation(instance: dict, actor: dict) -> dict:
    """The 'stewardship_remediation' workflow_type covers two different
    maker actions (resolve vs. reject) sharing one approval chain; this picks
    the right on_approved executor based on the instance's action_type."""
    if instance["action_type"] == "resolve":
        return _execute_stewardship_resolve(instance, actor)
    elif instance["action_type"] == "reject":
        return _execute_stewardship_reject(instance, actor)
    raise ValueError(f"Unknown action_type '{instance['action_type']}' for stewardship_remediation")


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
    """Submits a confirm decision into the 'match_review_confirmation'
    maker-checker workflow (Data Owner, then Admin -- 2 sequential levels)
    instead of recording it immediately. See _execute_match_review below."""
    return _submit_match_review_decision(pair_id, "confirm", body, user)


@app.post("/api/v1/stewardship/match-review/{pair_id}/reject")
def reject_match(pair_id: str, body: MatchReviewDecisionRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a reject decision into the same 'match_review_confirmation'
    workflow as confirm() -- deciding these pairs are NOT duplicates still
    needs the same 2-level sign-off, not just a steward's own judgment."""
    return _submit_match_review_decision(pair_id, "reject", body, user)


def _submit_match_review_decision(pair_id: str, action_type: str, body: MatchReviewDecisionRequest, user: dict):
    df = run_query("SELECT * FROM main_gold.gold_match_review_queue WHERE pair_id = ?", [pair_id])
    if df.empty:
        raise HTTPException(404, "Match review candidate not found")

    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, 'in_review', ?)
    """, [pair_id, body.steward_note])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, ?, ?)
    """, [str(uuid.uuid4()), pair_id, f"{action_type}_submitted", body.steward_note])

    try:
        instance = wf_mod.start_workflow(
            "match_review_confirmation", entity_type="match_pair", entity_id=pair_id, maker=user,
            action_type=action_type, payload={"steward_note": body.steward_note},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "pair_id": pair_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
        "note": "Submitted for approval (Data Owner, then Admin sign-off) before this decision takes effect.",
    }


def _execute_match_review(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'match_review_confirmation' -- runs once both
    levels (Data Owner, then Admin) have signed off. Applies whichever
    decision (confirm/reject) the steward originally proposed."""
    pair_id = instance["entity_id"]
    action_type = instance["action_type"]
    steward_note = instance["payload"].get("steward_note")
    final_status = "confirmed" if action_type == "confirm" else "rejected"

    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, ?, ?)
    """, [pair_id, final_status, steward_note])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, ?, ?)
    """, [str(uuid.uuid4()), pair_id, final_status, steward_note])

    note = (
        "This pair will merge into a single golden record on the next pipeline rebuild "
        "(python scripts/generate_matches.py && dbt run --select gold.*)."
        if final_status == "confirmed" else
        "This pair is permanently excluded from future matching runs and will not resurface "
        "in the review queue."
    )
    return {"action": final_status, "pair_id": pair_id, "note": note}


def _rollback_match_review_submission(instance: dict, actor: dict) -> dict:
    """on_rejected callback: an approver rejecting the steward's proposed
    confirm/reject decision returns the pair to 'pending' so it resurfaces
    in the normal Match Review queue for a fresh look."""
    pair_id = instance["entity_id"]
    run_write("""
        INSERT OR REPLACE INTO stewardship.match_review_overrides (pair_id, status, steward_note)
        VALUES (?, 'pending', ?)
    """, [pair_id, f"Approval rejected by {actor.get('full_name', 'unknown')} ({actor.get('role')})"])
    run_write("""
        INSERT INTO stewardship.match_review_log (log_id, pair_id, action, steward_note)
        VALUES (?, ?, 'approval_rejected', ?)
    """, [str(uuid.uuid4()), pair_id, f"Rejected by {actor.get('full_name', 'unknown')} ({actor.get('role')})"])
    return {"action": "returned_to_pending_queue"}


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
    """Submits a new-user request into the 'user_admin_change' maker-checker
    workflow (1 Admin, different from whoever requested it) instead of
    creating the account immediately. The one-time password is only
    generated -- and only shown -- once that Admin approves; see
    _execute_user_create below."""
    if body.role not in VALID_ROLES:
        raise HTTPException(400, f"role must be one of: {', '.join(VALID_ROLES)}")
    if body.gold_access not in ("read_write", "read", "none"):
        raise HTTPException(400, "gold_access must be one of: read_write, read, none")

    existing = run_query("SELECT user_id FROM auth.users WHERE username = ?", [body.username])
    if not existing.empty:
        raise HTTPException(409, f"Username '{body.username}' already exists")

    try:
        instance = wf_mod.start_workflow(
            "user_admin_change", entity_type="user", entity_id=f"new:{body.username}", maker=admin,
            action_type="create",
            payload={"username": body.username, "full_name": body.full_name,
                     "role": body.role, "gold_access": body.gold_access},
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
        "note": "This user will be created once a different Admin approves the request. "
                "The one-time password is generated at approval time and shown only to that approver.",
    }


def _execute_user_create(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'user_admin_change' (action_type='create')."""
    p = instance["payload"]
    try:
        result = auth_mod.create_user(p["username"], p["full_name"], p["role"], p["gold_access"])
    except ValueError as e:
        return {"error": str(e)}
    return {"action": "created", **result}  # includes the one-time temp_password


class UpdateUserRequest(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None
    gold_access: Optional[str] = None
    is_active: Optional[bool] = None


SENSITIVE_USER_FIELDS = {"role", "gold_access", "is_active"}


@app.put("/api/v1/admin/users/{user_id}")
def update_user(user_id: str, body: UpdateUserRequest, admin: dict = Depends(auth_mod.require_admin)):
    """Non-sensitive changes (full_name only) apply immediately. Any change
    touching role, gold_access, or is_active -- the fields that actually grant
    or remove access -- is instead submitted into the 'user_admin_change'
    maker-checker workflow (1 Admin, different from whoever requested it)."""
    existing = run_query("SELECT * FROM auth.users WHERE user_id = ?", [user_id])
    if existing.empty:
        raise HTTPException(404, "User not found")

    if body.role is not None and body.role not in VALID_ROLES:
        raise HTTPException(400, f"role must be one of: {', '.join(VALID_ROLES)}")

    updates = {k: v for k, v in body.dict().items() if v is not None}
    if not updates:
        return to_records(existing)[0]

    if SENSITIVE_USER_FIELDS & updates.keys():
        try:
            instance = wf_mod.start_workflow(
                "user_admin_change", entity_type="user", entity_id=user_id, maker=admin,
                action_type="update", payload={"updates": updates},
            )
        except wf_mod.WorkflowError as e:
            raise _wf_error(e)
        return {
            "user_id": user_id,
            "status": "pending_approval",
            "workflow_instance_id": instance["instance_id"],
            "steps": instance["steps"],
        }

    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(f"UPDATE auth.users SET {set_clause} WHERE user_id = ?", list(updates.values()) + [user_id])
    updated = run_query("""
        SELECT user_id, username, full_name, role, gold_access, is_active, created_ts, last_login_ts
        FROM auth.users WHERE user_id = ?
    """, [user_id])
    return to_records(updated)[0]


def _execute_user_update(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'user_admin_change' (action_type='update')."""
    user_id = instance["entity_id"]
    updates = instance["payload"]["updates"]
    existing = run_query("SELECT user_id FROM auth.users WHERE user_id = ?", [user_id])
    if existing.empty:
        return {"error": f"user_id {user_id} no longer exists"}
    set_clause = ", ".join(f"{col} = ?" for col in updates)
    run_write(f"UPDATE auth.users SET {set_clause} WHERE user_id = ?", list(updates.values()) + [user_id])
    updated = run_query("""
        SELECT user_id, username, full_name, role, gold_access, is_active, created_ts, last_login_ts
        FROM auth.users WHERE user_id = ?
    """, [user_id])
    return {"action": "updated", "user": to_records(updated)[0]}


def _dispatch_user_admin_change(instance: dict, actor: dict) -> dict:
    """The 'user_admin_change' workflow_type covers both create and update
    actions sharing one approval chain; picks the right executor."""
    if instance["action_type"] == "create":
        return _execute_user_create(instance, actor)
    elif instance["action_type"] == "update":
        return _execute_user_update(instance, actor)
    raise ValueError(f"Unknown action_type '{instance['action_type']}' for user_admin_change")


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


# ---------------------------------------------------------------------------
# Data Governance > Reference Data Maintenance: country codes and state
# codes. Read (GET) is open to any authenticated user; write (POST) is gated
# to dataSteward/dataOwner (auth_mod.require_steward_or_owner) and every
# change -- create, update, or deactivate (is_active=false; there is no hard
# delete, same convention as auth.users/column_rules/matching_rules) -- is
# submitted into the 'reference_data_change' maker-checker workflow (1 Data
# Owner, different from whoever requested it) instead of applied directly.
# ---------------------------------------------------------------------------

REFERENCE_ENTITY_CONFIG = {
    "ref_country_code": {"table": "ref.ref_country_codes", "pk": "country_code", "name_field": "country_name"},
    "ref_state_code": {"table": "ref.ref_state_codes", "pk": "state_code", "name_field": "state_name"},
}


class ReferenceDataRequest(BaseModel):
    entity_type: str          # 'ref_country_code' | 'ref_state_code'
    code: str                 # country_code or state_code value
    name: Optional[str] = None
    is_active: Optional[bool] = None


@app.get("/api/v1/reference-data/{entity_type}")
def list_reference_data(entity_type: str, user: dict = Depends(auth_mod.get_current_user)):
    cfg = REFERENCE_ENTITY_CONFIG.get(entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown reference data entity_type '{entity_type}'")
    df = run_query(f"SELECT * FROM {cfg['table']} ORDER BY {cfg['pk']}")
    return {"entity_type": entity_type, "items": to_records(df)}


@app.post("/api/v1/reference-data")
def submit_reference_data_change(body: ReferenceDataRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a create/update/deactivate of a country or state code row into
    the 'reference_data_change' maker-checker workflow instead of writing it
    directly -- see _execute_reference_data_change below."""
    cfg = REFERENCE_ENTITY_CONFIG.get(body.entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown reference data entity_type '{body.entity_type}'")
    if not body.code or not body.code.strip():
        raise HTTPException(400, "code is required")
    code = body.code.strip().upper()

    existing = run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
    action_type = "update" if not existing.empty else "create"
    if action_type == "create" and not body.name:
        raise HTTPException(400, f"{cfg['name_field']} is required when creating a new code")

    payload = {"entity_type": body.entity_type, "code": code, "name": body.name, "is_active": body.is_active}
    try:
        instance = wf_mod.start_workflow(
            "reference_data_change", entity_type=body.entity_type, entity_id=code, maker=user,
            action_type=action_type, payload=payload,
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "entity_type": body.entity_type, "code": code,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_reference_data_change(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'reference_data_change'."""
    p = instance["payload"]
    cfg = REFERENCE_ENTITY_CONFIG.get(p["entity_type"])
    if cfg is None:
        return {"error": f"Unknown entity_type '{p['entity_type']}'"}
    code = p["code"]
    name_col = cfg["name_field"]

    if instance["action_type"] == "create":
        existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
        if not existing.empty:
            return {"error": f"{code} already exists -- nothing to create"}
        run_write(
            f"INSERT INTO {cfg['table']} ({cfg['pk']}, {name_col}, is_active, updated_by) VALUES (?, ?, ?, ?)",
            [code, p.get("name"), p.get("is_active") if p.get("is_active") is not None else True, actor.get("user_id")],
        )
        action = "created"
    else:
        existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
        if existing.empty:
            return {"error": f"{code} no longer exists -- nothing to update"}
        set_parts, params = [], []
        if p.get("name") is not None:
            set_parts.append(f"{name_col} = ?")
            params.append(p["name"])
        if p.get("is_active") is not None:
            set_parts.append("is_active = ?")
            params.append(p["is_active"])
        if not set_parts:
            return {"action": "no_changes", "code": code}
        set_parts.append("updated_ts = current_timestamp")
        set_parts.append("updated_by = ?")
        params.append(actor.get("user_id"))
        params.append(code)
        run_write(f"UPDATE {cfg['table']} SET {', '.join(set_parts)} WHERE {cfg['pk']} = ?", params)
        action = "updated"

    updated_row = run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [code])
    return {"action": action, "entity_type": p["entity_type"], "row": to_records(updated_row)[0]}


# ---------------------------------------------------------------------------
# Data Governance > Rules Configuration: column rules, matching rules,
# matching thresholds/tiers, and survivorship rules. Read (GET) is open to
# any authenticated user; write (POST) is gated to dataSteward/dataOwner
# (auth_mod.require_steward_or_owner) and every change -- create, update, or
# deactivate -- is submitted into the 'rules_config_change' maker-checker
# workflow (quorum of 2 Data Owners -- higher-risk than reference data since
# it can change what the batch pipeline rejects/matches/survives) instead of
# applied directly. One workflow_type covers all four sub-tables (they share
# the identical approval shape); `entity_type` distinguishes which table a
# given instance targets.
# ---------------------------------------------------------------------------

RULES_ENTITY_CONFIG = {
    "column_rule": {
        "table": "bus_rules.column_rules", "pk": "rule_id", "id_prefix": "CR",
        "columns": ["source_system", "source_column", "rule_type", "rule_param", "severity", "description", "is_active"],
    },
    "matching_rule": {
        "table": "bus_rules.matching_rules", "pk": "rule_id", "id_prefix": "MR",
        "columns": ["tier_id", "rule_role", "rule_order", "source_column", "transform_function", "description", "is_active"],
    },
    "matching_threshold": {
        "table": "bus_rules.matching_thresholds", "pk": "tier_id", "id_prefix": "MT",
        "columns": ["tier_order", "tier_name", "match_method", "is_match_tier",
                    "auto_merge_threshold", "review_lower_threshold", "description", "is_active"],
    },
    "survivorship_rule": {
        "table": "bus_rules.survivorship_rules", "pk": "rule_id", "id_prefix": "SR",
        "columns": ["target_column", "rule_type", "rule_param", "description", "is_active"],
    },
}


class RulesConfigRequest(BaseModel):
    entity_type: str                  # 'column_rule' | 'matching_rule' | 'matching_threshold' | 'survivorship_rule'
    entity_id: Optional[str] = None   # rule_id/tier_id of the row being updated; omit/None to create a new row
    fields: dict = {}                 # column -> new value (create: all desired columns; update: only changed ones)


@app.get("/api/v1/rules-config/{entity_type}")
def list_rules_config(entity_type: str, user: dict = Depends(auth_mod.get_current_user)):
    cfg = RULES_ENTITY_CONFIG.get(entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown rules config entity_type '{entity_type}'")
    order_col = "tier_order" if entity_type == "matching_threshold" else cfg["pk"]
    df = run_query(f"SELECT * FROM {cfg['table']} ORDER BY {order_col}")
    return {"entity_type": entity_type, "items": to_records(df)}


@app.post("/api/v1/rules-config")
def submit_rules_config_change(body: RulesConfigRequest, user: dict = Depends(auth_mod.require_steward_or_owner)):
    """Submits a create/update/deactivate of a column rule, matching rule,
    matching threshold/tier, or survivorship rule row into the
    'rules_config_change' maker-checker workflow instead of writing it
    directly -- see _execute_rules_config_change below."""
    cfg = RULES_ENTITY_CONFIG.get(body.entity_type)
    if cfg is None:
        raise HTTPException(404, f"Unknown rules config entity_type '{body.entity_type}'")

    unknown_fields = set(body.fields.keys()) - set(cfg["columns"])
    if unknown_fields:
        raise HTTPException(400, f"Unknown field(s) for {body.entity_type}: {', '.join(sorted(unknown_fields))}")

    if body.entity_id:
        existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [body.entity_id])
        if existing.empty:
            raise HTTPException(404, f"{body.entity_type} '{body.entity_id}' not found")
        action_type = "update"
        workflow_entity_id = body.entity_id
    else:
        if not body.fields:
            raise HTTPException(400, "fields must not be empty when creating a new row")
        action_type = "create"
        workflow_entity_id = f"new:{body.entity_type}:{uuid.uuid4().hex[:8]}"

    payload = {"entity_type": body.entity_type, "entity_id": body.entity_id, "fields": body.fields}
    try:
        instance = wf_mod.start_workflow(
            "rules_config_change", entity_type=body.entity_type, entity_id=workflow_entity_id, maker=user,
            action_type=action_type, payload=payload,
        )
    except wf_mod.WorkflowError as e:
        raise _wf_error(e)

    return {
        "entity_type": body.entity_type, "entity_id": body.entity_id,
        "status": "pending_approval",
        "workflow_instance_id": instance["instance_id"],
        "steps": instance["steps"],
    }


def _execute_rules_config_change(instance: dict, actor: dict) -> dict:
    """on_approved callback for 'rules_config_change'."""
    p = instance["payload"]
    cfg = RULES_ENTITY_CONFIG.get(p["entity_type"])
    if cfg is None:
        return {"error": f"Unknown entity_type '{p['entity_type']}'"}
    fields = p.get("fields") or {}

    if instance["action_type"] == "create":
        new_id = f"{cfg['id_prefix']}{uuid.uuid4().hex[:8].upper()}"
        cols = [cfg["pk"]] + list(fields.keys())
        vals = [new_id] + list(fields.values())
        placeholders = ", ".join(["?"] * len(vals))
        run_write(f"INSERT INTO {cfg['table']} ({', '.join(cols)}) VALUES ({placeholders})", vals)
        row = to_records(run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [new_id]))[0]
        return {"action": "created", "entity_type": p["entity_type"], "row": row}

    entity_id = p["entity_id"]
    existing = run_query(f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?", [entity_id])
    if existing.empty:
        return {"error": f"{entity_id} no longer exists -- nothing to update"}
    if not fields:
        return {"action": "no_changes", "entity_id": entity_id}
    set_parts = [f"{col} = ?" for col in fields]
    params = list(fields.values())
    set_parts.append("updated_ts = current_timestamp")
    set_parts.append("updated_by = ?")
    params.append(actor.get("user_id"))
    params.append(entity_id)
    run_write(f"UPDATE {cfg['table']} SET {', '.join(set_parts)} WHERE {cfg['pk']} = ?", params)
    row = to_records(run_query(f"SELECT * FROM {cfg['table']} WHERE {cfg['pk']} = ?", [entity_id]))[0]
    return {"action": "updated", "entity_type": p["entity_type"], "row": row}


# ---------------------------------------------------------------------------
# workflow_type -> (on_approved, on_rejected) executor registry, used by
# decide_workflow_instance() above. Defined last because it references
# functions declared throughout this file; only looked up at call time.
# ---------------------------------------------------------------------------
_WORKFLOW_EXECUTORS = {
    "stewardship_remediation": (_dispatch_stewardship_remediation, _rollback_stewardship_submission),
    "gold_record_edit": (_execute_gold_edit, None),
    "match_review_confirmation": (_execute_match_review, _rollback_match_review_submission),
    "user_admin_change": (_dispatch_user_admin_change, None),
    "reference_data_change": (_execute_reference_data_change, None),
    "rules_config_change": (_execute_rules_config_change, None),
}


app.mount("/app", StaticFiles(directory=str(BASE_DIR / "stewardship_app" / "frontend"), html=True), name="stewardship_app")
app.mount("/portal", StaticFiles(directory=str(BASE_DIR / "portal_app" / "frontend"), html=True), name="portal_app")

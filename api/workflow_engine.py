"""
Generic maker-checker workflow engine, shared by Data Stewardship, the
Customer Data Portal, and User Administration.

This module knows nothing about exceptions, gold records, match pairs, or
users -- it only knows about "instances" of a named workflow_type moving
through an ordered list of steps (governance.workflow_definitions, seeded in
api/db.py) until every step is satisfied (status -> 'approved') or a checker
rejects it at any step (status -> 'rejected'). Callers (api/main.py) supply
`on_approved` / `on_rejected` callbacks that do the actual domain-specific
work -- e.g. reprocessing a corrected customer record, applying a gold-record
edit, or creating a user -- so this module stays reusable across all three.

Core rules, enforced here (not left to callers):
  - A maker can never decide on their own submission.
  - A single approver can never cast two decisions on the same instance --
    so a quorum step (approvals_required > 1, e.g. "2 different Data Owners")
    can't be satisfied by one person deciding twice.
  - Only the role named on the current step may decide at that step.
  - Rejection at any step is terminal -- the instance does not advance further.
  - A step is cleared once `approvals_required` distinct approvals are
    recorded for it; the instance then either advances to the next step_order
    or, if this was the last step, calls `on_approved` and completes.

Known simplification: if `on_approved` itself raises (the domain executor
fails to apply the change after every human has already signed off), the
instance is still marked 'approved' -- the humans' decision stands -- but
`result` records the executor error instead of the normal outcome, so it's
visible via GET /api/v1/workflows/{id} rather than silently lost. There is no
automatic retry; this mirrors how the rest of this demo treats a failed
mechanical step as something to surface, not hide (see reprocessing.py).
"""
import json
import uuid
from datetime import datetime

from db import run_query, run_write, to_records


def _json_default(obj):
    """json.dumps `default=` fallback for payload/result dicts that may
    contain pandas/numpy values passed straight through from a DataFrame row
    (e.g. a pandas.Timestamp column like queued_ts or gold_curated_ts, or a
    numpy scalar) -- db.py's to_records() only normalizes numpy arrays and
    NaN/NaT, not every pandas/numpy scalar type, so this is the safety net
    rather than requiring every caller to pre-sanitize its payload."""
    if hasattr(obj, "item"):  # numpy scalar types (int64, float64, bool_, ...)
        return obj.item()
    if hasattr(obj, "isoformat"):  # datetime, date, pandas.Timestamp
        return obj.isoformat()
    return str(obj)


def _dumps(obj) -> str:
    return json.dumps(obj, default=_json_default)


class WorkflowError(Exception):
    """Base class; api/main.py maps this to HTTP 400 unless a subclass below applies."""


class WorkflowNotFoundError(WorkflowError):
    pass


class WorkflowPermissionError(WorkflowError):
    """Wrong role for the current step, or a maker trying to decide on their own submission."""


class WorkflowStateError(WorkflowError):
    """Instance isn't pending, or this actor already decided on it."""


def get_steps(workflow_type: str) -> list[dict]:
    df = run_query("""
        SELECT * FROM governance.workflow_definitions
        WHERE workflow_type = ? AND is_active
        ORDER BY step_order
    """, [workflow_type])
    return to_records(df)


def start_workflow(workflow_type: str, entity_type: str, entity_id: str, maker: dict,
                    action_type: str, payload: dict) -> dict:
    """Creates a new pending instance at step 1. Raises WorkflowError if
    `workflow_type` has no active steps configured -- same posture as
    reprocessing.py raising RuntimeError on missing matching metadata: a
    workflow with nothing to gate is a configuration bug, not a no-op."""
    steps = get_steps(workflow_type)
    if not steps:
        raise WorkflowError(
            f"No active steps configured for workflow_type='{workflow_type}' in "
            f"governance.workflow_definitions -- cannot start this workflow."
        )

    instance_id = str(uuid.uuid4())
    now = datetime.utcnow()
    run_write("""
        INSERT INTO governance.workflow_instances
            (instance_id, workflow_type, entity_type, entity_id, action_type, payload,
             maker_user_id, maker_label, status, current_step, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
    """, [instance_id, workflow_type, entity_type, entity_id, action_type, _dumps(payload),
          maker.get("user_id"), maker.get("full_name") or maker.get("username", "unknown"),
          steps[0]["step_order"], now, now])

    return get_instance(instance_id)


def _row_to_instance(row: dict) -> dict:
    row = dict(row)
    row["payload"] = json.loads(row["payload"]) if row.get("payload") else {}
    row["result"] = json.loads(row["result"]) if row.get("result") else None
    return row


def get_instance(instance_id: str) -> dict:
    df = run_query("SELECT * FROM governance.workflow_instances WHERE instance_id = ?", [instance_id])
    if df.empty:
        raise WorkflowNotFoundError(f"No workflow instance found for {instance_id}")
    instance = _row_to_instance(to_records(df)[0])
    instance["steps"] = get_steps(instance["workflow_type"])

    dec_df = run_query("""
        SELECT * FROM governance.workflow_decisions WHERE instance_id = ? ORDER BY decided_ts
    """, [instance_id])
    instance["decisions"] = to_records(dec_df)
    return instance


def list_mine(maker_user_id: str) -> list[dict]:
    df = run_query("""
        SELECT * FROM governance.workflow_instances
        WHERE maker_user_id = ? ORDER BY created_ts DESC
    """, [maker_user_id])
    return [_row_to_instance(r) for r in to_records(df)]


def list_pending_for_user(user: dict) -> list[dict]:
    """Every pending instance currently awaiting a decision from someone with
    this user's role, excluding instances this user made themselves, and
    excluding instances this user has already decided on (relevant for
    quorum steps, so one approver can't see -- or re-decide -- a step
    they've already cast a vote on)."""
    df = run_query("""
        SELECT i.*
        FROM governance.workflow_instances i
        JOIN governance.workflow_definitions d
          ON d.workflow_type = i.workflow_type AND d.step_order = i.current_step AND d.is_active
        WHERE i.status = 'pending'
          AND d.required_role = ?
          AND i.maker_user_id != ?
          AND NOT EXISTS (
              SELECT 1 FROM governance.workflow_decisions dec
              WHERE dec.instance_id = i.instance_id
                AND dec.step_order = i.current_step
                AND dec.actor_user_id = ?
          )
        ORDER BY i.created_ts
    """, [user["role"], user["user_id"], user["user_id"]])
    instances = [_row_to_instance(r) for r in to_records(df)]
    for inst in instances:
        inst["steps"] = get_steps(inst["workflow_type"])
    return instances


def decide(instance_id: str, actor: dict, decision: str, comment: str = None,
           on_approved=None, on_rejected=None) -> dict:
    """`decision` must be 'approved' or 'rejected'. `on_approved(instance, actor)` /
    `on_rejected(instance, actor)` are optional callables; their return value
    (a JSON-serializable dict) is stored in the instance's `result` column."""
    if decision not in ("approved", "rejected"):
        raise WorkflowError("decision must be 'approved' or 'rejected'")

    instance = get_instance(instance_id)
    if instance["status"] != "pending":
        raise WorkflowStateError(
            f"Workflow instance {instance_id} is already '{instance['status']}' -- no further decisions accepted."
        )

    step = next((s for s in instance["steps"] if s["step_order"] == instance["current_step"]), None)
    if step is None:
        raise WorkflowError(
            f"Instance {instance_id} is at step {instance['current_step']}, which has no active "
            f"definition in governance.workflow_definitions for workflow_type='{instance['workflow_type']}'."
        )

    if actor["role"] != step["required_role"]:
        raise WorkflowPermissionError(
            f"This step requires role '{step['required_role']}'; you are '{actor['role']}'."
        )
    if actor["user_id"] == instance["maker_user_id"]:
        raise WorkflowPermissionError("You submitted this request and cannot approve or reject your own submission.")

    prior = run_query("""
        SELECT 1 FROM governance.workflow_decisions
        WHERE instance_id = ? AND step_order = ? AND actor_user_id = ?
    """, [instance_id, step["step_order"], actor["user_id"]])
    if not prior.empty:
        raise WorkflowStateError("You have already recorded a decision at this step.")

    now = datetime.utcnow()
    run_write("""
        INSERT INTO governance.workflow_decisions
            (decision_id, instance_id, step_order, actor_user_id, actor_label, decision, comment, decided_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [str(uuid.uuid4()), instance_id, step["step_order"], actor["user_id"],
          actor.get("full_name") or actor.get("username", "unknown"), decision, comment, now])

    if decision == "rejected":
        result = None
        if on_rejected:
            result = on_rejected(instance, actor)
        run_write("""
            UPDATE governance.workflow_instances
            SET status = 'rejected', updated_ts = ?, completed_ts = ?, result = ?
            WHERE instance_id = ?
        """, [now, now, _dumps(result) if result is not None else None, instance_id])
        return get_instance(instance_id)

    # decision == 'approved': check whether this step's quorum is now satisfied.
    approvals = run_query("""
        SELECT COUNT(*) AS n FROM governance.workflow_decisions
        WHERE instance_id = ? AND step_order = ? AND decision = 'approved'
    """, [instance_id, step["step_order"]]).iloc[0]["n"]

    if int(approvals) < int(step["approvals_required"]):
        run_write("UPDATE governance.workflow_instances SET updated_ts = ? WHERE instance_id = ?", [now, instance_id])
        return get_instance(instance_id)

    remaining_steps = [s for s in instance["steps"] if s["step_order"] > step["step_order"]]
    if remaining_steps:
        next_step = min(s["step_order"] for s in remaining_steps)
        run_write("""
            UPDATE governance.workflow_instances SET current_step = ?, updated_ts = ? WHERE instance_id = ?
        """, [next_step, now, instance_id])
        return get_instance(instance_id)

    # Last step, quorum satisfied: complete the workflow.
    result = None
    if on_approved:
        try:
            result = on_approved(instance, actor)
        except Exception as e:
            result = {"executor_error": str(e)}
    run_write("""
        UPDATE governance.workflow_instances
        SET status = 'approved', updated_ts = ?, completed_ts = ?, result = ?
        WHERE instance_id = ?
    """, [now, now, _dumps(result) if result is not None else None, instance_id])
    return get_instance(instance_id)

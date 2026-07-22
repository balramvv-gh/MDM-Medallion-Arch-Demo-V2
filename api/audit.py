"""
Append-only audit trail for the gold layer.

Every row written here is either part of a "creation" batch (one row per
non-null tracked field, old_value=NULL), an "update" batch (one row per field
that actually changed), or a single record-level "logically_deleted" row
(emitted only by the batch pipeline diff step, scripts/audit_pipeline_diff.py,
when a golden_id present in the previous build no longer exists in the new
one). Rows from one logical operation share a change_batch_id so the portal
UI can render them as a single grouped timeline entry with expandable
field-level diffs.

Three write paths feed this module:
  - api/main.py's update_customer()      -> event_source='portal_manual_edit'
  - api/reprocessing.py                  -> event_source='steward_reprocessing'
  - scripts/audit_pipeline_diff.py       -> event_source='pipeline_batch'
    (a standalone script -- it duplicates the DDL and insert logic here rather
    than importing this module, matching this project's existing convention
    for standalone scripts, see scripts/generate_matches.py's docstring.)

No function in this module issues an UPDATE or DELETE against audit.audit_trail.
Once written, a row is permanent, and there is deliberately no API endpoint
that could mutate or remove one.
"""
import uuid
from datetime import datetime

from db import run_write, run_query, to_records

TRACKED_FIELDS = [
    "first_name", "last_name", "email", "phone",
    "address_line1", "address_line2", "city", "state_code",
    "postal_code", "country_code",
    "source_system_count", "survivor_source_system", "survivor_source_record_id",
]


def _s(v):
    """Stringify for storage/comparison; None stays None (never the string 'None')."""
    return None if v is None else str(v)


def log_creation(golden_id, record: dict, event_source, changed_by, changed_by_label,
                  change_reason, related_exception_id=None, event_ts=None):
    """Logs one field-level 'created' row per non-null tracked field in `record`,
    all sharing a single change_batch_id."""
    batch_id = str(uuid.uuid4())
    ts = event_ts or datetime.utcnow()
    rows = []
    for field in TRACKED_FIELDS:
        val = _s(record.get(field))
        if val is None:
            continue
        rows.append((str(uuid.uuid4()), golden_id, batch_id, ts, "created", event_source,
                     changed_by, changed_by_label, field, None, val, change_reason, related_exception_id))
    _insert_rows(rows)


def log_update(golden_id, old_record: dict, new_record: dict, event_source, changed_by,
                changed_by_label, change_reason, related_exception_id=None, event_ts=None):
    """Diffs old_record vs new_record over TRACKED_FIELDS and logs one row per
    field that actually changed, all sharing a single change_batch_id. Writes
    nothing if no tracked field actually changed. Returns the number of fields
    logged as changed."""
    batch_id = str(uuid.uuid4())
    ts = event_ts or datetime.utcnow()
    rows = []
    for field in TRACKED_FIELDS:
        old_val = _s(old_record.get(field))
        new_val = _s(new_record.get(field))
        if old_val == new_val:
            continue
        rows.append((str(uuid.uuid4()), golden_id, batch_id, ts, "updated", event_source,
                     changed_by, changed_by_label, field, old_val, new_val, change_reason, related_exception_id))
    _insert_rows(rows)
    return len(rows)


def log_logical_delete(golden_id, event_source, changed_by, changed_by_label,
                        change_reason, event_ts=None):
    ts = event_ts or datetime.utcnow()
    _insert_rows([(str(uuid.uuid4()), golden_id, str(uuid.uuid4()), ts, "logically_deleted",
                   event_source, changed_by, changed_by_label, None, None, None, change_reason, None)])


def _insert_rows(rows):
    for row in rows:
        run_write("""
            INSERT INTO audit.audit_trail
                (audit_id, golden_id, change_batch_id, event_ts, event_type, event_source,
                 changed_by, changed_by_label, field_name, old_value, new_value, change_reason,
                 related_exception_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, list(row))


def get_audit_trail(golden_id: str) -> list:
    """Every audit row for a golden_id, oldest first (callers group by
    change_batch_id for display)."""
    df = run_query("""
        SELECT * FROM audit.audit_trail WHERE golden_id = ? ORDER BY event_ts ASC, field_name ASC
    """, [golden_id])
    return to_records(df)


def reconstruct_last_known_state(golden_id: str) -> dict:
    """Fallback header info for when the golden record no longer exists in
    main_gold.gold_customers (logically deleted) -- takes each tracked field's
    most recently logged new_value."""
    df = run_query("""
        SELECT field_name, new_value FROM audit.audit_trail
        WHERE golden_id = ? AND field_name IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY field_name ORDER BY event_ts DESC) = 1
    """, [golden_id])
    return {row["field_name"]: row["new_value"] for row in to_records(df)}

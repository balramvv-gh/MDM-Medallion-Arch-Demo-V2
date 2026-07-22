"""
Audit-trail diff step for the batch pipeline.

Runs after `dbt run --select gold.*` (see scripts/build_pipeline.py, final
step). Compares the just-rebuilt main_gold.gold_customers against a snapshot
of the previous build (audit.gold_customers_snapshot -- a table in a schema
dbt never touches, so it survives every `dbt run`'s CREATE OR REPLACE of the
gold schema), and writes append-only rows to audit.audit_trail for every
golden_id that:

  - is new since the last snapshot                          -> 'created'
  - existed before and has one or more changed tracked
    fields (survivorship/match recompute)                   -> 'updated'
  - existed before but is no longer produced by this build   -> 'logically_deleted'

Then refreshes the snapshot to the current state.

This is the ONLY place that captures pipeline-driven ("systemic") gold-layer
changes -- api/audit.py's log_creation/log_update cover the two live, in-app
write paths (portal manual edit, steward real-time reprocessing). This script
duplicates the audit table DDL rather than importing api/audit.py or api/db.py,
matching this project's existing convention for standalone scripts owning
their own idempotent DDL (see scripts/generate_matches.py's docstring on
stewardship.match_review_* -- same rationale applies here).

Known limitation (consistent with reprocessing.py's documented divergence): a
golden_id created via real-time reprocessing can be renumbered by the next
full `dbt run` (dbt's match-group numbering is a fresh dense_rank over the
complete silver set on every run). When that happens, this diff logs the old
number as 'logically_deleted' and the new number as 'created' -- even though
it's the same underlying customer. That's an accepted tradeoff of this demo's
golden ID numbering scheme, not a bug in the diff logic itself.

Usage: python scripts/audit_pipeline_diff.py
(called automatically as the final step of scripts/build_pipeline.py)
"""
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

TRACKED_FIELDS = [
    "first_name", "last_name", "email", "phone",
    "address_line1", "address_line2", "city", "state_code",
    "postal_code", "country_code",
    "source_system_count", "survivor_source_system", "survivor_source_record_id",
]

CHANGED_BY = "system:dbt_pipeline"
CHANGED_BY_LABEL = "Batch Pipeline (dbt run)"


def _ensure_audit_tables(con):
    """Same idempotent-DDL pattern as api/db.py's _ensure_audit_tables --
    whichever of this script or the API runs first creates them, both must
    stay column-for-column identical."""
    con.execute("CREATE SCHEMA IF NOT EXISTS audit;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit.audit_trail (
            audit_id VARCHAR PRIMARY KEY,
            golden_id VARCHAR,
            change_batch_id VARCHAR,
            event_ts TIMESTAMP,
            event_type VARCHAR,
            event_source VARCHAR,
            changed_by VARCHAR,
            changed_by_label VARCHAR,
            field_name VARCHAR,
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


def _s(v):
    return None if v is None else str(v)


def main():
    con = duckdb.connect(DB_PATH)
    _ensure_audit_tables(con)

    snapshot_was_empty = con.execute("SELECT COUNT(*) FROM audit.gold_customers_snapshot").fetchone()[0] == 0

    field_list = ", ".join(TRACKED_FIELDS)
    prev = {
        row[0]: dict(zip(TRACKED_FIELDS, row[1:]))
        for row in con.execute(f"SELECT golden_id, {field_list} FROM audit.gold_customers_snapshot").fetchall()
    }
    curr = {
        row[0]: dict(zip(TRACKED_FIELDS, row[1:]))
        for row in con.execute(f"SELECT golden_id, {field_list} FROM main_gold.gold_customers").fetchall()
    }

    now = datetime.now(timezone.utc)
    audit_rows = []
    created_count = updated_count = deleted_count = 0

    for golden_id, rec in curr.items():
        if golden_id not in prev:
            batch_id = str(uuid.uuid4())
            reason = (
                "Existing gold record captured on audit trail initialization "
                "(pre-dates audit tracking; true creation time unknown)"
                if snapshot_was_empty else
                "Initial gold record created by pipeline batch run"
            )
            for field in TRACKED_FIELDS:
                val = _s(rec.get(field))
                if val is None:
                    continue
                audit_rows.append((str(uuid.uuid4()), golden_id, batch_id, now, "created", "pipeline_batch",
                                    CHANGED_BY, CHANGED_BY_LABEL, field, None, val, reason, None))
            created_count += 1
        else:
            old = prev[golden_id]
            batch_id = str(uuid.uuid4())
            changed_any = False
            for field in TRACKED_FIELDS:
                old_val, new_val = _s(old.get(field)), _s(rec.get(field))
                if old_val == new_val:
                    continue
                changed_any = True
                audit_rows.append((str(uuid.uuid4()), golden_id, batch_id, now, "updated", "pipeline_batch",
                                    CHANGED_BY, CHANGED_BY_LABEL, field, old_val, new_val,
                                    "Recomputed via pipeline batch run (match/survivorship recompute)", None))
            if changed_any:
                updated_count += 1

    for golden_id in prev:
        if golden_id not in curr:
            audit_rows.append((str(uuid.uuid4()), golden_id, str(uuid.uuid4()), now, "logically_deleted",
                                "pipeline_batch", CHANGED_BY, CHANGED_BY_LABEL, None, None, None,
                                "Golden record no longer produced by the pipeline (contributing source "
                                "record(s) removed, or record merged into a different golden ID during "
                                "re-matching)", None))
            deleted_count += 1

    if audit_rows:
        con.executemany("""
            INSERT INTO audit.audit_trail
                (audit_id, golden_id, change_batch_id, event_ts, event_type, event_source,
                 changed_by, changed_by_label, field_name, old_value, new_value, change_reason,
                 related_exception_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, audit_rows)

    con.execute("DELETE FROM audit.gold_customers_snapshot;")
    con.execute(f"INSERT INTO audit.gold_customers_snapshot SELECT golden_id, {field_list} FROM main_gold.gold_customers")

    con.close()
    print(f"Audit diff complete: {created_count} created, {updated_count} updated, "
          f"{deleted_count} logically deleted (of {len(curr)} current gold records).")


if __name__ == "__main__":
    main()

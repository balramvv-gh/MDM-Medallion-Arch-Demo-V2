"""
Real-time reprocessing path for records corrected by a data steward.

This module is only ever invoked by api/main.py's `resolve()` endpoint AFTER
the corrected record has already passed re-validation (see api/validation.py)
against the same reject-severity rules that routed it to the exception queue
in the first place. By the time `reprocess_corrected_record` runs, the record
is confirmed valid -- it is not this module's job to re-check that.

When a steward's correction passes re-validation, this module:
  1. Upserts the corrected record into the silver layer (main_silver.silver_customers) --
     it is now treated as canonical, on the strength of having passed re-validation.
  2. Matches the corrected record against the CURRENT set of golden records, using the
     same deterministic strategy as the batch dbt gold_match_candidates model
     (normalized email OR normalized phone).
  3. Runs survivorship:
       - If matched: recomputes the survivor across the whole contributing group
         (most-recently-modified source wins, CRM preferred as tiebreak) and updates
         the existing golden record + crosswalk in place.
       - If not matched: creates a brand-new golden record with this record as its
         sole (and therefore survivor) source.

Design notes / known simplifications (consistent with the rest of this demo):
  - Re-validation (api/validation.py) only re-checks the reject-severity rules
    from column_rules.csv (the ones that determine silver eligibility), not the
    correct-severity standardization rules (proper-casing, phone formatting) --
    those are cosmetic and don't gate silver eligibility in the batch pipeline
    either. A steward's sign-off on a now-valid record is still what makes the
    correction authoritative; the record just has to actually be valid first.
  - The corrected record's "modified" timestamp is stamped as the moment of
    correction. Combined with the recency-wins survivorship rule, this means a fresh
    steward correction will typically become the new survivor -- which is usually the
    desired behavior (a just-verified value should be trusted as current).
  - Matching compares the corrected record's email/phone against each existing golden
    record's current (survivor) email/phone, not against every historical non-survivor
    member of that group. This mirrors the batch gold layer's record-level (not
    attribute-level) survivorship design; a follow-on iteration could match against
    every underlying source row for full precision.
  - New golden IDs are assigned by incrementing the current max -- this can diverge
    from the numbering a full `dbt run` would produce from scratch (dbt's dense_rank
    is order-dependent on the complete silver set). Both paths remain internally
    consistent; the numbers themselves just aren't guaranteed portable between them.
"""
import re
from datetime import datetime

import pandas as pd

from db import run_query, run_write


def _normalize_email(email):
    return (email or "").strip().lower()


def _normalize_phone(phone):
    return re.sub(r"[^0-9]", "", phone or "")


def _epoch(dt):
    """Comparable numeric timestamp; treats missing/unparseable dates as oldest possible."""
    if dt is None:
        return -1
    ts = pd.to_datetime(dt, errors="coerce")
    if pd.isna(ts):
        return -1
    return ts.timestamp()


def reprocess_corrected_record(record: dict) -> dict:
    """Entry point called after a steward resolves an exception. `record` must contain
    source_system, source_record_id, and the (possibly corrected) customer fields."""
    source_system = record["source_system"]
    source_record_id = record["source_record_id"]
    now = datetime.utcnow()

    # --- (a) Upsert into the silver layer -----------------------------------
    run_write(
        "DELETE FROM main_silver.silver_customers WHERE source_system = ? AND source_record_id = ?",
        [source_system, source_record_id],
    )
    run_write("""
        INSERT INTO main_silver.silver_customers
            (source_system, source_record_id, first_name, last_name, email, phone,
             address_line1, address_line2, city, state_code, postal_code, country_code,
             source_created_date, source_modified_date, silver_load_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [source_system, source_record_id, record.get("first_name"), record.get("last_name"),
          record.get("email"), record.get("phone"), record.get("address_line1"),
          record.get("address_line2"), record.get("city"), record.get("state_code"),
          record.get("postal_code"), record.get("country_code"), now, now, now])

    # --- (b) Match against the current gold set ------------------------------
    match_email = _normalize_email(record.get("email"))
    match_phone = _normalize_phone(record.get("phone"))

    matched = run_query("""
        SELECT DISTINCT golden_id FROM main_gold.gold_customers
        WHERE (? != '' AND lower(trim(email)) = ?)
           OR (? != '' AND regexp_replace(coalesce(phone,''), '[^0-9]', '', 'g') = ?)
    """, [match_email, match_email, match_phone, match_phone])

    # --- (c) Survivorship: update existing golden record, or create a new one --
    if not matched.empty:
        golden_id = matched.iloc[0]["golden_id"]
        return _update_existing_golden_record(golden_id, source_system, source_record_id, record, now)
    else:
        return _create_new_golden_record(source_system, source_record_id, record, now)


def _create_new_golden_record(source_system, source_record_id, record, now) -> dict:
    golden_id = _next_golden_id()

    run_write("""
        INSERT INTO main_gold.gold_customers
            (golden_id, first_name, last_name, email, phone, address_line1, address_line2,
             city, state_code, postal_code, country_code, source_system_count,
             survivor_source_system, survivor_source_record_id, gold_curated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
    """, [golden_id, record.get("first_name"), record.get("last_name"), record.get("email"),
          record.get("phone"), record.get("address_line1"), record.get("address_line2"),
          record.get("city"), record.get("state_code"), record.get("postal_code"),
          record.get("country_code"), source_system, source_record_id, now])

    run_write("""
        INSERT INTO main_gold.gold_crosswalk
            (golden_id, source_system, source_record_id, match_confidence_score, is_survivor_record, crosswalk_created_ts)
        VALUES (?, ?, ?, 1.00, true, ?)
    """, [golden_id, source_system, source_record_id, now])

    return {
        "action": "created_new_golden_record",
        "golden_id": golden_id,
        "is_survivor": True,
        "matched_existing_sources": 0,
    }


def _update_existing_golden_record(golden_id, source_system, source_record_id, record, now) -> dict:
    # Idempotency: drop any stale crosswalk row for this exact source before recomputing.
    run_write(
        "DELETE FROM main_gold.gold_crosswalk WHERE golden_id = ? AND source_system = ? AND source_record_id = ?",
        [golden_id, source_system, source_record_id],
    )

    existing_crosswalk = run_query(
        "SELECT * FROM main_gold.gold_crosswalk WHERE golden_id = ?", [golden_id]
    )

    candidates = []
    for _, row in existing_crosswalk.iterrows():
        s = run_query(
            "SELECT * FROM main_silver.silver_customers WHERE source_system = ? AND source_record_id = ?",
            [row["source_system"], row["source_record_id"]],
        )
        if not s.empty:
            srow = s.iloc[0]
            candidates.append({
                "source_system": row["source_system"],
                "source_record_id": row["source_record_id"],
                "modified_date": srow.get("source_modified_date"),
                "data": srow.to_dict(),
            })

    candidates.append({
        "source_system": source_system,
        "source_record_id": source_record_id,
        "modified_date": now,
        "data": record,
    })

    def sort_key(c):
        crm_preferred = 0 if c["source_system"] == "CRM" else 1
        return (-_epoch(c["modified_date"]), crm_preferred)

    candidates.sort(key=sort_key)
    survivor = candidates[0]
    survivor_data = survivor["data"]
    distinct_sources = {c["source_system"] for c in candidates}

    run_write("""
        UPDATE main_gold.gold_customers SET
            first_name = ?, last_name = ?, email = ?, phone = ?, address_line1 = ?,
            address_line2 = ?, city = ?, state_code = ?, postal_code = ?, country_code = ?,
            source_system_count = ?, survivor_source_system = ?, survivor_source_record_id = ?,
            gold_curated_ts = ?
        WHERE golden_id = ?
    """, [survivor_data.get("first_name"), survivor_data.get("last_name"), survivor_data.get("email"),
          survivor_data.get("phone"), survivor_data.get("address_line1"), survivor_data.get("address_line2"),
          survivor_data.get("city"), survivor_data.get("state_code"), survivor_data.get("postal_code"),
          survivor_data.get("country_code"), len(distinct_sources),
          survivor["source_system"], survivor["source_record_id"], now, golden_id])

    # Recompute is_survivor_record across the whole group (the survivor may have changed).
    for c in candidates:
        is_surv = (c["source_system"] == survivor["source_system"]
                   and c["source_record_id"] == survivor["source_record_id"])
        run_write(
            "DELETE FROM main_gold.gold_crosswalk WHERE golden_id = ? AND source_system = ? AND source_record_id = ?",
            [golden_id, c["source_system"], c["source_record_id"]],
        )
        run_write("""
            INSERT INTO main_gold.gold_crosswalk
                (golden_id, source_system, source_record_id, match_confidence_score, is_survivor_record, crosswalk_created_ts)
            VALUES (?, ?, ?, 1.00, ?, ?)
        """, [golden_id, c["source_system"], c["source_record_id"], is_surv, now])

    new_source_is_survivor = (survivor["source_system"] == source_system
                               and survivor["source_record_id"] == source_record_id)
    return {
        "action": "updated_existing_golden_record",
        "golden_id": golden_id,
        "is_survivor": new_source_is_survivor,
        "matched_existing_sources": len(candidates) - 1,
    }


def _next_golden_id() -> str:
    df = run_query("SELECT golden_id FROM main_gold.gold_customers")
    max_n = 0
    for gid in df["golden_id"]:
        try:
            n = int(str(gid).split("-")[1])
            max_n = max(max_n, n)
        except (IndexError, ValueError):
            continue
    return f"GOLD-{max_n + 1:05d}"

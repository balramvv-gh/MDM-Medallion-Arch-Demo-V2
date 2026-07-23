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
     tier-1 (exact_match_field) rules from main_rules.matching_rules -- the same
     metadata the batch dbt gold_match_candidates model's tier 1 reads (today:
     normalized email OR normalized phone, see dbt_project/seeds/matching_rules.csv).
     Confidence for a tier-1 match is that tier's auto_merge_threshold, read from
     main_rules.matching_thresholds -- not a hardcoded literal.
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
  - Matching is metadata-driven: the exact_match_field rows for the 'exact'
    tier in main_rules.matching_rules determine which columns are compared
    (and how each is normalized, via that rule's transform_function) -- see
    _load_tier1_matching_metadata() below. This module only ever evaluates
    the 'exact' tier; a 'fuzzy_tfidf_cosine' tier (if any) is intentionally
    skipped here -- fitting a TF-IDF vectorizer per API request would be a
    real latency/complexity cost for a demo-scoped real-time path. This is
    the same documented divergence as before, just no longer duplicated as
    hardcoded email/phone logic that could drift from the batch script.
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
import audit as audit_mod

# Same transform_function registry as scripts/generate_matches.py (Python-side
# values). SQL_TRANSFORMS below is the equivalent for building the WHERE
# clause against DuckDB directly -- kept alongside deliberately so both stay
# in lockstep as new transform_function values are added to matching_rules.csv.
PY_TRANSFORMS = {
    "none": lambda v: v,
    "normalize_email": lambda v: (v or "").strip().lower(),
    "normalize_phone": lambda v: re.sub(r"[^0-9]", "", v or ""),
}

SQL_TRANSFORMS = {
    "none": lambda col: col,
    "normalize_email": lambda col: f"lower(trim({col}))",
    "normalize_phone": lambda col: f"regexp_replace(coalesce({col},''), '[^0-9]', '', 'g')",
}


def _load_tier1_matching_metadata():
    """Reads the active 'exact' tier from main_rules.matching_thresholds and its
    exact_match_field rules from main_rules.matching_rules. Raises if no active
    exact tier is configured -- this module has nothing meaningful to do
    without one."""
    tiers = run_query("""
        SELECT * FROM main_rules.matching_thresholds
        WHERE active AND is_match_tier AND match_method = 'exact'
        ORDER BY tier_order
    """)
    if tiers.empty:
        raise RuntimeError(
            "No active tier with match_method='exact' found in "
            "main_rules.matching_thresholds -- real-time reprocessing has no "
            "matching rule to apply."
        )
    tier1 = tiers.iloc[0]

    rules = run_query("""
        SELECT * FROM main_rules.matching_rules
        WHERE active AND tier_id = ? AND rule_role = 'exact_match_field'
        ORDER BY rule_order
    """, [tier1["tier_id"]])

    return float(tier1["auto_merge_threshold"]), rules.to_dict(orient="records")


def _load_baseline_confidence():
    """Reads the non-tier 'no_match_baseline' row from main_rules.matching_thresholds
    -- the confidence assigned to a golden record with no corroborating match at
    all (single source, e.g. a brand-new record created by this module when no
    existing golden record matched). Mirrors the batch gold_crosswalk.sql fallback
    for the identical situation, so the two paths can't drift on this value."""
    baseline = run_query("""
        SELECT * FROM main_rules.matching_thresholds
        WHERE active AND NOT is_match_tier AND match_method = 'no_match_baseline'
    """)
    if baseline.empty:
        raise RuntimeError(
            "No active 'no_match_baseline' row found in main_rules.matching_thresholds."
        )
    return float(baseline.iloc[0]["auto_merge_threshold"])


def _epoch(dt):
    """Comparable numeric timestamp; treats missing/unparseable dates as oldest possible."""
    if dt is None:
        return -1
    ts = pd.to_datetime(dt, errors="coerce")
    if pd.isna(ts):
        return -1
    return ts.timestamp()


def reprocess_corrected_record(record: dict, actor: dict = None) -> dict:
    """Entry point called after a steward resolves an exception. `record` must contain
    source_system, source_record_id, and the (possibly corrected) customer fields.
    `actor` is the authenticated steward/owner (api/auth.py's _public_user shape) --
    used to attribute the resulting audit trail entry; falls back to a generic
    'system' attribution if not supplied."""
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

    # --- (b) Match against the current gold set, using tier-1 exact_match_field
    # rules from main_rules.matching_rules (see _load_tier1_matching_metadata) --
    tier1_confidence, exact_rules = _load_tier1_matching_metadata()

    conditions, params = [], []
    for rule in exact_rules:
        col = rule["source_column"]
        transform = rule["transform_function"] or "none"
        sql_col = SQL_TRANSFORMS.get(transform, SQL_TRANSFORMS["none"])(col)
        val = PY_TRANSFORMS.get(transform, PY_TRANSFORMS["none"])(record.get(col))
        conditions.append(f"(? != '' AND {sql_col} = ?)")
        params += [val, val]

    matched = pd.DataFrame()
    if conditions:
        where_clause = " OR ".join(conditions)
        matched = run_query(
            f"SELECT DISTINCT golden_id FROM main_gold.gold_customers WHERE {where_clause}",
            params,
        )

    # --- (c) Survivorship: update existing golden record, or create a new one --
    if not matched.empty:
        golden_id = matched.iloc[0]["golden_id"]
        return _update_existing_golden_record(golden_id, source_system, source_record_id, record, now, tier1_confidence, actor)
    else:
        # No existing golden record matched -- this source becomes its own new
        # golden record with no corroboration, so it gets the no-match baseline
        # confidence (main_rules.matching_thresholds, match_method='no_match_baseline'),
        # not the tier-1 confidence -- consistent with the batch gold_crosswalk.sql
        # fallback for the identical single-source situation.
        baseline_confidence = _load_baseline_confidence()
        return _create_new_golden_record(source_system, source_record_id, record, now, baseline_confidence, actor)


def _create_new_golden_record(source_system, source_record_id, record, now, new_record_confidence, actor=None) -> dict:
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
        VALUES (?, ?, ?, ?, true, ?)
    """, [golden_id, source_system, source_record_id, new_record_confidence, now])

    new_row = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id]).iloc[0].to_dict()
    audit_mod.log_creation(
        golden_id, new_row, event_source="steward_reprocessing",
        changed_by=(actor or {}).get("user_id", "system:steward_reprocessing"),
        changed_by_label=(actor or {}).get("full_name") or (actor or {}).get("username", "steward"),
        change_reason=f"Created via real-time reprocessing of exception {record.get('exception_id', 'unknown')}",
        related_exception_id=record.get("exception_id"), event_ts=now,
    )

    return {
        "action": "created_new_golden_record",
        "golden_id": golden_id,
        "is_survivor": True,
        "matched_existing_sources": 0,
    }


def _update_existing_golden_record(golden_id, source_system, source_record_id, record, now, tier1_confidence, actor=None) -> dict:
    old_row_df = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id])
    old_record = old_row_df.iloc[0].to_dict() if not old_row_df.empty else {}

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
            VALUES (?, ?, ?, ?, ?, ?)
        """, [golden_id, c["source_system"], c["source_record_id"], tier1_confidence, is_surv, now])

    new_source_is_survivor = (survivor["source_system"] == source_system
                               and survivor["source_record_id"] == source_record_id)

    new_row = run_query("SELECT * FROM main_gold.gold_customers WHERE golden_id = ?", [golden_id]).iloc[0].to_dict()
    audit_mod.log_update(
        golden_id, old_record=old_record, new_record=new_row,
        event_source="steward_reprocessing",
        changed_by=(actor or {}).get("user_id", "system:steward_reprocessing"),
        changed_by_label=(actor or {}).get("full_name") or (actor or {}).get("username", "steward"),
        change_reason=(
            f"Survivorship recomputed via real-time reprocessing of exception "
            f"{record.get('exception_id', 'unknown')} (new survivor: "
            f"{survivor['source_system']}:{survivor['source_record_id']})"
        ),
        related_exception_id=record.get("exception_id"), event_ts=now,
    )

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

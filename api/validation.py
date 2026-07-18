"""
Re-validation for steward-corrected records, run at "Approve & Resolve" time.

A record only reaches the exception queue because it failed one or more of the
metadata-driven rules in dbt_project/seeds/column_rules.csv (see
models/silver/stg_crm_customers.sql, stg_erp_customers.sql, and
silver_all_staged.sql for the batch-pipeline implementation of these same
checks). This module re-runs the *reject-severity* subset of those rules
against a steward's corrected fields before the record is allowed to flow
into reprocessing (silver upsert + match/merge).

Design notes:
  - Only reject-severity checks are re-verified here (not the correct-severity
    standardization rules like proper-casing or phone formatting) -- those are
    cosmetic and don't gate silver eligibility in the batch pipeline either.
  - By the time a record reaches the exception queue, CRM and ERP records have
    already been staged into the same canonical field names (first_name,
    last_name, email, phone, state_code, country_code), so a single set of
    checks covers both source systems -- mirroring how silver_all_staged.sql
    computes is_invalid after staging, regardless of source.
  - State/country reference lists are read from the same dbt seed tables the
    batch pipeline validates against (main_rules.ref_state_codes /
    ref_country_codes), not hardcoded here, so the two paths can't drift.
  - The reject reason strings intentionally match the wording already shown in
    the stewardship UI's "FAILED VALIDATION" box (silver_all_staged.sql), so a
    "still failing" alert reads consistently with what the steward saw when
    the record first landed in the queue.
"""
import re

from db import run_query

_EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

_state_codes_cache = None
_country_codes_cache = None


def _valid_state_codes():
    global _state_codes_cache
    if _state_codes_cache is None:
        df = run_query("SELECT state_code FROM main_rules.ref_state_codes")
        _state_codes_cache = set(df["state_code"].tolist())
    return _state_codes_cache


def _valid_country_codes():
    global _country_codes_cache
    if _country_codes_cache is None:
        df = run_query("SELECT country_code FROM main_rules.ref_country_codes")
        _country_codes_cache = set(df["country_code"].tolist())
    return _country_codes_cache


def validate_record(record: dict) -> list[str]:
    """Returns the list of reject reasons still present in `record` (empty
    list means the record now passes every reject-severity rule and is safe
    to reprocess)."""
    first_name = (record.get("first_name") or "").strip()
    last_name = (record.get("last_name") or "").strip()
    email = (record.get("email") or "").strip()
    phone = (record.get("phone") or "").strip()
    state_code = (record.get("state_code") or "").strip().upper()
    country_code = (record.get("country_code") or "").strip().upper()

    reasons = []
    if not first_name:
        reasons.append("R001/R009: missing first name")
    if not last_name:
        reasons.append("R002/R009: missing last name")
    if not email or not _EMAIL_RE.match(email):
        reasons.append("R003/R010: invalid email format")
    if not phone:
        reasons.append("R011: missing phone")
    if not state_code or state_code not in _valid_state_codes():
        reasons.append("R004/R012: invalid state code")
    if not country_code or country_code not in _valid_country_codes():
        reasons.append("R005/R013: invalid country code")
    return reasons

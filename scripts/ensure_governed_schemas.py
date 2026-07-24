"""
Provisions the 'ref' and 'bus_rules' schemas/tables (Reference Data
Maintenance and Rules Configuration, both under the Data Governance nav) and
seeds their starter rows if empty.

Why this script exists, standalone, separate from the FastAPI app: dbt's
silver models (stg_crm_customers.sql, stg_erp_customers.sql) now read
ref.ref_state_codes/ref_country_codes as a source, and the gold layer
(gold_crosswalk.sql) reads bus_rules.matching_thresholds as a source -- both
via {{ source(...) }}, not {{ ref(...) }}, since these are DB-native tables,
not dbt seeds. That means `dbt seed`/`dbt run` need these schemas to already
exist and be populated *before* dbt runs at all -- which can be before the
FastAPI app (api/db.py's _ensure_reference_tables/_ensure_bus_rules_tables)
has ever started, e.g. on a fresh clone. scripts/build_pipeline.py therefore
runs this script first, as step 0.

Same "CREATE TABLE IF NOT EXISTS + seed only if empty" DDL as
api/db.py's _ensure_reference_tables/_ensure_bus_rules_tables, duplicated here
deliberately -- this project's existing convention for schemas that both a
standalone script and the API might create (see api/db.py's
_ensure_match_review_tables docstring: "whichever of this script or the API
runs first creates them, both stay consistent"). If you change one, change
the other to match.
"""
from pathlib import Path

import duckdb

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")


DEFAULT_COUNTRY_CODES = [
    ("US", "United States"), ("CA", "Canada"), ("MX", "Mexico"), ("GB", "United Kingdom"),
    ("DE", "Germany"), ("FR", "France"), ("IN", "India"), ("CN", "China"),
    ("JP", "Japan"), ("AU", "Australia"),
]

DEFAULT_STATE_CODES = [
    ("AL", "Alabama"), ("AK", "Alaska"), ("AZ", "Arizona"), ("AR", "Arkansas"),
    ("CA", "California"), ("CO", "Colorado"), ("CT", "Connecticut"), ("DE", "Delaware"),
    ("FL", "Florida"), ("GA", "Georgia"), ("HI", "Hawaii"), ("ID", "Idaho"),
    ("IL", "Illinois"), ("IN", "Indiana"), ("IA", "Iowa"), ("KS", "Kansas"),
    ("KY", "Kentucky"), ("LA", "Louisiana"), ("ME", "Maine"), ("MD", "Maryland"),
    ("MA", "Massachusetts"), ("MI", "Michigan"), ("MN", "Minnesota"), ("MS", "Mississippi"),
    ("MO", "Missouri"), ("MT", "Montana"), ("NE", "Nebraska"), ("NV", "Nevada"),
    ("NH", "New Hampshire"), ("NJ", "New Jersey"), ("NM", "New Mexico"), ("NY", "New York"),
    ("NC", "North Carolina"), ("ND", "North Dakota"), ("OH", "Ohio"), ("OK", "Oklahoma"),
    ("OR", "Oregon"), ("PA", "Pennsylvania"), ("RI", "Rhode Island"), ("SC", "South Carolina"),
    ("SD", "South Dakota"), ("TN", "Tennessee"), ("TX", "Texas"), ("UT", "Utah"),
    ("VT", "Vermont"), ("VA", "Virginia"), ("WA", "Washington"), ("WV", "West Virginia"),
    ("WI", "Wisconsin"), ("WY", "Wyoming"), ("DC", "District of Columbia"),
]

DEFAULT_COLUMN_RULES = [
    ("R001", "CRM", "first_name", "not_null", None, "reject", "First name is required"),
    ("R002", "CRM", "last_name", "not_null", None, "reject", "Last name is required"),
    ("R003", "CRM", "email", "regex", r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "reject", "Email must match standard format"),
    ("R004", "CRM", "state", "reference_state", None, "reject", "State must be a valid two-letter US state code"),
    ("R005", "CRM", "country", "reference_country", None, "reject", "Country must be a valid ISO country code"),
    ("R006", "CRM", "first_name", "trim_case_proper", None, "correct", "Trim whitespace and apply proper case"),
    ("R007", "CRM", "last_name", "trim_case_proper", None, "correct", "Trim whitespace and apply proper case"),
    ("R008", "CRM", "phone", "standardize_phone_us", None, "correct", "Standardize to E.164-style +1 format"),
    ("R009", "ERP", "full_name", "not_null", None, "reject", "Full name is required"),
    ("R010", "ERP", "email_addr", "regex", r"^[^@\s]+@[^@\s]+\.[^@\s]+$", "reject", "Email must match standard format"),
    ("R011", "ERP", "contact_phone", "not_null", None, "reject", "Phone is required"),
    ("R012", "ERP", "state_code", "reference_state", None, "reject", "State must be a valid two-letter US state code"),
    ("R013", "ERP", "country_code", "reference_country", None, "reject", "Country must be a valid ISO country code"),
    ("R014", "ERP", "email_addr", "lowercase", None, "correct", "Normalize email to lowercase"),
    ("R015", "ERP", "contact_phone", "standardize_phone_us", None, "correct", "Standardize to E.164-style +1 format"),
]

DEFAULT_MATCHING_RULES = [
    ("MR001", "MT001", "exact_match_field", 1, "email", "normalize_email", "Normalized (trimmed, lowercased) email must match exactly"),
    ("MR002", "MT001", "exact_match_field", 2, "phone", "normalize_phone", "Normalized (digits-only) phone must match exactly"),
    ("MR003", "MT002", "similarity_text_field", 1, "first_name", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR004", "MT002", "similarity_text_field", 2, "last_name", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR005", "MT002", "similarity_text_field", 3, "address_line1", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR006", "MT002", "similarity_text_field", 4, "address_line2", "none", "Included in the concatenated text used for TF-IDF similarity scoring (optional field, blank treated as empty string)"),
    ("MR007", "MT002", "similarity_text_field", 5, "city", "none", "Included in the concatenated text used for TF-IDF similarity scoring"),
    ("MR008", "MT002", "blocking_key", 1, "state_code", "none", "Candidate pairs are only compared within the same blocking key value, to avoid O(n^2) comparisons at scale. Deliberately excludes email/phone from tier 2 entirely -- those are exactly the fields tier 2 exists to catch disagreement on."),
]

DEFAULT_MATCHING_THRESHOLDS = [
    ("MT000", 0, "No-Match Baseline", "no_match_baseline", False, 0.50, None,
     "Not a real matching tier -- confidence assigned to a single-source golden record with no corroborating match from any tier (\"provisional\")"),
    ("MT001", 1, "Tier 1: Exact Match", "exact", True, 1.00, None,
     "Two records are the same customer if they share an exact match on any active exact_match_field rule (see matching_rules) after that field's transform_function is applied. Binary -- no review band; confidence is always this tier's auto_merge_threshold."),
    ("MT002", 2, "Tier 2: Fuzzy Similarity Match", "fuzzy_tfidf_cosine", True, 0.80, 0.35,
     "Candidate pairs within the same blocking_key value (see matching_rules rule_role=blocking_key) are scored by TF-IDF character n-gram cosine similarity over the concatenated similarity_text_field columns. score >= auto_merge_threshold auto-merges; review_lower_threshold <= score < auto_merge_threshold is queued to the Match Review queue for steward confirm/reject; score < review_lower_threshold is not a candidate at all."),
]

DEFAULT_SURVIVORSHIP_RULES = [
    ("SR001", "first_name", "newest", None, "Most recently modified source wins"),
    ("SR002", "last_name", "newest", None, "Most recently modified source wins"),
    ("SR003", "email", "newest", None, "Most recently modified source wins"),
    ("SR004", "phone", "newest", None, "Most recently modified source wins"),
    ("SR005", "address_line1", "newest", None, "Most recently modified source wins"),
    ("SR006", "address_line2", "newest", None, "Most recently modified source wins"),
    ("SR007", "city", "newest", None, "Most recently modified source wins"),
    ("SR008", "state_code", "newest", None, "Most recently modified source wins"),
    ("SR009", "postal_code", "newest", None, "Most recently modified source wins"),
    ("SR010", "country_code", "newest", None, "Most recently modified source wins"),
]


def ensure_reference_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS ref;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ref.ref_country_codes (
            country_code VARCHAR PRIMARY KEY,
            country_name VARCHAR,
            is_active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ref.ref_state_codes (
            state_code VARCHAR PRIMARY KEY,
            state_name VARCHAR,
            is_active BOOLEAN DEFAULT true,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    if con.execute("SELECT COUNT(*) FROM ref.ref_country_codes").fetchone()[0] == 0:
        for row in DEFAULT_COUNTRY_CODES:
            con.execute("INSERT INTO ref.ref_country_codes (country_code, country_name) VALUES (?, ?)", row)
    if con.execute("SELECT COUNT(*) FROM ref.ref_state_codes").fetchone()[0] == 0:
        for row in DEFAULT_STATE_CODES:
            con.execute("INSERT INTO ref.ref_state_codes (state_code, state_name) VALUES (?, ?)", row)


def ensure_bus_rules_tables(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS bus_rules;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.column_rules (
            rule_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            source_column VARCHAR,
            rule_type VARCHAR,
            rule_param VARCHAR,
            severity VARCHAR,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.matching_rules (
            rule_id VARCHAR PRIMARY KEY,
            tier_id VARCHAR,
            rule_role VARCHAR,
            rule_order INTEGER,
            source_column VARCHAR,
            transform_function VARCHAR,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.matching_thresholds (
            tier_id VARCHAR PRIMARY KEY,
            tier_order INTEGER,
            tier_name VARCHAR,
            match_method VARCHAR,
            is_match_tier BOOLEAN,
            auto_merge_threshold DOUBLE,
            review_lower_threshold DOUBLE,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bus_rules.survivorship_rules (
            rule_id VARCHAR PRIMARY KEY,
            target_column VARCHAR UNIQUE,
            rule_type VARCHAR,
            rule_param VARCHAR,
            active BOOLEAN DEFAULT true,
            description VARCHAR,
            created_ts TIMESTAMP DEFAULT current_timestamp,
            updated_ts TIMESTAMP DEFAULT current_timestamp,
            updated_by VARCHAR
        );
    """)

    if con.execute("SELECT COUNT(*) FROM bus_rules.column_rules").fetchone()[0] == 0:
        for row in DEFAULT_COLUMN_RULES:
            con.execute("""
                INSERT INTO bus_rules.column_rules
                    (rule_id, source_system, source_column, rule_type, rule_param, severity, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row)
    if con.execute("SELECT COUNT(*) FROM bus_rules.matching_rules").fetchone()[0] == 0:
        for row in DEFAULT_MATCHING_RULES:
            con.execute("""
                INSERT INTO bus_rules.matching_rules
                    (rule_id, tier_id, rule_role, rule_order, source_column, transform_function, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, row)
    if con.execute("SELECT COUNT(*) FROM bus_rules.matching_thresholds").fetchone()[0] == 0:
        for row in DEFAULT_MATCHING_THRESHOLDS:
            con.execute("""
                INSERT INTO bus_rules.matching_thresholds
                    (tier_id, tier_order, tier_name, match_method, is_match_tier,
                     auto_merge_threshold, review_lower_threshold, description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, row)
    if con.execute("SELECT COUNT(*) FROM bus_rules.survivorship_rules").fetchone()[0] == 0:
        for row in DEFAULT_SURVIVORSHIP_RULES:
            con.execute("""
                INSERT INTO bus_rules.survivorship_rules (rule_id, target_column, rule_type, rule_param, description)
                VALUES (?, ?, ?, ?, ?)
            """, row)


def main():
    con = duckdb.connect(DB_PATH)
    ensure_reference_tables(con)
    ensure_bus_rules_tables(con)
    print("ref.* and bus_rules.* schemas/tables ready.")


if __name__ == "__main__":
    main()

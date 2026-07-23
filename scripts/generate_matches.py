"""
Match/merge step: embedding-based duplicate detection over the silver layer.

Runs BETWEEN silver and gold in the build sequence (silver_customers must
already exist; the gold layer reads this script's output as a dbt source --
see dbt_project/models/gold/gold_match_candidates.sql):

    dbt seed
    dbt run --exclude gold.*
    python scripts/generate_matches.py
    dbt run --select gold.*

(scripts/build_pipeline.py runs all four steps in order, cross-platform.)

Why this lives outside dbt: candidate embeddings require a Python ML library
(scikit-learn) that plain dbt-duckdb SQL can't produce. Everything downstream
of the embeddings (blocking, similarity scoring, clustering) is also done here
for a single coherent matching pass; the gold dbt models just consume the
result as a source table, the same pattern already used for the bronze layer
(scripts/load_bronze.py Python-loads bronze; dbt treats it as a source).

Matching is metadata-driven, not hardcoded. Tier definitions and thresholds
live in dbt_project/seeds/matching_thresholds.csv (main_rules.matching_thresholds
after `dbt seed`); the fields/columns each tier actually operates on live in
dbt_project/seeds/matching_rules.csv (main_rules.matching_rules), a child
table keyed by tier_id. This mirrors the existing column_rules.csv /
ref_state_codes.csv pattern elsewhere in the project: rules are read from
seeded DuckDB tables at runtime instead of hardcoded, so nothing can drift
out of sync with what the seed says the rules are.

Two-tier matching strategy, as configured today in the seeds (more tiers can
be added by adding rows -- each tier's match_method must be either 'exact'
or 'fuzzy_tfidf_cosine' for this script to know how to evaluate it):

  Tier 1 (match_method='exact'): two source records are the same customer if
    they agree, after that rule's transform_function is applied, on ANY
    active exact_match_field rule for the tier (today: normalized email OR
    normalized phone -- matching_rules.csv rows MR001/MR002). Confidence is
    always that tier's auto_merge_threshold (1.00).

  Tier 2 (match_method='fuzzy_tfidf_cosine'): candidate pairs are generated
    via blocking (grouped by the tier's blocking_key rule(s) -- avoids O(n^2)
    comparisons at scale; today a single column, state_code, but multiple
    blocking_key rows compose into a multi-column key), then scored by cosine
    similarity over TF-IDF character n-gram vectors built from the tier's
    similarity_text_field columns, concatenated in rule_order (today:
    first_name, last_name, address_line1, address_line2, city --
    matching_rules.csv rows MR003-MR007). Deliberately excludes any column
    not listed as a similarity_text_field for the tier -- email/phone are
    intentionally absent (those are exactly the fields tier 2 exists to
    catch disagreement on; including them would dilute the name/address
    signal with noise).
      - similarity >= auto_merge_threshold -> auto-merge, confidence = similarity
      - review_lower_threshold <= similarity < auto_merge_threshold -> NOT
        auto-merged; written to the Match Review queue for a data steward to
        confirm or reject. A prior steward decision
        (stewardship.match_review_overrides) is honored: 'confirmed' pairs
        are unioned as if tier-2-high, 'rejected' pairs are permanently
        excluded.
      - similarity < review_lower_threshold -> not a candidate at all.

Thresholds (0.80 / 0.35 today, see matching_thresholds.csv rows MT002) were
calibrated empirically against this project's synthetic data generator
(data/generate_source_data.py), which seeds 12 deliberate fuzzy-duplicate
pairs (different email+phone, similar name/address) specifically so this
tier has real positive cases to find, and against every same-state
non-duplicate pair as the negative class. At that calibration: true fuzzy
pairs scored 0.71-0.95, unrelated same-state pairs scored at most 0.19 -- a
wide, clean margin. Re-run the calibration check (see PROJECT_KNOWLEDGE.md)
if the data generator's population or field construction changes materially,
or if matching_rules.csv's similarity_text_field list changes.

The TF-IDF vectorizer's shape (char_wb analyzer, 2-4 char n-grams) is a fixed
algorithm parameter, not tier metadata -- it applies identically to every
fuzzy_tfidf_cosine tier, so it stays a code constant (TFIDF_ANALYZER /
TFIDF_NGRAM_RANGE below) rather than a seeded value.

Clustering: Union-Find (path compression + union by rank) over every edge that
should auto-merge (tier-1 exact, tier-2 >= auto_merge_threshold, tier-2
confirmed by a steward). Borderline non-overridden pairs are NOT unioned --
they surface in the review queue instead. match_group_id is assigned by
sorting clusters on their lowest (source_system, source_record_id) member, so
IDs are stable across reruns as long as the underlying silver population
doesn't change.

Known simplification: this batch step is the only place fuzzy
(match_method='fuzzy_tfidf_cosine') matching runs. The real-time reprocessing
path (api/reprocessing.py, triggered when a steward resolves a validation
exception) also reads its match fields from main_rules.matching_rules now,
but only ever evaluates the 'exact' tier -- extending it to fuzzy tiers would
mean fitting a TF-IDF vectorizer per API request, which is a real
latency/complexity cost for a demo-scoped feature. This mirrors this
project's existing documented divergence between real-time reprocessing and
the full batch pipeline.
"""
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

# Fixed algorithm shape for every fuzzy_tfidf_cosine tier -- not tier metadata
# (see docstring above).
TFIDF_ANALYZER = "char_wb"
TFIDF_NGRAM_RANGE = (2, 4)

# Registry of the transform_function values matching_rules.csv rows may use.
# 'none' is the identity transform; add new named transforms here (and to the
# matching seed data) rather than hardcoding field-specific logic elsewhere.
TRANSFORM_FUNCTIONS = {
    "none": lambda v: v,
    "normalize_email": lambda v: (v or "").strip().lower(),
    "normalize_phone": lambda v: re.sub(r"[^0-9]", "", v or ""),
}


def _apply_transform(transform_function, value):
    fn = TRANSFORM_FUNCTIONS.get(transform_function or "none", TRANSFORM_FUNCTIONS["none"])
    return fn(value)


def _combined_text(row, similarity_rules):
    """Concatenates a tier's similarity_text_field columns, in rule_order, after
    each column's transform_function is applied."""
    parts = []
    for rule in similarity_rules:
        val = _apply_transform(rule["transform_function"], row.get(rule["source_column"]))
        parts.append(str(val) if val else "")
    return " ".join(p for p in parts if p)


def _blocking_key(row, blocking_rules):
    """Composes a tier's blocking_key rule(s), in rule_order, into a single
    grouping key. Multiple rows compose into a multi-column composite key."""
    if not blocking_rules:
        return None
    parts = []
    for rule in blocking_rules:
        val = _apply_transform(rule["transform_function"], row.get(rule["source_column"]))
        parts.append(str(val) if val is not None else "")
    return "||".join(parts)


def _pair_id(a, b):
    """Deterministic id for an unordered pair of (source_system, source_record_id),
    stable across reruns so steward review decisions stay attached to the same pair
    even after this table is rebuilt from a fresh pipeline run."""
    key_a, key_b = f"{a[0]}:{a[1]}", f"{b[0]}:{b[1]}"
    ordered = sorted([key_a, key_b])
    return hashlib.md5("|".join(ordered).encode()).hexdigest()


class UnionFind:
    def __init__(self, items):
        self.parent = {i: i for i in items}
        self.rank = {i: 0 for i in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


def _ensure_match_review_tables(con):
    """Same IF-NOT-EXISTS pattern as api/db.py's stewardship tables -- whichever
    of this script or the API runs first creates them, both stay consistent."""
    con.execute("CREATE SCHEMA IF NOT EXISTS stewardship;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS stewardship.match_review_overrides (
            pair_id VARCHAR PRIMARY KEY,
            status VARCHAR,             -- 'confirmed' | 'rejected'
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


def _load_matching_metadata(con):
    """Reads the two matching seed tables and returns (tiers, rules_by_tier),
    where tiers is the list of active tier rows (as dicts) from
    matching_thresholds, and rules_by_tier maps tier_id -> list of that
    tier's active rule rows from matching_rules, in rule_order."""
    thresholds = con.execute("""
        SELECT * FROM main_rules.matching_thresholds
        WHERE active ORDER BY tier_order
    """).fetchdf().to_dict(orient="records")

    rules = con.execute("""
        SELECT * FROM main_rules.matching_rules
        WHERE active ORDER BY tier_id, rule_order
    """).fetchdf().to_dict(orient="records")

    rules_by_tier = {}
    for r in rules:
        rules_by_tier.setdefault(r["tier_id"], []).append(r)

    return thresholds, rules_by_tier


def main():
    con = duckdb.connect(DB_PATH)
    _ensure_match_review_tables(con)

    thresholds, rules_by_tier = _load_matching_metadata(con)
    match_tiers = [t for t in thresholds if t["is_match_tier"]]
    tier1 = next((t for t in match_tiers if t["match_method"] == "exact"), None)
    tier2 = next((t for t in match_tiers if t["match_method"] == "fuzzy_tfidf_cosine"), None)
    if tier1 is None:
        raise RuntimeError(
            "No active tier with match_method='exact' found in "
            "main_rules.matching_thresholds -- at least one exact tier is required."
        )

    exact_rules = [r for r in rules_by_tier.get(tier1["tier_id"], []) if r["rule_role"] == "exact_match_field"]
    tier1_confidence = float(tier1["auto_merge_threshold"])

    silver = con.execute("SELECT * FROM main_silver.silver_customers").fetchdf()
    records = silver.to_dict(orient="records")
    keys = [(r["source_system"], r["source_record_id"]) for r in records]
    by_key = dict(zip(keys, records))

    overrides = con.execute(
        "SELECT pair_id, status FROM stewardship.match_review_overrides"
    ).fetchdf()
    override_status = dict(zip(overrides["pair_id"], overrides["status"]))

    # --- Tier 1: exact edges, one field-value grouping per active exact_match_field rule ---
    exact_edges = []  # (key_a, key_b, confidence)
    for rule in exact_rules:
        groups = {}
        for k, r in by_key.items():
            val = _apply_transform(rule["transform_function"], r.get(rule["source_column"]))
            if val:
                groups.setdefault(val, []).append(k)
        for group in groups.values():
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    exact_edges.append((group[i], group[j], tier1_confidence))

    # --- Tier 2: fuzzy candidate generation via blocking + TF-IDF cosine similarity ---
    fuzzy_auto_edges = []       # (key_a, key_b, confidence) -- similarity >= auto_merge_threshold
    fuzzy_confirmed_edges = []  # steward-confirmed borderline pairs
    review_candidates = []      # rows for gold_prep.match_review_candidates

    if tier2 is not None:
        similarity_rules = [r for r in rules_by_tier.get(tier2["tier_id"], []) if r["rule_role"] == "similarity_text_field"]
        blocking_rules = [r for r in rules_by_tier.get(tier2["tier_id"], []) if r["rule_role"] == "blocking_key"]
        tau_high = float(tier2["auto_merge_threshold"])
        tau_low = float(tier2["review_lower_threshold"])

        blocks = {}
        for k, r in by_key.items():
            blocks.setdefault(_blocking_key(r, blocking_rules), []).append(k)

        texts = [_combined_text(r, similarity_rules) for r in records]
        vectorizer = TfidfVectorizer(analyzer=TFIDF_ANALYZER, ngram_range=TFIDF_NGRAM_RANGE)
        if texts:
            X = vectorizer.fit_transform(texts)
            key_to_row = {k: i for i, k in enumerate(keys)}

            already_exact = set()
            for a, b, _ in exact_edges:
                already_exact.add(frozenset((a, b)))

            for block_val, block_keys in blocks.items():
                for i in range(len(block_keys)):
                    for j in range(i + 1, len(block_keys)):
                        ka, kb = block_keys[i], block_keys[j]
                        if ka[0] == kb[0]:
                            continue  # same source system can't be a cross-source duplicate pair
                        if frozenset((ka, kb)) in already_exact:
                            continue  # already an exact match, no need for a fuzzy edge too
                        sim = float(cosine_similarity(X[key_to_row[ka]], X[key_to_row[kb]])[0, 0])
                        if sim < tau_low:
                            continue
                        if sim >= tau_high:
                            fuzzy_auto_edges.append((ka, kb, sim))
                            continue
                        # borderline: honor any prior steward decision, else queue for review
                        pid = _pair_id(ka, kb)
                        status = override_status.get(pid)
                        if status == "confirmed":
                            fuzzy_confirmed_edges.append((ka, kb, sim))
                        elif status == "rejected":
                            pass  # permanently excluded, don't resurface
                        review_candidates.append({
                            "pair_id": pid,
                            "source_system_a": ka[0], "source_record_id_a": ka[1],
                            "source_system_b": kb[0], "source_record_id_b": kb[1],
                            "similarity_score": round(sim, 4),
                            "blocking_key": str(block_val),
                        })

    # --- Clustering ---
    uf = UnionFind(keys)
    all_auto_edges = exact_edges + fuzzy_auto_edges + fuzzy_confirmed_edges
    for a, b, _ in all_auto_edges:
        uf.union(a, b)

    clusters = {}
    for k in keys:
        clusters.setdefault(uf.find(k), []).append(k)
    ordered_roots = sorted(clusters.keys(), key=lambda root: min(clusters[root]))
    match_group_id = {}
    for gid, root in enumerate(ordered_roots, start=1):
        for k in clusters[root]:
            match_group_id[k] = gid

    now = datetime.now(timezone.utc)

    # --- Write results ---
    con.execute("CREATE SCHEMA IF NOT EXISTS gold_prep;")

    con.execute("""
        CREATE OR REPLACE TABLE gold_prep.match_groups (
            source_system VARCHAR, source_record_id VARCHAR, match_group_id INTEGER
        );
    """)
    con.executemany(
        "INSERT INTO gold_prep.match_groups VALUES (?, ?, ?)",
        [(k[0], k[1], match_group_id[k]) for k in keys],
    )

    con.execute("""
        CREATE OR REPLACE TABLE gold_prep.match_edges (
            source_system_a VARCHAR, source_record_id_a VARCHAR,
            source_system_b VARCHAR, source_record_id_b VARCHAR,
            match_type VARCHAR, confidence DOUBLE, created_ts TIMESTAMP
        );
    """)
    edge_rows = (
        [(a[0], a[1], b[0], b[1], "exact", conf, now) for a, b, conf in exact_edges]
        + [(a[0], a[1], b[0], b[1], "fuzzy_auto", conf, now) for a, b, conf in fuzzy_auto_edges]
        + [(a[0], a[1], b[0], b[1], "fuzzy_confirmed", conf, now) for a, b, conf in fuzzy_confirmed_edges]
    )
    if edge_rows:
        con.executemany("INSERT INTO gold_prep.match_edges VALUES (?, ?, ?, ?, ?, ?, ?)", edge_rows)

    con.execute("""
        CREATE OR REPLACE TABLE gold_prep.match_review_candidates (
            pair_id VARCHAR, source_system_a VARCHAR, source_record_id_a VARCHAR,
            source_system_b VARCHAR, source_record_id_b VARCHAR,
            similarity_score DOUBLE, blocking_key VARCHAR, generated_ts TIMESTAMP
        );
    """)
    if review_candidates:
        con.executemany(
            "INSERT INTO gold_prep.match_review_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(rc["pair_id"], rc["source_system_a"], rc["source_record_id_a"],
              rc["source_system_b"], rc["source_record_id_b"], rc["similarity_score"],
              rc["blocking_key"], now) for rc in review_candidates],
        )

    con.close()

    tau_high_display = tier2["auto_merge_threshold"] if tier2 is not None else "n/a"
    tau_low_display = tier2["review_lower_threshold"] if tier2 is not None else "n/a"
    print(f"Matching complete: {len(keys)} silver records -> {len(ordered_roots)} match groups")
    print(f"  tier-1 exact edges: {len(exact_edges)}")
    print(f"  tier-2 auto-merge edges (>= {tau_high_display}): {len(fuzzy_auto_edges)}")
    print(f"  tier-2 steward-confirmed edges: {len(fuzzy_confirmed_edges)}")
    print(f"  match review candidates ({tau_low_display}-{tau_high_display}, all-time): {len(review_candidates)}")


if __name__ == "__main__":
    main()

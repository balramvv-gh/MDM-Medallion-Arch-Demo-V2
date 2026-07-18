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

Two-tier matching strategy:
  Tier 1 (exact, deterministic): two source records are the same customer if
    they share a normalized email OR a normalized phone. Confidence 1.00.
  Tier 2 (fuzzy, embedding similarity): candidate pairs are generated via
    blocking (same state_code -- avoids O(n^2) comparisons at scale), then
    scored by cosine similarity over TF-IDF character n-gram vectors of each
    record's name+address text. Deliberately EXCLUDES email/phone from the
    similarity text -- those are exactly the fields tier 2 exists to catch
    disagreement on, so including them would just dilute the name/address
    signal with noise.
      - similarity >= TAU_HIGH  -> auto-merge, confidence = similarity score
      - TAU_LOW <= similarity < TAU_HIGH -> NOT auto-merged; written to the
        Match Review queue for a data steward to confirm or reject. A prior
        steward decision (stewardship.match_review_overrides) is honored:
        'confirmed' pairs are unioned as if tier-2-high, 'rejected' pairs are
        permanently excluded.
      - similarity < TAU_LOW -> not a candidate at all.

Thresholds (TAU_HIGH=0.80, TAU_LOW=0.35) were calibrated empirically against
this project's synthetic data generator (data/generate_source_data.py), which
seeds 12 deliberate fuzzy-duplicate pairs (different email+phone, similar
name/address) specifically so this tier has real positive cases to find, and
against every same-state non-duplicate pair as the negative class. At that
calibration: true fuzzy pairs scored 0.71-0.95, unrelated same-state pairs
scored at most 0.19 -- a wide, clean margin. Re-run the calibration check
(see PROJECT_KNOWLEDGE.md) if the data generator's population or field
construction changes materially.

Clustering: Union-Find (path compression + union by rank) over every edge that
should auto-merge (tier-1 exact, tier-2 >= TAU_HIGH, tier-2 confirmed by a
steward). Borderline non-overridden pairs are NOT unioned -- they surface in
the review queue instead. match_group_id is assigned by sorting clusters on
their lowest (source_system, source_record_id) member, so IDs are stable
across reruns as long as the underlying silver population doesn't change.

Known simplification: this batch step is the only place fuzzy matching runs.
The real-time reprocessing path (api/reprocessing.py, triggered when a
steward resolves a validation exception) still matches on exact email/phone
only -- extending it to fuzzy matching would mean fitting a TF-IDF vectorizer
per API request, which is a real latency/complexity cost for a demo-scoped
feature. This mirrors this project's existing documented divergence between
real-time reprocessing and the full batch pipeline.
"""
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

DB_PATH = str(Path(__file__).resolve().parent.parent / "mdm_demo.duckdb")

BLOCK_KEY_COLUMN = "state_code"
TAU_HIGH = 0.80   # >= this: auto-merge via the fuzzy tier
TAU_LOW = 0.35    # >= this (and < TAU_HIGH): borderline, goes to Match Review


def _normalize_email(email):
    return (email or "").strip().lower()


def _normalize_phone(phone):
    return re.sub(r"[^0-9]", "", phone or "")


def _combined_text(row):
    parts = [row["first_name"], row["last_name"], row["address_line1"],
             row["address_line2"] or "", row["city"]]
    return " ".join(str(p) for p in parts if p)


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


def main():
    con = duckdb.connect(DB_PATH)
    _ensure_match_review_tables(con)

    silver = con.execute("SELECT * FROM main_silver.silver_customers").fetchdf()
    records = silver.to_dict(orient="records")
    keys = [(r["source_system"], r["source_record_id"]) for r in records]
    by_key = dict(zip(keys, records))

    overrides = con.execute(
        "SELECT pair_id, status FROM stewardship.match_review_overrides"
    ).fetchdf()
    override_status = dict(zip(overrides["pair_id"], overrides["status"]))

    # --- Tier 1: exact email/phone edges (proper OR-graph, not a single sort key) ---
    email_groups, phone_groups = {}, {}
    for k, r in by_key.items():
        e = _normalize_email(r.get("email"))
        if e:
            email_groups.setdefault(e, []).append(k)
        p = _normalize_phone(r.get("phone"))
        if p:
            phone_groups.setdefault(p, []).append(k)

    exact_edges = []  # (key_a, key_b, confidence)
    for group in list(email_groups.values()) + list(phone_groups.values()):
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                exact_edges.append((group[i], group[j], 1.00))

    # --- Tier 2: fuzzy candidate generation via blocking + TF-IDF cosine similarity ---
    blocks = {}
    for k, r in by_key.items():
        blocks.setdefault(r.get(BLOCK_KEY_COLUMN), []).append(k)

    fuzzy_auto_edges = []       # (key_a, key_b, confidence) -- similarity >= TAU_HIGH
    fuzzy_confirmed_edges = []  # steward-confirmed borderline pairs
    review_candidates = []      # rows for gold_prep.match_review_candidates

    texts = [_combined_text(r) for r in records]
    vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
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
                    if sim < TAU_LOW:
                        continue
                    if sim >= TAU_HIGH:
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

    print(f"Matching complete: {len(keys)} silver records -> {len(ordered_roots)} match groups")
    print(f"  tier-1 exact edges: {len(exact_edges)}")
    print(f"  tier-2 auto-merge edges (>= {TAU_HIGH}): {len(fuzzy_auto_edges)}")
    print(f"  tier-2 steward-confirmed edges: {len(fuzzy_confirmed_edges)}")
    print(f"  match review candidates ({TAU_LOW}-{TAU_HIGH}, all-time): {len(review_candidates)}")


if __name__ == "__main__":
    main()

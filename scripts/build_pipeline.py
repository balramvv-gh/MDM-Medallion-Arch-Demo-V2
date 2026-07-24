"""
Runs the full pipeline build in order, cross-platform (Windows/macOS/Linux):

  0. python scripts/ensure_governed_schemas.py  (provisions ref.*/bus_rules.* --
                                                   see below for why this runs first)
  1. dbt seed
  2. dbt run --exclude gold.*          (bronze is already loaded separately, see below)
  3. python scripts/generate_matches.py  (embedding-based match/merge -- needs silver)
  4. dbt run --select gold.*           (gold layer consumes the matches from step 3)
  5. python scripts/audit_pipeline_diff.py  (logs creates/updates/logical deletes
                                              to the gold-layer audit trail)

This exists because the gold layer now depends on a Python step (embedding
similarity via scikit-learn) that has to run strictly between silver and gold
-- a single `dbt run` can no longer build the whole pipeline in one command.
See scripts/generate_matches.py's docstring for why.

Step 0 exists because Reference Data Maintenance and Rules Configuration
(Data Governance nav) moved country/state codes and column/matching/
survivorship rules out of dbt seeds into DB-native tables (schemas 'ref' and
'bus_rules', maintained via those screens' maker-checker workflows). Step 2's
silver models now read ref.ref_state_codes/ref_country_codes as a
{{ source(...) }}, and step 4's gold_crosswalk.sql reads
bus_rules.matching_thresholds the same way -- both schemas must already exist
and be populated before dbt runs, which can be before the FastAPI app has
ever started (e.g. a fresh clone). See scripts/ensure_governed_schemas.py's
docstring for the full rationale.

Step 5 must run immediately after step 4, before any other write touches
main_gold.gold_customers -- it diffs the freshly rebuilt table against the
audit trail's snapshot of the previous build. If you ever rebuild just the
gold layer manually (`dbt run --select gold.*`) instead of via this script,
run `python scripts/audit_pipeline_diff.py` afterward too, or that rebuild's
changes won't be captured in the audit trail.

Does NOT run data/generate_source_data.py or scripts/load_bronze.py -- run
those first if you need fresh source data (same as before; unchanged).

Usage:
    python scripts/generate_source_data.py
    python scripts/load_bronze.py
    python scripts/build_pipeline.py
"""
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"


def run(cmd, cwd=None, env=None):
    print(f"\n$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main():
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = "."

    # --no-partial-parse: forces a full reparse instead of trusting
    # target/partial_parse.msgpack. Without it, a manifest cache left over
    # from a previous run/OS/path can go stale and dbt raises a KeyError
    # looking up a macro file (e.g. 'mdm_demo://macros\\proper_case.sql')
    # that doesn't match the current parse. Cheap insurance, no downside.
    run([sys.executable, str(PROJECT_ROOT / "scripts" / "ensure_governed_schemas.py")])
    run(["dbt", "--no-partial-parse", "seed"], cwd=DBT_PROJECT_DIR, env=env)
    run(["dbt", "--no-partial-parse", "run", "--exclude", "gold.*"], cwd=DBT_PROJECT_DIR, env=env)
    run([sys.executable, str(PROJECT_ROOT / "scripts" / "generate_matches.py")])
    run(["dbt", "--no-partial-parse", "run", "--select", "gold.*"], cwd=DBT_PROJECT_DIR, env=env)
    run([sys.executable, str(PROJECT_ROOT / "scripts" / "audit_pipeline_diff.py")])

    print("\nPipeline build complete.")


if __name__ == "__main__":
    main()

"""
Bronze layer: lands raw source extracts into DuckDB exactly as received,
tagged with source_system, source_record_id (natural key), and load timestamp.
No cleansing happens here by design -- bronze is the immutable raw copy.
"""
import duckdb
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB_PATH = str(BASE / "mdm_demo.duckdb")
# forward slashes for SQL string literals -- avoids backslash-escaping pitfalls on Windows
CRM_CSV = (BASE / "data" / "sources" / "crm_customers.csv").as_posix()
ERP_CSV = (BASE / "data" / "sources" / "erp_customers.csv").as_posix()

con = duckdb.connect(DB_PATH)
con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

con.execute(f"""
CREATE OR REPLACE TABLE bronze.crm_customers_raw AS
SELECT
    'CRM' AS source_system,
    customer_id AS source_record_id,
    current_timestamp AS _load_ts,
    *
FROM read_csv_auto('{CRM_CSV}', ALL_VARCHAR=TRUE);
""")

con.execute(f"""
CREATE OR REPLACE TABLE bronze.erp_customers_raw AS
SELECT
    'ERP' AS source_system,
    customer_number AS source_record_id,
    current_timestamp AS _load_ts,
    *
FROM read_csv_auto('{ERP_CSV}', ALL_VARCHAR=TRUE);
""")

crm_count = con.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw").fetchone()[0]
erp_count = con.execute("SELECT COUNT(*) FROM bronze.erp_customers_raw").fetchone()[0]
print(f"Bronze loaded: CRM={crm_count} rows, ERP={erp_count} rows -> {DB_PATH}")
con.close()

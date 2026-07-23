"""
Seeds the extra demo accounts needed to actually exercise every maker-checker
workflow level/quorum end to end.

Why these are needed: a maker can never approve their own submission, and the
same approver can never cast two decisions on one workflow instance (see
api/workflow_engine.py). Two of the four workflows configured in
governance.workflow_definitions need more distinct people in a role than this
demo ships with by default (one of each: mdm_admin, mdm_dataowner,
mdm_dataSteward, mdm_bususer):

  - gold_record_edit needs a quorum of 2 different Data Owners to approve one
    edit -- and if the maker themselves happens to be a Data Owner with
    read_write gold access (like mdm_dataowner), neither of the 2 approvers
    can be that same person, so at least 3 total Data Owners are needed to
    guarantee the quorum is reachable regardless of who made the edit.
  - user_admin_change needs a second Admin, distinct from whichever admin
    requested the user create/update, to approve it.

This script is safe to re-run: any username that already exists is skipped,
matching scripts/create_admin_user.py's convention. It only ever adds
accounts -- it never modifies or deletes existing ones.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "api"))

from auth import create_user
from db import run_query

DEMO_ACCOUNTS = [
    # username,        full_name,              role,        gold_access
    ("mdm_dataowner2", "MDM Data Owner Two",   "dataOwner", "read_write"),
    ("mdm_dataowner3", "MDM Data Owner Three", "dataOwner", "read_write"),
    ("mdm_admin2",     "MDM Administrator Two", "admin",    "read_write"),
]

print("=" * 60)
print("Seeding demo governance accounts (for testing maker-checker quorums)")
print("=" * 60)

for username, full_name, role, gold_access in DEMO_ACCOUNTS:
    existing = run_query("SELECT username FROM auth.users WHERE username = ?", [username])
    if not existing.empty:
        print(f"  '{username}' already exists -- skipped.")
        continue
    result = create_user(username=username, full_name=full_name, role=role, gold_access=gold_access)
    print(f"  created '{result['username']}' ({role}) -- password: {result['temp_password']}")

print("=" * 60)
print("Save any printed passwords now -- they will not be shown again.")
print("Log in at http://localhost:8000/portal/ (or /app/ for dataSteward/dataOwner).")

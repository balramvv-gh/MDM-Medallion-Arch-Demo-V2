"""
Creates the initial administrative user for the UX portal:
  username: mdm_admin
  role: admin          (sees the Administration menu, incl. User Administration)
  gold_access: read_write  (can browse AND edit gold-layer customer records)

A secure random password is generated and printed ONCE. It is not stored or
logged anywhere in plaintext -- only its bcrypt hash is persisted. If you lose
it, use scripts/reset_admin_password.py or the User Administration screen
(once logged in as another admin) to reset it.

Safe to re-run: if mdm_admin already exists, it exits without changes.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "api"))

from auth import create_user
from db import run_query

existing = run_query("SELECT username FROM auth.users WHERE username = 'mdm_admin'")
if not existing.empty:
    print("User 'mdm_admin' already exists -- no changes made.")
    print("If you need to reset the password, use the User Administration screen "
          "(logged in as another admin) or scripts/reset_admin_password.py.")
else:
    result = create_user(
        username="mdm_admin",
        full_name="MDM Administrator",
        role="admin",
        gold_access="read_write",
    )
    print("=" * 60)
    print("Admin user created.")
    print(f"  username: {result['username']}")
    print(f"  password: {result['temp_password']}")
    print("=" * 60)
    print("Save this password now -- it will not be shown again.")
    print("Log in at http://localhost:8000/portal/ and change it on first use")
    print("via the account menu, or rotate it from User Administration.")

"""
CLI password reset, for when you're locked out of the portal entirely.
Usage: python3 scripts/reset_admin_password.py [username]
Defaults to 'mdm_admin' if no username given.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "api"))

import secrets
from auth import hash_password
from db import run_query, run_write

username = sys.argv[1] if len(sys.argv) > 1 else "mdm_admin"

existing = run_query("SELECT user_id FROM auth.users WHERE username = ?", [username])
if existing.empty:
    print(f"No user found with username '{username}'.")
    sys.exit(1)

new_password = secrets.token_urlsafe(9)
run_write("UPDATE auth.users SET password_hash = ? WHERE username = ?",
          [hash_password(new_password), username])

print("=" * 60)
print(f"Password reset for '{username}'.")
print(f"  new password: {new_password}")
print("=" * 60)
print("Save this now -- it will not be shown again.")

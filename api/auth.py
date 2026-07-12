import secrets
import uuid
from datetime import datetime, timedelta

import bcrypt
from fastapi import Header, HTTPException, Depends

from db import run_query, run_write

SESSION_TTL_HOURS = 8


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


def create_user(username: str, full_name: str, role: str, gold_access: str, password: str = None) -> dict:
    """Creates a user and returns {user_id, username, temp_password}.
    If no password is supplied, a secure random one is generated and returned
    once -- it is never stored or logged in plaintext."""
    existing = run_query("SELECT user_id FROM auth.users WHERE username = ?", [username])
    if not existing.empty:
        raise ValueError(f"Username '{username}' already exists")

    temp_password = password or secrets.token_urlsafe(9)
    user_id = str(uuid.uuid4())
    run_write("""
        INSERT INTO auth.users (user_id, username, password_hash, full_name, role, gold_access, is_active)
        VALUES (?, ?, ?, ?, ?, ?, true)
    """, [user_id, username, hash_password(temp_password), full_name, role, gold_access])

    return {"user_id": user_id, "username": username, "temp_password": temp_password}


def authenticate(username: str, password: str) -> dict | None:
    df = run_query("SELECT * FROM auth.users WHERE username = ? AND is_active = true", [username])
    if df.empty:
        return None
    user = df.to_dict(orient="records")[0]
    if not verify_password(password, user["password_hash"]):
        return None

    token = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS)
    run_write("INSERT INTO auth.sessions (token, user_id, expires_ts) VALUES (?, ?, ?)",
              [token, user["user_id"], expires])
    run_write("UPDATE auth.users SET last_login_ts = current_timestamp WHERE user_id = ?", [user["user_id"]])

    return {"token": token, "user": _public_user(user)}


def _public_user(user: dict) -> dict:
    return {
        "user_id": user["user_id"],
        "username": user["username"],
        "full_name": user["full_name"],
        "role": user["role"],
        "gold_access": user["gold_access"],
    }


def logout(token: str):
    run_write("DELETE FROM auth.sessions WHERE token = ?", [token])


def get_user_from_token(token: str) -> dict | None:
    df = run_query("""
        SELECT u.* FROM auth.sessions s
        JOIN auth.users u ON u.user_id = s.user_id
        WHERE s.token = ? AND s.expires_ts > current_timestamp AND u.is_active = true
    """, [token])
    if df.empty:
        return None
    return df.to_dict(orient="records")[0]


# --- FastAPI dependencies -------------------------------------------------

def get_current_user(authorization: str = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    user = get_user_from_token(token)
    if user is None:
        raise HTTPException(401, "Invalid or expired session")
    return _public_user(user)


def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(403, "Administrator role required")
    return user


def require_steward_or_owner(user: dict = Depends(get_current_user)) -> dict:
    """Gates the Data Stewardship console: only data stewards and data owners
    may review/resolve exception-queue records. Admin and businessUser are
    deliberately excluded -- stewardship is a distinct governance function,
    not a superset of admin rights."""
    if user["role"] not in ("dataSteward", "dataOwner"):
        raise HTTPException(403, "Data Steward or Data Owner role required")
    return user


def require_gold_write(user: dict = Depends(get_current_user)) -> dict:
    if user["gold_access"] != "read_write":
        raise HTTPException(403, "Read/write access to the gold layer is required")
    return user


def require_gold_read(user: dict = Depends(get_current_user)) -> dict:
    if user["gold_access"] not in ("read", "read_write"):
        raise HTTPException(403, "Read access to the gold layer is required")
    return user

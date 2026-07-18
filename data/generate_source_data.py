"""
Generates synthetic source system extracts for the MDM demo:
  - CRM system export (data/sources/crm_customers.csv)  -- ~100 rows
  - ERP system export (data/sources/erp_customers.csv)  -- ~100 rows

Design intent (deterministic: random.seed + Faker.seed are both fixed, so this
script produces byte-identical output every run):
  - CURATED_PEOPLE (18 hand-picked, real-city people) are kept as-is for
    continuity with the original demo -- they exercise the original set of
    validation/exception cases by hand-picked index.
  - GENERATED_PEOPLE tops the population up to a scale where each source system
    lands at ~100 rows, split across:
      * BOTH, exact-match pairs   -- same person in CRM+ERP, email/phone are the
        same underneath reformatting (caught by the deterministic match tier)
      * BOTH, fuzzy-match pairs   -- same person in CRM+ERP, but email AND phone
        both genuinely differ between systems while name/address stay similar
        (only findable by the embedding-similarity tier in
        scripts/generate_matches.py -- exact tier will legitimately miss these)
      * CRM-only / ERP-only       -- no counterpart in the other system at all
  - A subset of rows get deliberately dirty data (bad email, missing required
    field, invalid state/country code, missing phone) so the cleansing/
    validation layer and the stewardship exception queue have real work to do.
"""
import csv
import os
import random
import re
from datetime import datetime, timedelta

from faker import Faker

random.seed(42)
fake = Faker("en_US")
Faker.seed(42)

BASE_DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(BASE_DIR, "sources")

# ---------------------------------------------------------------------------
# Curated people (original 18) -- kept verbatim for continuity. Indices 1-18.
# ---------------------------------------------------------------------------
CURATED_PEOPLE = [
    ("Maria", "Gonzalez", "maria.gonzalez@example.com", "410-555-0142", "142 Oak St", "Bel Air", "MD", "21014", "US"),
    ("James", "Whitfield", "james.whitfield@example.com", "443-555-0199", "88 Birchwood Ave", "Baltimore", "MD", "21201", "US"),
    ("Ling", "Zhao", "ling.zhao@example.com", "202-555-0117", "500 Constitution Ave", "Washington", "DC", "20001", "US"),
    ("Aditya", "Rao", "aditya.rao@example.com", "212-555-0163", "9 Wall St", "New York", "NY", "10005", "US"),
    ("Sofia", "Moretti", "sofia.moretti@example.com", "617-555-0180", "22 Beacon St", "Boston", "MA", "02108", "US"),
    ("David", "Okafor", "david.okafor@example.com", "312-555-0111", "1 State St", "Chicago", "IL", "60602", "US"),
    ("Emily", "Carter", "emily.carter@example.com", "215-555-0155", "300 Market St", "Philadelphia", "PA", "19106", "US"),
    ("Noah", "Kim", "noah.kim@example.com", "512-555-0177", "77 Congress Ave", "Austin", "TX", "78701", "US"),
    ("Fatima", "Haidari", "fatima.haidari@example.com", "703-555-0122", "14 Duke St", "Alexandria", "VA", "22314", "US"),
    ("Liam", "OBrien", "liam.obrien@example.com", "404-555-0133", "10 Peachtree St", "Atlanta", "GA", "30303", "US"),
    ("Ava", "Thompson", "ava.thompson@example.com", "303-555-0144", "1 Civic Center Dr", "Denver", "CO", "80202", "US"),
    ("Mohammed", "Al-Sayed", "mohammed.alsayed@example.com", "602-555-0166", "200 W Jefferson St", "Phoenix", "AZ", "85003", "US"),
    ("Grace", "Nguyen", "grace.nguyen@example.com", "206-555-0188", "600 4th Ave", "Seattle", "WA", "98104", "US"),
    ("Ethan", "Brooks", "ethan.brooks@example.com", "614-555-0121", "90 W Broad St", "Columbus", "OH", "43215", "US"),
    ("Olivia", "Schmidt", "olivia.schmidt@example.com", "651-555-0139", "15 Kellogg Blvd", "St Paul", "MN", "55102", "US"),
    ("Lucas", "Ferreira", "lucas.ferreira@example.com", "305-555-0114", "111 SW 1st St", "Miami", "FL", "33130", "US"),
    ("Chloe", "Bennett", "chloe.bennett@example.com", "702-555-0128", "500 S Grand Central Pkwy", "Las Vegas", "NV", "89106", "US"),
    ("Ryan", "Sullivan", "ryan.sullivan@example.com", "801-555-0119", "451 S State St", "Salt Lake City", "UT", "84111", "US"),
]
# curated single-source cases (unchanged from the original design)
CURATED_CRM_ONLY = {13, 14}     # missing from ERP
CURATED_ERP_ONLY = {16, 17, 18}  # missing from CRM
# curated fuzzy-only pair, folded into CURATED_PEOPLE as indices 19-20 below
CURATED_PEOPLE.append(("Benjamin", "Carter", "benjamin.carter@example.com", "617-555-0192", "5 Newbury St", "Boston", "MA", "02116", "US"))
CURATED_PEOPLE.append(("Isabella", "Martinez", "isabella.martinez@example.com", "480-555-0145", "220 Cactus Rd", "Tempe", "AZ", "85281", "US"))
CURATED_FUZZY_OVERRIDES = {
    19: {"full_name": "Carter, Ben", "email_addr": "bcarter@example-corp.com",
         "contact_phone": "6175559192", "addr1": "5 Newbury Street"},
    20: {"full_name": "Martinez, Izzy", "email_addr": "izzy.m@example-corp.com",
         "contact_phone": "4805551145", "addr1": "220 Cactus Road"},
}

# ---------------------------------------------------------------------------
# Scale knobs: each source system lands at BOTH + <source>_ONLY rows.
# ---------------------------------------------------------------------------
CRM_ONLY_TOTAL = 12
ERP_ONLY_TOTAL = 12
FUZZY_PAIR_TOTAL = 12     # includes the 2 curated ones above
TARGET_ROWS_PER_SOURCE = 100
BOTH_TOTAL = TARGET_ROWS_PER_SOURCE - CRM_ONLY_TOTAL   # = TARGET - ERP_ONLY_TOTAL too (symmetric)
assert TARGET_ROWS_PER_SOURCE - ERP_ONLY_TOTAL == BOTH_TOTAL

STREET_ABBREV = [("Street", "St"), ("Avenue", "Ave"), ("Road", "Rd"), ("Boulevard", "Blvd"), ("Drive", "Dr")]

# Must match dbt_project/seeds/ref_state_codes.csv exactly (50 states + DC) -- Faker's
# state_abbr() also returns US-affiliated jurisdictions (MH, FM, PW, ...) that aren't in
# that reference table, which would otherwise send an otherwise-clean generated person to
# the exception queue for an unrelated reason ("invalid state code") and make match-tier
# test cases unreliable.
VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
    "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}


def _valid_state():
    while True:
        s = fake.state_abbr(include_territories=False)
        if s in VALID_STATES:
            return s

_used_emails = set()
_used_names = set()


def _unique_person():
    """Draws a Faker person not already used (by name or derived email)."""
    while True:
        first, last = fake.first_name(), fake.last_name()
        key = (first.lower(), last.lower())
        if key in _used_names:
            continue
        email = f"{first.lower()}.{last.lower()}@example.com"
        if email in _used_emails:
            continue
        _used_names.add(key)
        _used_emails.add(email)
        state = _valid_state()
        area_code = random.randint(201, 989)
        return (
            first, last, email, f"{area_code}-555-{random.randint(0, 9999):04d}",
            fake.street_address(), fake.city(), state, fake.zipcode_in_state(state), "US",
        )


def _nickname_variant(first_name):
    """Cheap, generic 'looks like a nickname or typo' transform -- no real
    nickname dictionary needed, just enough drift that exact string/email/phone
    match won't catch it but the name is still recognizably close."""
    style = random.choice(["prefix", "swap", "drop"])
    if style == "prefix" and len(first_name) > 3:
        return first_name[: random.randint(3, 4)]
    if style == "swap" and len(first_name) > 3:
        i = random.randint(1, len(first_name) - 2)
        chars = list(first_name)
        chars[i], chars[i + 1] = chars[i + 1], chars[i]
        return "".join(chars)
    if style == "drop" and len(first_name) > 4:
        i = random.randint(1, len(first_name) - 2)
        return first_name[:i] + first_name[i + 1 :]
    return first_name


def _fuzzy_address_variant(addr):
    for full, abbr in STREET_ABBREV:
        if addr.endswith(" " + abbr):
            return addr[: -len(abbr)] + full
        if addr.endswith(" " + full):
            return addr[: -len(full)] + abbr
    return addr


GENERATED_PEOPLE = []
GENERATED_CRM_ONLY = set()
GENERATED_ERP_ONLY = set()
GENERATED_FUZZY = {}  # index -> ERP dirty overrides, same shape as CURATED_FUZZY_OVERRIDES

_next_idx = len(CURATED_PEOPLE) + 1  # continue numbering after curated people

n_generated_both_exact = BOTH_TOTAL - len(CURATED_PEOPLE) + len(CURATED_CRM_ONLY) + len(CURATED_ERP_ONLY) - (FUZZY_PAIR_TOTAL - len(CURATED_FUZZY_OVERRIDES))
n_generated_fuzzy = FUZZY_PAIR_TOTAL - len(CURATED_FUZZY_OVERRIDES)
n_generated_crm_only = CRM_ONLY_TOTAL - len(CURATED_CRM_ONLY)
n_generated_erp_only = ERP_ONLY_TOTAL - len(CURATED_ERP_ONLY)

for _ in range(n_generated_both_exact):
    GENERATED_PEOPLE.append(_unique_person())
    _next_idx += 1

for _ in range(n_generated_fuzzy):
    p = _unique_person()
    idx = len(CURATED_PEOPLE) + len(GENERATED_PEOPLE) + 1
    GENERATED_PEOPLE.append(p)
    first, last, email, phone, addr, city, state, zipc, country = p
    GENERATED_FUZZY[idx] = {
        "full_name": f"{last}, {_nickname_variant(first)}",
        "email_addr": f"{_nickname_variant(first).lower()}.{last.lower()}{random.randint(1,99)}@example-corp.com",
        "contact_phone": f"{random.randint(201,989)}555{random.randint(0,9999):04d}",
        "addr1": _fuzzy_address_variant(addr),
    }

for _ in range(n_generated_crm_only):
    GENERATED_PEOPLE.append(_unique_person())
    idx = len(CURATED_PEOPLE) + len(GENERATED_PEOPLE)
    GENERATED_CRM_ONLY.add(idx)

for _ in range(n_generated_erp_only):
    GENERATED_PEOPLE.append(_unique_person())
    idx = len(CURATED_PEOPLE) + len(GENERATED_PEOPLE)
    GENERATED_ERP_ONLY.add(idx)

PEOPLE = CURATED_PEOPLE + GENERATED_PEOPLE
CRM_ONLY = CURATED_CRM_ONLY | GENERATED_CRM_ONLY
ERP_ONLY = CURATED_ERP_ONLY | GENERATED_ERP_ONLY
FUZZY_OVERRIDES = {**CURATED_FUZZY_OVERRIDES, **GENERATED_FUZZY}

# ---------------------------------------------------------------------------
# Dirty (reject-severity validation failure) injection -- scaled up from the
# original 6 hand-picked cases to a proportional ~16 across the larger set.
# Picked deterministically (seeded random.sample) from indices that are NOT
# already fuzzy-override or single-source-only rows, so each violation type
# is easy to reason about in isolation.
# ---------------------------------------------------------------------------
eligible = [i for i in range(1, len(PEOPLE) + 1) if i not in FUZZY_OVERRIDES]
random.shuffle(eligible)
CRM_DIRTY_MISSING_LASTNAME = set(eligible[0:2])
CRM_DIRTY_BAD_EMAIL = set(eligible[2:4])
CRM_DIRTY_INVALID_STATE = set(eligible[4:6])
CRM_DIRTY_MESSY_CASE = set(eligible[6:8])
ERP_DIRTY_MISSING_PHONE = set(eligible[8:10])
ERP_DIRTY_BAD_EMAIL = set(eligible[10:12])
ERP_DIRTY_INVALID_COUNTRY = set(eligible[12:14])
# keep the two original hand-picked dirty cases too (indices 2,5,9,11 CRM / 3,6,12 ERP)
CRM_DIRTY_MISSING_LASTNAME.add(2)
CRM_DIRTY_BAD_EMAIL.add(5)
CRM_DIRTY_INVALID_STATE.add(9)
CRM_DIRTY_MESSY_CASE.add(11)
ERP_DIRTY_MISSING_PHONE.add(3)
ERP_DIRTY_BAD_EMAIL.add(6)
ERP_DIRTY_INVALID_COUNTRY.add(12)


def rand_date(days_back_max=400):
    d = datetime(2025, 1, 1) + timedelta(days=random.randint(0, days_back_max))
    return d.strftime("%Y-%m-%d")


def crm_row(idx, p, dirty=None):
    first, last, email, phone, addr, city, state, zipc, country = p
    dirty = dirty or {}
    return {
        "customer_id": f"CRM-{idx:04d}",
        "first_name": dirty.get("first_name", first),
        "last_name": dirty.get("last_name", last),
        "email": dirty.get("email", email),
        "phone": dirty.get("phone", phone),
        "address_line1": dirty.get("address_line1", addr),
        "city": dirty.get("city", city),
        "state": dirty.get("state", state),
        "zip": dirty.get("zip", zipc),
        "country": dirty.get("country", country),
        "created_date": rand_date(600),
        "modified_date": rand_date(150),
    }


def erp_row(idx, p, dirty=None):
    first, last, email, phone, addr, city, state, zipc, country = p
    dirty = dirty or {}
    full_name = dirty.get("full_name", f"{last}, {first}")
    return {
        "customer_number": f"ERP-{idx:05d}",
        "full_name": full_name,
        "email_addr": dirty.get("email_addr", email.upper() if idx % 3 == 0 else email),
        "contact_phone": dirty.get("contact_phone", phone.replace("-", "")),
        "addr1": dirty.get("addr1", addr),
        "addr2": dirty.get("addr2", ""),
        "city_code": dirty.get("city_code", city),
        "state_code": dirty.get("state_code", state),
        "postal_code": dirty.get("postal_code", zipc),
        "country_code": dirty.get("country_code", country),
        "last_updated": rand_date(150),
    }


crm_rows, erp_rows = [], []

for i, p in enumerate(PEOPLE, start=1):
    in_crm = i not in ERP_ONLY
    in_erp = i not in CRM_ONLY

    if in_crm:
        dirty = {}
        if i in CRM_DIRTY_MISSING_LASTNAME:
            dirty["last_name"] = ""
        if i in CRM_DIRTY_BAD_EMAIL:
            dirty["email"] = f"{p[0].lower()}.{p[1].lower()}[at]example.com"
        if i in CRM_DIRTY_INVALID_STATE:
            dirty["state"] = "ZZ"
        if i in CRM_DIRTY_MESSY_CASE:
            dirty["first_name"] = f"  {p[0].lower()} "
            dirty["last_name"] = p[1].upper()
        crm_rows.append(crm_row(i, p, dirty))

    if in_erp:
        dirty = {}
        if i in ERP_DIRTY_MISSING_PHONE:
            dirty["contact_phone"] = ""
        if i in ERP_DIRTY_BAD_EMAIL:
            dirty["email_addr"] = f"{p[0].lower()}.{p[1].lower()}@@example"
        if i in ERP_DIRTY_INVALID_COUNTRY:
            dirty["country_code"] = "USA1"
        if i in FUZZY_OVERRIDES:
            dirty.update(FUZZY_OVERRIDES[i])
        erp_rows.append(erp_row(i, p, dirty))

os.makedirs(OUT_DIR, exist_ok=True)

with open(os.path.join(OUT_DIR, "crm_customers.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(crm_rows[0].keys()))
    w.writeheader()
    w.writerows(crm_rows)

with open(os.path.join(OUT_DIR, "erp_customers.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(erp_rows[0].keys()))
    w.writeheader()
    w.writerows(erp_rows)

n_both = len(PEOPLE) - len(CRM_ONLY) - len(ERP_ONLY)
print(f"People: {len(PEOPLE)}  (both={n_both}, crm_only={len(CRM_ONLY)}, erp_only={len(ERP_ONLY)})")
print(f"Fuzzy-only pairs (of the 'both' group): {len(FUZZY_OVERRIDES)}")
print(f"CRM rows: {len(crm_rows)}  ERP rows: {len(erp_rows)}")

"""
Generates synthetic source system extracts for the MDM demo:
  - CRM system export (data/sources/crm_customers.csv)
  - ERP system export (data/sources/erp_customers.csv)

Design intent:
  - ~18 "true" people. Most appear in BOTH systems (to drive match/merge in gold),
    a few appear in only one system.
  - Deliberate data quality issues are seeded so the cleansing/validation layer
    and the stewardship exception queue have real work to do:
      * bad/missing emails
      * inconsistent name casing
      * missing required fields (last name, phone)
      * invalid state/country codes
      * inconsistent phone formats
      * duplicate whitespace / typos
"""
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)
BASE_DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(BASE_DIR, "sources")

PEOPLE = [
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
    in_crm = True
    in_erp = i not in (10, 15, 17)   # 3 people ERP-only... wait these must be CRM-absent instead
    # Explicit split: indices 16,17,18 => ERP only (not in CRM); indices 13,14 => CRM only (not in ERP)
    in_crm = i not in (16, 17, 18)
    in_erp = i not in (13, 14)

    if in_crm:
        dirty = {}
        if i == 2:   # missing last name -> required field violation
            dirty["last_name"] = ""
        if i == 5:   # bad email format
            dirty["email"] = "sofia.moretti[at]example.com"
        if i == 9:   # invalid state code
            dirty["state"] = "ZZ"
        if i == 11:  # messy casing + extra whitespace
            dirty["first_name"] = "  ava "
            dirty["last_name"] = "THOMPSON"
        crm_rows.append(crm_row(i, p, dirty))

    if in_erp:
        dirty = {}
        if i == 3:   # missing phone
            dirty["contact_phone"] = ""
        if i == 6:   # bad email
            dirty["email_addr"] = "david.okafor@@example"
        if i == 12:  # invalid country code
            dirty["country_code"] = "USA1"
        erp_rows.append(erp_row(i, p, dirty))

# A couple of ERP-only "noise" records with no CRM counterpart at all (indices 16-18 already handled via in_erp above naturally since PEOPLE only has 18)

os.makedirs(OUT_DIR, exist_ok=True)

with open(os.path.join(OUT_DIR, "crm_customers.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(crm_rows[0].keys()))
    w.writeheader()
    w.writerows(crm_rows)

with open(os.path.join(OUT_DIR, "erp_customers.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(erp_rows[0].keys()))
    w.writeheader()
    w.writerows(erp_rows)

print(f"CRM rows: {len(crm_rows)}  ERP rows: {len(erp_rows)}")

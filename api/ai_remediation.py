import json
import os
import re

ANTHROPIC_MODEL = "claude-sonnet-4-6"


def _client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=api_key)
    except Exception:
        return None


def suggest_remediation(record: dict, reject_reasons: list[str]) -> dict:
    """
    Returns: {
        "suggested_fields": {...},   # proposed corrected values, only for fields it has an opinion on
        "rationale": "...",
        "source": "ai" | "heuristic_fallback"
    }
    A data steward always reviews and approves before anything is written back
    to the pipeline -- this function only proposes, it never auto-applies.
    """
    client = _client()
    if client is not None:
        try:
            return _ai_suggest(client, record, reject_reasons)
        except Exception as e:
            fallback = _heuristic_suggest(record, reject_reasons)
            fallback["rationale"] = f"(AI call failed: {e}) " + fallback["rationale"]
            return fallback
    return _heuristic_suggest(record, reject_reasons)


def _ai_suggest(client, record, reject_reasons) -> dict:
    prompt = f"""You are assisting a data steward remediating a rejected customer master-data record.

Record (source system: {record.get('source_system')}, source id: {record.get('source_record_id')}):
{json.dumps(record, default=str, indent=2)}

This record failed these validation rules:
{json.dumps(reject_reasons, indent=2)}

Propose corrections ONLY for the fields that failed validation. Do not guess values you
have no evidential basis for (e.g. do not invent a phone number from nothing) -- in that
case, leave the field out of suggested_fields and explain in the rationale that it needs
manual steward input.

Respond with ONLY a JSON object, no other text, in this exact shape:
{{"suggested_fields": {{"<field>": "<value>", ...}}, "rationale": "<1-3 sentences>"}}"""

    resp = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(b.text for b in resp.content if b.type == "text").strip()
    text = re.sub(r"^```(json)?|```$", "", text.strip(), flags=re.MULTILINE).strip()
    parsed = json.loads(text)
    return {
        "suggested_fields": parsed.get("suggested_fields", {}),
        "rationale": parsed.get("rationale", ""),
        "source": "ai",
    }


def _heuristic_suggest(record, reject_reasons) -> dict:
    """Deterministic rule-based fallback so the demo is fully functional without
    an API key configured. Handles the specific patterns seeded in this dataset
    plus a few generically common ones."""
    suggestions = {}
    notes = []

    reasons_text = " ".join(reject_reasons)

    if "invalid email format" in reasons_text:
        email = (record.get("email") or "")
        fixed = email.replace("[at]", "@").replace("@@", "@")
        fixed = re.sub(r"@([^.\s]+)$", r"@\1.com", fixed) if "@" in fixed and "." not in fixed.split("@")[-1] else fixed
        if fixed != email and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", fixed):
            suggestions["email"] = fixed
            notes.append(f"Corrected likely email typo: '{email}' -> '{fixed}'.")
        else:
            notes.append("Email format is invalid and no confident auto-fix was found; needs manual steward input.")

    if "missing last name" in reasons_text:
        notes.append("Last name is missing. Consider checking the source system's original record or contacting the customer; no reliable auto-fix available.")

    if "missing phone" in reasons_text:
        notes.append("Phone number is missing. No auto-fix possible -- requires manual entry or a follow-up data request.")

    if "invalid state code" in reasons_text:
        city = (record.get("city") or "").strip().lower()
        city_to_state = {
            "alexandria": "VA", "washington": "DC", "new york": "NY", "boston": "MA",
        }
        guess = city_to_state.get(city)
        if guess:
            suggestions["state_code"] = guess
            notes.append(f"Inferred state '{guess}' from city '{record.get('city')}'; steward should confirm.")
        else:
            notes.append("State code is invalid and could not be confidently inferred from other fields.")

    if "invalid country code" in reasons_text:
        # In this dataset the only seeded case is a malformed 'USA1' -- an obvious near-match to 'US'
        country = (record.get("country_code") or "")
        if country.upper().startswith("US"):
            suggestions["country_code"] = "US"
            notes.append(f"Country code '{country}' looks like a malformed 'US'; suggesting the ISO code 'US'.")
        else:
            notes.append("Country code is invalid and no confident auto-fix was found.")

    if not notes:
        notes.append("No specific heuristic matched this failure pattern; manual steward review needed.")

    return {
        "suggested_fields": suggestions,
        "rationale": " ".join(notes),
        "source": "heuristic_fallback",
    }

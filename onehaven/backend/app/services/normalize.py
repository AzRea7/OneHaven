import re

DISALLOWED_TYPES = {"condo", "townhouse", "manufactured"}

# A tiny normalizer you can expand
def normalize_property_type(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip().lower()
    s = s.replace("/", " ")
    s = re.sub(r"\s+", " ", s)

    if "manufact" in s:
        return "manufactured"
    if "condo" in s or "town" in s:
        return "condo"
    if "multi" in s or "duplex" in s or "triplex" in s or "fourplex" in s:
        return "multi_family"
    if "single" in s or "sfr" in s or "house" in s:
        return "single_family"
    return s


def is_allowed_type(norm_type: str | None) -> bool:
    if not norm_type:
        return True  # allow unknown at ingestion; you can tighten later
    return norm_type not in DISALLOWED_TYPES

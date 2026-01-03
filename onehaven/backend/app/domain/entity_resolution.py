from dataclasses import dataclass

@dataclass(frozen=True)
class CanonicalAddress:
    address_line: str
    city: str
    state: str
    zipcode: str


def canonicalize_address(address_line: str, city: str, state: str, zipcode: str) -> CanonicalAddress:
    # Minimal canonicalization — you can replace with USPS lib later
    return CanonicalAddress(
        address_line=address_line.strip().upper(),
        city=city.strip().upper(),
        state=state.strip().upper(),
        zipcode=zipcode.strip(),
    )

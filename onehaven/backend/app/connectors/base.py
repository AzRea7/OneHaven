from dataclasses import dataclass
from typing import Any

@dataclass
class RawLead:
    source: str
    source_ref: str | None
    payload: dict[str, Any]
    provenance: dict[str, Any]

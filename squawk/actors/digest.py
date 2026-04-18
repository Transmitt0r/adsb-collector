"""DigestActor — generates and broadcasts weekly digests.

DigestOutput is defined here and imported by:
- squawk/repositories/digest.py (get_cached return type)
- squawk/bot/broadcaster.py (broadcast argument type)

The full DigestActor, DigestClient protocol, and _GeminiDigestClient are
implemented in Phase 7 (task 7.7).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DigestOutput:
    text: str
    photo_url: str | None
    photo_caption: str | None

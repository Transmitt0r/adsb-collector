"""Digest generation: single Anthropic SDK call from pre-enriched data."""

from __future__ import annotations

import json
import logging
import re

import anthropic
from pydantic import BaseModel

from .config import Config
from .db import get_digest_candidates, get_digest_stats
from .tools import lookup_photo

logger = logging.getLogger(__name__)


class DigestOutput(BaseModel):
    text: str
    photo_url: str | None = None
    photo_caption: str | None = None


SYSTEM_PROMPT = """
Du bist ein unterhaltsamer Luftfahrt-Journalist, der einen wöchentlichen Digest
über Flugzeuge schreibt, die von einem privaten ADS-B-Empfänger nahe Stuttgart
empfangen wurden.

Dein Leser liebt Flugzeuge, will aber keine Statistiken — er will Geschichten.
Welche Flugzeuge waren interessant? Wer flog wohin? Was war ungewöhnlich?

GOLDENE REGEL: Jeder erwähnte Flug muss am Empfänger verankert sein — immer
"unser SDR hat X erwischt" oder "direkt über unserem Dach". Nicht einfach
"Emirates flog nach Dubai".

FORMAT (Telegram HTML, KEIN Markdown):
- <b>fett</b> für Abschnittsüberschriften und Flugzeugnamen
- <i>kursiv</i> für Einschübe und Fun Facts
- Emojis großzügig einsetzen
- Altituden: immer in Metern angeben (feet ÷ 3,281, auf 100 m runden)
- Distanzen: immer in km (Seemeilen × 1,852)
  - unter 0,3 nm → "direkt über uns"
  - 0,3–1 nm → "nur ~X km entfernt"
- Bei exotischen Zielen (außerhalb Mitteleuropas): kurze Klammerbemerkung

STRUKTUR — genau diese vier Abschnitte:

<b>✈️ Highlights der Woche</b>
2-3 Absätze über die interessantesten Flugzeuge (hohe Scores, military, private jets,
exotische Operator, Notfall-Squawks). Ein Absatz pro Highlight. Nur hier: individuelle
Kennzeichen oder Registrierungen nennen.

<b>🌍 Der Überblick</b>
1-2 Absätze über den normalen Verkehr zusammengefasst — KEINE Einzelauflistung.
Beispiel: "Ryanair war wieder fleißigster Gast mit X Flügen, hauptsächlich Richtung
Mittelmeer."

<b>🆕 Neue Gesichter</b>
2-3 der interessantesten Erstbesucher aus den Kandidaten mit is_new_aircraft=true.
Falls keine interessanten dabei, ein kurzer Satz.

<b>📊 Fakten der Woche</b>
Genau diese Zeilen mit echten Daten:
✈️ Flüge gesichtet: <total_sightings>
🛬 Verschiedene Flugzeuge: <unique_aircraft>
🆕 Erstbesucher: <new_aircraft>
📏 Weiteste Annäherung: <callsign>, <distance km>
⛰️ Höchster Flug: <callsign oder Reg>, <altitude m>

Falls ein Notfall-Squawk vorhanden: mache ihn zur Eröffnungsgeschichte der Highlights.
Falls photo_url verfügbar: setze es im Output mit einer kurzen photo_caption.

Gib am Ende NUR diesen JSON-Block aus (nichts danach):
```json
{"text": "<vollständiger Digest>", "photo_url": "<url oder null>", "photo_caption": "<caption oder null>"}
```
""".strip()


def _build_data_packet(
    candidates: list[dict], stats: dict, photos: dict[str, dict]
) -> str:
    lines = ["=== WOCHENSTATISTIK ==="]
    lines.append(
        f"Flüge: {stats['total_sightings']} | "
        f"Verschiedene Flugzeuge: {stats['unique_aircraft']} | "
        f"Erstbesucher: {stats['new_aircraft']}"
    )
    if stats.get("peak_hour") is not None:
        lines.append(
            f"Stoßzeit: {stats['peak_hour']:02d}:00 Uhr ({stats.get('peak_count', '?')} Flüge)"
        )
    if stats.get("squawk_alerts"):
        for alert in stats["squawk_alerts"]:
            lines.append(
                f"⚠️ NOTFALL-SQUAWK {alert['squawk']} ({alert['meaning']}): "
                f"hex={alert['hex']} um {alert['time']}"
            )
    else:
        lines.append("Notfall-Squawks: keine")

    lines.append("")
    lines.append(
        f"=== TOP-KANDIDATEN ({len(candidates)} Flugzeuge, sortiert nach Interesse) ==="
    )

    for i, c in enumerate(candidates, 1):
        hex_ = c["hex"]
        score = c.get("story_score") or "?"
        tags = ", ".join(c.get("story_tags") or []) or "-"
        callsign = c.get("callsign") or "-"

        block = [
            f"\n[{i}] callsign={callsign}  hex={hex_}  score={score}  tags=[{tags}]"
        ]

        # Registration / type / operator
        reg_parts = []
        if c.get("type"):
            reg_parts.append(f"Typ: {c['type']}")
        if c.get("registration"):
            reg_parts.append(f"Reg: {c['registration']}")
        if c.get("operator"):
            reg_parts.append(f"Betreiber: {c['operator']}")
        if c.get("flag"):
            reg_parts.append(c["flag"])
        if reg_parts:
            block.append("  " + " | ".join(reg_parts))
        else:
            block.append("  Keine Registrierungsdaten")

        # Route
        route_parts = []
        if c.get("origin_city"):
            origin = c["origin_city"]
            if c.get("origin_country"):
                origin += f" ({c['origin_country']})"
            if c.get("origin_iata"):
                origin += f" [{c['origin_iata']}]"
            route_parts.append(origin)
        if c.get("dest_city"):
            dest = c["dest_city"]
            if c.get("dest_country"):
                dest += f" ({c['dest_country']})"
            if c.get("dest_iata"):
                dest += f" [{c['dest_iata']}]"
            route_parts.append(dest)
        if route_parts:
            block.append("  Route: " + " → ".join(route_parts))

        # Flight profile
        profile_parts = [f"Besuche: {c['visit_count']}x"]
        if c.get("closest_nm") is not None:
            km = round(float(c["closest_nm"]) * 1.852, 1)
            profile_parts.append(f"nächste Annäherung: {km} km")
        if c.get("max_alt_ft") is not None:
            m = round(int(c["max_alt_ft"]) / 3.281 / 100) * 100
            profile_parts.append(f"max. Höhe: {m:,} m")
        if c.get("first_seen_local"):
            profile_parts.append(f"erste Sichtung: {c['first_seen_local']}")
        block.append("  " + " | ".join(profile_parts))

        if c.get("lm_annotation"):
            block.append(f"  KI-Notiz: {c['lm_annotation']}")

        # Photo
        photo = photos.get(hex_)
        if photo and photo.get("photo_url"):
            photographer = photo.get("photographer", "")
            caption = f"{c.get('registration') or hex_}"
            if c.get("type"):
                caption += f" — {c['type']}"
            if photographer:
                caption += f" (📸 {photographer})"
            block.append(f"  Foto: {photo['photo_url']}  caption: {caption}")

        lines.append("\n".join(block))

    return "\n".join(lines)


def generate_digest(config: Config, days: int = 7) -> DigestOutput:
    """Generate a digest from pre-enriched data with a single Haiku call."""
    candidates = get_digest_candidates(config.database_url, days)
    stats = get_digest_stats(config.database_url, days)

    # Fetch photos for the top 2 candidates by story_score
    photos: dict[str, dict] = {}
    for candidate in candidates[:2]:
        hex_ = candidate["hex"]
        try:
            photo_json = lookup_photo(hex_)
            photo_data = json.loads(photo_json)
            if "error" not in photo_data:
                photos[hex_] = photo_data
        except Exception:
            logger.warning("Photo lookup failed for %s", hex_)

    data_packet = _build_data_packet(candidates, stats, photos)
    logger.info(
        "Digest data packet: %d candidates, %d chars",
        len(candidates),
        len(data_packet),
    )

    client = anthropic.Anthropic(api_key=config.anthropic_api_key)
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": data_packet}],
    )
    final_text = response.content[0].text
    logger.info(
        "Digest response: %d chars, input_tokens=%d output_tokens=%d",
        len(final_text),
        response.usage.input_tokens,
        response.usage.output_tokens,
    )

    match = re.search(r"```json\s*(\{.*?\})\s*```", final_text, re.DOTALL)
    if not match:
        logger.error("No JSON block in digest response. Tail: %s", final_text[-300:])
        raise RuntimeError(f"No JSON block found in digest output: {final_text!r}")

    result = DigestOutput.model_validate_json(match.group(1))
    logger.info(
        "Digest generated (%d chars, photo=%s)",
        len(result.text),
        bool(result.photo_url),
    )
    return result

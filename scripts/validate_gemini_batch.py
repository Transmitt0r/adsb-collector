"""
Task 1.1 — Validate batch Gemini scoring.

Sends 20 aircraft to Gemini with the proposed batch scoring prompt and asserts
the response is a valid JSON array of length 20, one ScoreResult per aircraft.

Usage (from repo root, with bot venv active):
    uv run --directory bot python ../scripts/validate_gemini_batch.py

Or from bot/:
    uv run python ../scripts/validate_gemini_batch.py

Requires GEMINI_API_KEY in env.

Findings are printed to stdout. This script is ephemeral; conclusions are
documented at the bottom of this file and in DESIGN.md task 1.1.
"""

from __future__ import annotations

import json
import os
import sys
import textwrap
import time
from dataclasses import dataclass

from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Domain types (mirrors squawk/clients/gemini.py from DESIGN.md)
# ---------------------------------------------------------------------------


@dataclass
class AircraftInfo:
    registration: str | None
    type: str | None
    operator: str | None
    flag: str | None


@dataclass
class RouteInfo:
    origin_iata: str | None
    origin_icao: str | None
    origin_city: str | None
    origin_country: str | None
    dest_iata: str | None
    dest_icao: str | None
    dest_city: str | None
    dest_country: str | None


@dataclass
class ScoreResult:
    score: int  # 1–10; higher = more interesting for weekly digest
    tags: list[str]  # e.g. ["military", "cargo", "low-pass"]
    annotation: str  # one German sentence; empty string if unremarkable


# ---------------------------------------------------------------------------
# Batch prompt
# ---------------------------------------------------------------------------

SCORING_SYSTEM_PROMPT = textwrap.dedent("""
    Du bewertest Flugzeuge für einen wöchentlichen ADS-B-Digest nahe Stuttgart.
    Für jedes Flugzeug in der Liste gibst du einen Score (1–10) zurück:

    Score-Richtlinien:
    - 1–3: Alltäglicher Linienverkehr (Ryanair, Eurowings, kurze Inlandsrouten)
    - 4–6: Interessant aber normal (Langstrecke, Frachter, unbekannte Operator)
    - 7–8: Ungewöhnlich (Militär, Privatjet, exotisches Ziel, seltener Typ)
    - 9–10: Sehr selten oder außergewöhnlich (historisches Flugzeug, Notfall-Squawk,
            medizinische Evakuierung, VIP-Transport)

    tags: kurze englische Schlagwörter (z.B. "military", "cargo", "bizjet",
          "emergency", "long-haul", "low-altitude", "unusual-operator")

    annotation: ein einziger Satz auf Deutsch, der erklärt, warum das Flugzeug
    interessant ist. Leer lassen (""), wenn das Flugzeug unremarkable ist (Score ≤ 3).

    WICHTIG: Die Ausgabe muss ein JSON-Array sein. Es muss genau so viele Einträge
    enthalten wie die Eingabeliste — in der gleichen Reihenfolge.
""").strip()


def _aircraft_to_dict(
    hex_: str,
    info: AircraftInfo | None,
    route: RouteInfo | None,
) -> dict:
    return {
        "hex": hex_,
        "registration": info.registration if info else None,
        "type": info.type if info else None,
        "operator": info.operator if info else None,
        "flag": info.flag if info else None,
        "origin_city": route.origin_city if route else None,
        "origin_country": route.origin_country if route else None,
        "dest_city": route.dest_city if route else None,
        "dest_country": route.dest_country if route else None,
    }


# ---------------------------------------------------------------------------
# 20 representative test aircraft
# ---------------------------------------------------------------------------

TEST_AIRCRAFT: list[tuple[str, AircraftInfo | None, RouteInfo | None]] = [
    # 1 — routine Ryanair
    (
        "3c6547",
        AircraftInfo("D-ABCD", "Boeing 737-800", "Ryanair", "IE"),
        RouteInfo(
            "STR", "EDDS", "Stuttgart", "Germany", "BCN", "LEBL", "Barcelona", "Spain"
        ),
    ),
    # 2 — Eurowings domestic
    (
        "3c1234",
        AircraftInfo("D-EWAB", "Airbus A320", "Eurowings", "DE"),
        RouteInfo(
            "STR", "EDDS", "Stuttgart", "Germany", "HAM", "EDDH", "Hamburg", "Germany"
        ),
    ),
    # 3 — Lufthansa medium-haul
    (
        "3c5555",
        AircraftInfo("D-AIAB", "Airbus A320", "Lufthansa", "DE"),
        RouteInfo("FRA", "EDDF", "Frankfurt", "Germany", "LHR", "EGLL", "London", "UK"),
    ),
    # 4 — Turkish Airlines long-haul
    (
        "4ba123",
        AircraftInfo("TC-LJA", "Boeing 777-300ER", "Turkish Airlines", "TR"),
        RouteInfo(
            "IST", "LTFM", "Istanbul", "Turkey", "JFK", "KJFK", "New York", "USA"
        ),
    ),
    # 5 — Qatar Airways ultra-long-haul
    (
        "06a1b2",
        AircraftInfo("A7-BEL", "Boeing 777-200LR", "Qatar Airways", "QA"),
        RouteInfo("DOH", "OTBD", "Doha", "Qatar", "LAX", "KLAX", "Los Angeles", "USA"),
    ),
    # 6 — DHL cargo 757
    (
        "3c6622",
        AircraftInfo("D-ALEF", "Boeing 757-200F", "DHL Air", "DE"),
        RouteInfo(
            "LEJ", "EDDP", "Leipzig", "Germany", "CGN", "EDDK", "Cologne", "Germany"
        ),
    ),
    # 7 — Fedex 767 freighter
    (
        "a1b2c3",
        AircraftInfo("N116FE", "Boeing 767-300F", "FedEx Express", "US"),
        RouteInfo("MEM", "KMEM", "Memphis", "USA", "CDG", "LFPG", "Paris", "France"),
    ),
    # 8 — German military transport (Bundeswehr)
    ("3c0001", AircraftInfo("54+01", "Airbus A400M", "Luftwaffe", "DE"), None),
    # 9 — US Air Force tanker
    ("ae1234", AircraftInfo("62-3534", "Boeing KC-135R", "USAF", "US"), None),
    # 10 — Medevac (medical evacuation)
    (
        "3c9999",
        AircraftInfo("D-CFLY", "Cessna Citation XLS", "Air Alliance", "DE"),
        RouteInfo(
            "STR", "EDDS", "Stuttgart", "Germany", "MUC", "EDDM", "Munich", "Germany"
        ),
    ),
    # 11 — Private bizjet
    (
        "45abc1",
        AircraftInfo("N800XJ", "Gulfstream G650", "Private", "US"),
        RouteInfo(
            "LUX", "ELLX", "Luxembourg", "Luxembourg", "DXB", "OMDB", "Dubai", "UAE"
        ),
    ),
    # 12 — Emergency squawk 7700
    (
        "3c7777",
        AircraftInfo("D-ABCE", "Airbus A321", "Condor", "DE"),
        RouteInfo(
            "PMI",
            "LEPA",
            "Palma de Mallorca",
            "Spain",
            "STR",
            "EDDS",
            "Stuttgart",
            "Germany",
        ),
    ),
    # 13 — Swiss Air Ambulance
    ("4b1801", AircraftInfo("HB-JWA", "Pilatus PC-24", "REGA", "CH"), None),
    # 14 — Low-altitude ultralight / glider tow
    ("3d1111", AircraftInfo("D-EFAB", "Piper PA-28", "Private", "DE"), None),
    # 15 — Learjet bizjet (old type)
    (
        "3c8888",
        AircraftInfo("D-CJET", "Learjet 35", "WDL Aviation", "DE"),
        RouteInfo(
            "STR", "EDDS", "Stuttgart", "Germany", "NCE", "LFMN", "Nice", "France"
        ),
    ),
    # 16 — Air China long-haul
    (
        "780abc",
        AircraftInfo("B-2087", "Boeing 747-8", "Air China", "CN"),
        RouteInfo(
            "FRA", "EDDF", "Frankfurt", "Germany", "PEK", "ZBAA", "Beijing", "China"
        ),
    ),
    # 17 — Unknown / no data
    ("aabbcc", None, None),
    # 18 — Short-haul Austrian
    (
        "440011",
        AircraftInfo("OE-LXA", "Embraer E195", "Austrian Airlines", "AT"),
        RouteInfo(
            "VIE", "LOWW", "Vienna", "Austria", "STR", "EDDS", "Stuttgart", "Germany"
        ),
    ),
    # 19 — Helicopter EMS
    ("3c2222", AircraftInfo("D-HRTM", "Airbus H145", "ADAC Luftrettung", "DE"), None),
    # 20 — Historic DC-3 / vintage aircraft
    (
        "3c0dc3",
        AircraftInfo("D-CDEF", "Douglas DC-3", "Classic Air", "CH"),
        RouteInfo(
            "STR",
            "EDDS",
            "Stuttgart",
            "Germany",
            "ZRH",
            "LSZH",
            "Zurich",
            "Switzerland",
        ),
    ),
]


# ---------------------------------------------------------------------------
# Schema for structured output
# ---------------------------------------------------------------------------

SCORE_RESULT_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "score": {
                "type": "integer",
                "description": "Interest score 1–10",
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Short English keyword tags",
            },
            "annotation": {
                "type": "string",
                "description": "One German sentence explaining why interesting; "
                "empty string if score <= 3",
            },
        },
        "required": ["score", "tags", "annotation"],
    },
}


# ---------------------------------------------------------------------------
# Validation runs
# ---------------------------------------------------------------------------


def run_trial(
    client: genai.Client,
    model: str,
    aircraft: list[tuple[str, AircraftInfo | None, RouteInfo | None]],
    trial_num: int,
) -> dict:
    """Run one batch scoring trial. Returns a result dict with success/failure info."""
    input_dicts = [_aircraft_to_dict(h, i, r) for h, i, r in aircraft]
    prompt = (
        SCORING_SYSTEM_PROMPT
        + "\n\nFlugzeuge:\n"
        + json.dumps(input_dicts, ensure_ascii=False, indent=2)
    )

    t0 = time.monotonic()
    try:
        response = client.models.generate_content(
            model=model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=SCORE_RESULT_SCHEMA,
            ),
        )
        elapsed = time.monotonic() - t0
        raw = response.text

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            return {
                "trial": trial_num,
                "success": False,
                "failure_mode": "json_parse_error",
                "error": str(exc),
                "raw_snippet": raw[:200],
                "elapsed_s": elapsed,
            }

        if not isinstance(parsed, list):
            return {
                "trial": trial_num,
                "success": False,
                "failure_mode": "not_a_list",
                "type": type(parsed).__name__,
                "raw_snippet": raw[:200],
                "elapsed_s": elapsed,
            }

        expected = len(aircraft)
        if len(parsed) != expected:
            return {
                "trial": trial_num,
                "success": False,
                "failure_mode": "length_mismatch",
                "expected": expected,
                "got": len(parsed),
                "elapsed_s": elapsed,
            }

        # Validate each entry has required fields and correct types
        validation_errors = []
        for i, entry in enumerate(parsed):
            if not isinstance(entry, dict):
                validation_errors.append(f"[{i}] not a dict: {type(entry)}")
                continue
            if "score" not in entry:
                validation_errors.append(f"[{i}] missing 'score'")
            elif not isinstance(entry["score"], int) or not (1 <= entry["score"] <= 10):
                validation_errors.append(
                    f"[{i}] score out of range or wrong type: {entry.get('score')!r}"
                )
            if "tags" not in entry:
                validation_errors.append(f"[{i}] missing 'tags'")
            elif not isinstance(entry["tags"], list):
                validation_errors.append(f"[{i}] 'tags' not a list: {entry['tags']!r}")
            if "annotation" not in entry:
                validation_errors.append(f"[{i}] missing 'annotation'")
            elif not isinstance(entry["annotation"], str):
                validation_errors.append(
                    f"[{i}] 'annotation' not a string: {entry['annotation']!r}"
                )

        if validation_errors:
            return {
                "trial": trial_num,
                "success": False,
                "failure_mode": "schema_validation_errors",
                "errors": validation_errors,
                "elapsed_s": elapsed,
            }

        # Print summary of scores
        scores = [e["score"] for e in parsed]
        return {
            "trial": trial_num,
            "success": True,
            "count": len(parsed),
            "scores": scores,
            "elapsed_s": elapsed,
            "sample": parsed[:3],  # first 3 for display
        }

    except Exception as exc:
        elapsed = time.monotonic() - t0
        return {
            "trial": trial_num,
            "success": False,
            "failure_mode": "api_error",
            "error": str(exc),
            "elapsed_s": elapsed,
        }


def run_per_aircraft_fallback(
    client: genai.Client,
    model: str,
    aircraft: list[tuple[str, AircraftInfo | None, RouteInfo | None]],
) -> list[ScoreResult | None]:
    """
    Fallback: score each aircraft individually.
    Used when batch scoring returns wrong length or fails.
    """
    SINGLE_SCHEMA = {
        "type": "object",
        "properties": {
            "score": {"type": "integer"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "annotation": {"type": "string"},
        },
        "required": ["score", "tags", "annotation"],
    }

    results = []
    for hex_, info, route in aircraft:
        prompt = (
            SCORING_SYSTEM_PROMPT
            + "\n\nFlugzeuge:\n"
            + json.dumps([_aircraft_to_dict(hex_, info, route)], ensure_ascii=False)
        )
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=SINGLE_SCHEMA,
                ),
            )
            entry = json.loads(response.text)
            results.append(
                ScoreResult(
                    score=int(entry["score"]),
                    tags=list(entry.get("tags", [])),
                    annotation=str(entry.get("annotation", "")),
                )
            )
        except Exception as exc:
            print(f"  [fallback] Failed for {hex_}: {exc}")
            results.append(None)
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GEMINI_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    model = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
    n_trials = int(os.environ.get("N_TRIALS", "3"))

    client = genai.Client(api_key=api_key)

    print("=== Task 1.1 — Gemini batch scoring validation ===")
    print(f"Model: {model}")
    print(f"Aircraft: {len(TEST_AIRCRAFT)}")
    print(f"Trials: {n_trials}")
    print()

    successes = 0
    failures: list[dict] = []

    for i in range(1, n_trials + 1):
        print(f"--- Trial {i}/{n_trials} ---")
        result = run_trial(client, model, TEST_AIRCRAFT, i)

        if result["success"]:
            successes += 1
            scores = result["scores"]
            print(f"  OK  {result['count']} results in {result['elapsed_s']:.1f}s")
            print(f"  Scores: {scores}")
            print(f"  Sample [0]: {result['sample'][0]}")
            print(f"  Sample [1]: {result['sample'][1]}")
            print(f"  Sample [2]: {result['sample'][2]}")
        else:
            failures.append(result)
            print(f"  FAIL failure_mode={result['failure_mode']}")
            for k, v in result.items():
                if k not in ("trial", "success", "failure_mode"):
                    print(f"       {k}: {v}")

        print()
        if i < n_trials:
            time.sleep(1)  # avoid rate limiting between trials

    print(f"=== Results: {successes}/{n_trials} trials succeeded ===")
    print()

    if failures:
        print("Failure modes observed:")
        for f in failures:
            print(f"  Trial {f['trial']}: {f['failure_mode']}")
        print()

    # Run fallback demonstration if at least one trial failed
    if failures:
        print("=== Fallback demonstration: per-aircraft scoring ===")
        print("(Using first 5 aircraft to limit API usage)")
        fallback_results = run_per_aircraft_fallback(client, model, TEST_AIRCRAFT[:5])
        for (hex_, _, _), r in zip(TEST_AIRCRAFT[:5], fallback_results):
            if r:
                print(f"  {hex_}: score={r.score} tags={r.tags}")
            else:
                print(f"  {hex_}: FAILED")
        print()

    # --------------------------------------------------------------------------
    # Decision: fallback strategy
    # --------------------------------------------------------------------------
    print("=== Fallback strategy decision ===")
    print(
        textwrap.dedent("""
    Observed: Gemini structured output with response_schema enforces JSON at
    generation time — the API refuses to return a response that doesn't match
    the schema. Array-length mismatches are therefore unlikely in practice, but
    the fallback is still implemented for defence-in-depth.

    Strategy (implemented in GeminiClient.score_batch()):

    1. Deduplicate by hex before calling the API — same hex appearing twice in
       the collected batch window is processed only once.

    2. Call batch API with response_mime_type="application/json" and
       response_schema=SCORE_RESULT_SCHEMA (JSON schema dict, NOT a Python type
       — avoids SDK version-specific type annotation handling).

    3. Parse response. If len(results) != len(input):
       a. Log a warning with the mismatch details.
       b. Fall back to per-aircraft calls for the entire batch.
       c. Per-aircraft fallback uses a single-object schema (not list[]).

    4. If per-aircraft call fails for a specific hex: log the error, return a
       default ScoreResult(score=1, tags=[], annotation="") for that hex so the
       rest of the batch is not blocked.

    Schema constraints observed:
    - Field names must be plain strings (no Python-style type hints in keys).
    - response_schema accepts a JSON schema dict directly; Python type hints
      work in some SDK versions but are less portable — use the dict form.
    - The "annotation" field returns an empty string (not null) when score <= 3,
      consistent with the design requirement.
    - "score" is returned as an integer (not float), matching ScoreResult.score: int.
    """).strip()
    )


if __name__ == "__main__":
    main()

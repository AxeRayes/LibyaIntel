import json
import os
import re

import requests
from requests.exceptions import ConnectionError, ReadTimeout, Timeout
from dotenv import load_dotenv

load_dotenv()

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))
OLLAMA_DISABLE = os.getenv("OLLAMA_DISABLE", "").strip().lower() in {"1", "true", "yes"}


def _empty_entities() -> dict:
    return {"orgs": [], "people": [], "locations": [], "topics": []}


def is_ollama_healthy(timeout: tuple[int, int] = (1, 2)) -> bool:
    if OLLAMA_DISABLE:
        return False
    try:
        r = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def _clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    return s


def _extract_json(text: str) -> dict:
    """
    Ollama sometimes wraps JSON in text. We recover the first {...} block.
    """
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass

    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("No JSON object found in model output")
    return json.loads(m.group(0))


def extract_entities(
    title: str | None,
    summary: str | None,
    content: str | None,
    lang: str | None = None,
) -> dict:
    """
    Returns:
      { "orgs":[], "people":[], "locations":[], "topics":[] }
    """
    if OLLAMA_DISABLE:
        print("ENTITY_SKIP reason=ollama_disabled")
        return _empty_entities()

    title = _clean_text(title or "")
    summary = _clean_text(summary or "")
    content = _clean_text(content or "")

    max_chars = int(os.getenv("ENTITY_TEXT_MAX_CHARS", "4000"))
    text = (content or summary or "")
    text = text[:max_chars]

    prompt = f"""
You are extracting entities from a Libya-related news item.
Return ONLY valid JSON matching this schema:

{{
  "orgs": ["..."],
  "people": ["..."],
  "locations": ["..."],
  "topics": ["..."]
}}

Rules:
- Use the original language of the entity name as it appears (Arabic stays Arabic, English stays English).
- Do not invent entities not present in the text.
- Keep each list unique, max 15 items.
- Topics should be short phrases (1-4 words).
- Output JSON only. No explanation.

Language hint: {lang or "unknown"}

Title: {title}

Text: {text}
""".strip()

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
    }

    for attempt in range(2):
        try:
            r = requests.post(
                f"{OLLAMA_HOST}/api/generate", json=payload, timeout=(10, OLLAMA_TIMEOUT)
            )
            r.raise_for_status()
            data = r.json()
            break
        except (ConnectionError, Timeout):
            print("ENTITY_SKIP reason=ollama_unavailable")
            return _empty_entities()
        except ReadTimeout:
            if attempt == 1:
                print("ENTITY_SKIP reason=ollama_timeout")
                return _empty_entities()
    else:
        print("ENTITY_SKIP reason=ollama_failed")
        return _empty_entities()
    raw = data.get("response", "")

    obj = _extract_json(raw)

    def uniq(xs):
        out = []
        seen = set()
        for x in xs or []:
            x = _clean_text(str(x))
            if not x or x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out[:15]

    return {
        "orgs": uniq(obj.get("orgs")),
        "people": uniq(obj.get("people")),
        "locations": uniq(obj.get("locations")),
        "topics": uniq(obj.get("topics")),
    }

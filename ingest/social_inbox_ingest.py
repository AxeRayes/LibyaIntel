import json
import os
import time
from typing import Any, Dict

import requests
import traceback
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout

from backend.db import (
    finish_ingest_run,
    get_source_id,
    should_extract_entities,
    start_ingest_run,
    get_client,
    upsert_feed_item,
)
from backend.ollama import extract_entities, is_ollama_healthy
from backend.config import get_bool, get_int

load_dotenv()

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))


def ollama_generate(prompt: str) -> str:
    for attempt in range(2):
        try:
            resp = requests.post(
                OLLAMA_URL,
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
                timeout=(10, OLLAMA_TIMEOUT),
            )
            resp.raise_for_status()
            data = resp.json()
            return (data.get("response") or "").strip()
        except ReadTimeout:
            if attempt == 1:
                raise
    return ""


def translate_to_english(text: str) -> str:
    # Fail-soft: if model chokes, return empty translated text.
    prompt = (
        "Translate the following text to English. Preserve names and numbers.\n\n"
        f"{text}\n"
    )
    try:
        return ollama_generate(prompt)
    except Exception:
        return ""


def summarize_bullets(text: str) -> str:
    prompt = "Summarise in 3 concise bullet points:\n\n" + text
    try:
        return ollama_generate(prompt)
    except Exception:
        return ""


def detect_language_fast(text: str) -> str:
    # Cheap heuristic: Arabic unicode range presence
    for ch in text:
        if "\u0600" <= ch <= "\u06FF" or "\u0750" <= ch <= "\u077F" or "\u08A0" <= ch <= "\u08FF":
            return "ar"
    return "en"


def load_social_sources() -> Dict[str, Any]:
    path = os.path.join(os.path.dirname(__file__), "sources_social.json")
    with open(path, "r", encoding="utf-8") as f:
        items = json.load(f)
    return {item["id"]: item for item in items}

def make_title(text: str, limit: int = 80) -> str:
    if not text:
        return ""
    text = " ".join(text.split())
    return text[:limit].rstrip()


def main() -> int:
    sb = get_client()
    run_id = start_ingest_run(sb, "social_inbox_ingest")
    stats = {
        "total": 0,
        "saved": 0,
        "failed": 0,
        "blocked": 0,
        "llm_failed": 0,
        "llm_unavailable": 0,
        "skipped": 0,
        "by_source": {},
    }
    sources = load_social_sources()
    inbox_path = os.path.join(os.path.dirname(__file__), "social_inbox.jsonl")

    if not os.path.exists(inbox_path):
        finish_ingest_run(sb, run_id, False, stats, error=f"Missing inbox file: {inbox_path}")
        raise SystemExit(f"Missing inbox file: {inbox_path}")

    inserted = 0
    skipped = 0
    processed = 0
    started_ts = time.monotonic()
    finished = False
    error_msg = None
    aborted = False
    max_items = get_int("MAX_ITEMS", 0) or 0
    progress_every = get_int("PROGRESS_EVERY", 5) or 5

    def _bs(source_key: str) -> dict:
        return stats["by_source"].setdefault(
            source_key,
            {
                "total": 0,
                "saved": 0,
                "failed": 0,
                "blocked": 0,
                "llm_failed": 0,
                "skipped": 0,
                "last_error": None,
            },
        )

    ollama_ok = True
    if get_bool("EXTRACT_ENTITIES", False):
        if not is_ollama_healthy():
            ollama_ok = False
            stats["llm_unavailable"] += 1
            stats["warnings"] = ["ollama_unreachable"]
        stats["llm_available"] = ollama_ok

    try:
        with open(inbox_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if max_items and processed >= max_items:
                    break

                item = json.loads(line)
                source_id = item.get("source_id")
                post_url = item.get("post_url")
                captured_text = (item.get("captured_text") or "").strip()
                captured_at = item.get("captured_at") or item.get("published_at")

                if not source_id or not post_url:
                    print("Skipped (missing source_id or post_url)")
                    skipped += 1
                    stats["skipped"] += 1
                    continue

                src = sources.get(source_id)
                if not src:
                    print(f"Skipped (unknown source_id): {source_id}")
                    skipped += 1
                    stats["skipped"] += 1
                    continue

                if not src.get("enabled", False):
                    print(f"Skipped (source disabled): {source_id}")
                    skipped += 1
                    stats["skipped"] += 1
                    continue

                try:
                    source_uuid = get_source_id(sb, source_id)
                except ValueError:
                    print(f"Skipped (source not seeded in sources table): {source_id}")
                    skipped += 1
                    stats["skipped"] += 1
                    continue

                stats["total"] += 1
                bs = _bs(source_id)
                bs["total"] += 1
                processed += 1

                lang = detect_language_fast(captured_text) if captured_text else None

                translated = ""
                summary = ""

                # Only translate/summarize if we have text. If not, store the link only.
                if captured_text:
                    if ollama_ok:
                        if lang == "ar":
                            translated = translate_to_english(captured_text)
                            if not translated:
                                stats["llm_failed"] += 1
                                bs["llm_failed"] += 1
                                bs["last_error"] = "translate_error"
                            base_for_summary = translated or captured_text
                        else:
                            base_for_summary = captured_text

                        # Keep summaries short and stable
                        summary = summarize_bullets(base_for_summary)
                        if not summary:
                            stats["llm_failed"] += 1
                            bs["llm_failed"] += 1
                            bs["last_error"] = "summarize_error"
                    else:
                        base_for_summary = captured_text

                feed_item = {
                    "source_id": source_uuid,
                    "source_type": "social",
                    "external_id": item.get("external_id") or post_url,
                    "url": post_url,
                    "title": item.get("title") or make_title(captured_text),
                    "summary": summary or captured_text or None,
                    "content": captured_text or None,
                    "language": lang,
                    "published_at": captured_at,
                    "raw": {
                        "source_id": source_id,
                        "page_url": src.get("page_url"),
                        "page_handle": src.get("page_handle"),
                        "captured_text": captured_text or None,
                        "translated_text": translated or None,
                        "summary": summary or None,
                        "region": (src.get("region_coverage") or ["national"])[0],
                        "input": item,
                    },
                }

                upsert_feed_item(sb, feed_item)
                if ollama_ok and should_extract_entities(feed_item):
                    try:
                        ents = extract_entities(
                            title=feed_item.get("title"),
                            summary=feed_item.get("summary"),
                            content=feed_item.get("content"),
                            lang=feed_item.get("language"),
                        )
                        if feed_item.get("external_id"):
                            sb.table("feed_items").update({"entities": ents}).eq(
                                "external_id", feed_item["external_id"]
                            ).execute()
                        else:
                            sb.table("feed_items").update({"entities": ents}).eq(
                                "hash", feed_item["hash"]
                            ).execute()
                    except Exception:
                        err = traceback.format_exc()
                        print("ENTITY ERROR FULL:\n", err)
                        marker = {"_entity_error": err[:800]}
                        stats["llm_failed"] += 1
                        bs["llm_failed"] += 1
                        bs["last_error"] = "entity_error"
                        if feed_item.get("external_id"):
                            sb.table("feed_items").update({"entities": marker}).eq(
                                "external_id", feed_item["external_id"]
                            ).execute()
                        else:
                            sb.table("feed_items").update({"entities": marker}).eq(
                                "hash", feed_item["hash"]
                            ).execute()
                inserted += 1
                stats["saved"] += 1
                bs["saved"] += 1
                print(f"Saved social feed item: {source_id} | {post_url}")
                if progress_every and processed % progress_every == 0:
                    elapsed = int(time.monotonic() - started_ts)
                    print(
                        f"Progress {processed}: saved={stats['saved']} "
                        f"failed={stats['failed']} blocked={stats['blocked']} "
                        f"llm_failed={stats['llm_failed']} elapsed={elapsed}s"
                    )

                # Be polite to your own machine
                time.sleep(0.2)
    except KeyboardInterrupt:
        aborted = True
        error_msg = "KeyboardInterrupt"
        stats["failed"] += 1
    except Exception:
        stats["failed"] += 1
        error_msg = traceback.format_exc()[:8000]
        raise
    finally:
        print(f"Done. Inserted={inserted} Skipped={skipped}")
        if aborted:
            stats["aborted"] = True
        ok = (
            stats["failed"] == 0
            and stats["blocked"] == 0
            and stats["llm_failed"] == 0
            and stats["llm_unavailable"] == 0
        )
        finish_ingest_run(sb, run_id, ok, stats, error=error_msg)

    return 0 if not aborted else 130


if __name__ == "__main__":
    raise SystemExit(main())

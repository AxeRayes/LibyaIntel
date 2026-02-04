import json
import sys
import traceback
from pathlib import Path
import time

import feedparser

from backend.db import (
    finish_ingest_run,
    get_source_id,
    should_extract_entities,
    start_ingest_run,
    get_client,
    upsert_feed_item,
    is_source_in_cooldown,
    mark_source_blocked,
    get_article_id_by_url,
    upsert_entities_for_article,
)
from backend.ollama import extract_entities, is_ollama_healthy
from backend.config import get_bool, get_int
from .extract import HEADERS, extract_main_text, fetch_url


def _article_has_column(sb, column: str) -> bool:
    try:
        sb.table("articles").select(column).limit(1).execute()
        return True
    except Exception:
        return False

def load_sources(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_published(entry):
    return entry.get("published") or entry.get("updated")


def main():
    sb = get_client()
    run_id = start_ingest_run(sb, "rss_ingest")
    stats = {
        "total": 0,
        "saved": 0,
        "failed": 0,
        "blocked": 0,
        "llm_failed": 0,
        "llm_unavailable": 0,
        "skipped": 0,
        "summary_skipped": 0,
        "summary_failed": 0,
        "by_source": {},
    }
    sources_path = Path(__file__).parent / "sources.json"
    sources = load_sources(sources_path)
    blocked_sources = set()
    processed = 0
    started_ts = time.monotonic()
    finished = False
    error_msg = None
    aborted = False
    do_summary = os.getenv("EXTRACT_SUMMARY", "0") == "1"

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
                "summary_skipped": 0,
                "summary_failed": 0,
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
        source_count = 0
        max_sources = get_int("MAX_SOURCES", 0) or 0
        for source in sources:
            if not source.get("enabled"):
                continue

            rss_url = source.get("rss")
            if not rss_url:
                continue
            source_key = source.get("id")
            if source_key in blocked_sources:
                continue
            if get_bool("SKIP_BLOCKED_SOURCES", True) and is_source_in_cooldown(sb, source_key):
                continue
            source_count += 1
            if max_sources and source_count > max_sources:
                break

            feed = feedparser.parse(rss_url)

            for entry in feed.entries:
                title = entry.get("title")
                link = entry.get("link")

                if not link:
                    stats["skipped"] += 1
                    bs = _bs(source_key)
                    bs["skipped"] += 1
                    continue
                stats["total"] += 1
                bs = _bs(source_key)
                bs["total"] += 1
                processed += 1

                try:
                    html, err = fetch_url(link, HEADERS)
                    if err:
                        stats["failed"] += 1
                        bs["failed"] += 1
                        bs["last_error"] = err
                        if err.startswith("blocked:"):
                            stats["blocked"] += 1
                            bs["blocked"] += 1
                            blocked_sources.add(source_key)
                            mark_source_blocked(sb, source_key)
                        print(f"Failed: {link} -> {err}", file=sys.stderr)
                        if err.startswith("blocked:"):
                            break
                        continue
                    content = extract_main_text(html or "")
                except Exception as e:
                    stats["failed"] += 1
                    bs["failed"] += 1
                    bs["last_error"] = f"parse_error:{type(e).__name__}"
                    print(f"Failed: {link} -> {e}", file=sys.stderr)
                    continue

                try:
                    source_uuid = get_source_id(sb, source_key)
                except ValueError:
                    stats["skipped"] += 1
                    bs["skipped"] += 1
                    print(f"Skipped (source not seeded in sources table): {source_key}")
                    continue

                summary = ""
                print(f"SUMMARY_GATE do_summary={do_summary} url={link}")

                feed_item = {
                    "source_id": source_uuid,
                    "source_type": "article",
                    "external_id": entry.get("id") or entry.get("guid") or link,
                    "url": link,
                    "title": title,
                    "summary": summary,
                    "content": content,
                    "language": "unknown",
                    "published_at": get_published(entry),
                    "raw": {"entry": entry, "source": source},
                }

                upsert_feed_item(sb, feed_item)
                stats["summary_skipped"] += 1
                bs["summary_skipped"] += 1

                article_id = None
                try:
                    article_row = {
                        "source": source_key,
                        "title": title,
                        "url": link,
                        "published_at": get_published(entry),
                        "content": content,
                        "summary": summary,
                        "language": "unknown",
                        "credibility": None,
                        "summary_status": "PENDING",
                    }
                    if _article_has_column(sb, "source_id"):
                        article_row["source_id"] = source_key
                    if _article_has_column(sb, "source_name"):
                        article_row["source_name"] = source.get("name") or source_key
                    sb.table("articles").upsert(article_row, on_conflict="url").execute()
                    article_id = get_article_id_by_url(sb, link)
                except Exception as e:
                    stats["failed"] += 1
                    bs["failed"] += 1
                    bs["last_error"] = f"article_upsert_error:{type(e).__name__}"
                    print(f"Article upsert failed for {link}: {e}")
                    continue

                if ollama_ok and should_extract_entities(feed_item):
                    print(
                        "ENTITY_HOOK_REACHED",
                        feed_item.get("external_id"),
                        feed_item.get("hash"),
                    )
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
                        upsert_entities_for_article(sb, article_id, ents)
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
                stats["saved"] += 1
                bs["saved"] += 1
                print(f"Saved: {title}")
                progress_every = get_int("PROGRESS_EVERY", 5) or 5
                if progress_every and processed % progress_every == 0:
                    elapsed = int(time.monotonic() - started_ts)
                    print(
                        f"Progress {processed}: saved={stats['saved']} "
                        f"failed={stats['failed']} blocked={stats['blocked']} "
                        f"llm_failed={stats['llm_failed']} elapsed={elapsed}s"
                    )
    except KeyboardInterrupt:
        aborted = True
        error_msg = "KeyboardInterrupt"
        stats["failed"] += 1
    except Exception:
        stats["failed"] += 1
        error_msg = traceback.format_exc()[:8000]
        raise
    finally:
        if not finished:
            if aborted:
                stats["aborted"] = True
            ok = stats["failed"] == 0 and stats["blocked"] == 0
            finish_ingest_run(sb, run_id, ok, stats, error=error_msg)
            finished = True

    return 0 if not aborted else 130


if __name__ == "__main__":
    raise SystemExit(main())

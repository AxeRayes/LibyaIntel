import hashlib
import json
import os
import random
import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

from backend.db import (
    finish_ingest_run,
    get_client,
    get_source_id,
    start_ingest_run,
    upsert_feed_item,
)
from backend.config import get_int


API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
CONFIG_PATH = Path(__file__).resolve().parents[1] / "ingest" / "gdelt_topics.json"
MAX_TOTAL = get_int("GDELT_MAX_TOTAL", 500) or 500
CONNECT_TIMEOUT = get_int("GDELT_CONNECT_TIMEOUT_SEC", 5) or 5
REQUEST_TIMEOUT = get_int("GDELT_TIMEOUT_SEC", 25) or 25
MAX_RETRIES = get_int("GDELT_MAX_RETRIES", 3) or 3
SLEEP_BASE = float(os.getenv("GDELT_SLEEP_BASE", "1.2"))
SLEEP_JITTER = float(os.getenv("GDELT_SLEEP_JITTER", "0.8"))
BACKOFF_CAP_SEC = int(os.getenv("GDELT_BACKOFF_CAP_SEC", "120")) or 120
RUN_LANG = os.getenv("GDELT_RUN_LANG", "all").strip().lower()
STOP_AFTER_CONSEC_429 = int(os.getenv("GDELT_STOP_AFTER_CONSEC_429", "2")) or 2
QUERY_OVERRIDE = os.getenv("GDELT_QUERY_OVERRIDE", "").strip()
QUERY_OVERRIDE_TOPIC = os.getenv("GDELT_QUERY_OVERRIDE_TOPIC", "").strip()
DUMP_BODY_PATH = os.getenv("GDELT_DUMP_BODY_PATH", "").strip()
DEBUG = os.getenv("GDELT_DEBUG", "0").strip() == "1"
_deny_raw = os.getenv("GDELT_DOMAIN_DENYLIST", "")
DOMAIN_DENYLIST = {
    d.strip().lower().lstrip(".") for d in _deny_raw.split(",") if d.strip()
}
_allow_raw = os.getenv("GDELT_DOMAIN_ALLOWLIST", "")
DOMAIN_ALLOWLIST = {
    d.strip().lower().lstrip(".") for d in _allow_raw.split(",") if d.strip()
}
_denylist_default_used = False
if not DOMAIN_DENYLIST:
    DOMAIN_DENYLIST = {
        "facebook.com",
        "twitter.com",
        "x.com",
        "youtube.com",
        "t.me",
        "telegram.me",
        "instagram.com",
    }
    _denylist_default_used = True

_DROP_PARAMS = {
    "fbclid",
    "gclid",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "mc_eid",
}

_TAG_KEYWORDS = {
    "tenders": [
        "tender",
        "procurement",
        "bid",
        "rfp",
        "rfq",
        "eoi",
        "prequalification",
        "expression of interest",
    ],
    "contracts": [
        "contract",
        "awarded",
        "award",
        "agreement",
        "mou",
        "memorandum",
        "signed",
        "deal",
    ],
    "oil_gas": [
        "noc",
        "oil",
        "gas",
        "pipeline",
        "refinery",
        "production",
        "concession",
        "crude",
        "field",
    ],
    "banking": [
        "central bank",
        "cbl",
        "dinar",
        "fx",
        "exchange rate",
        "reserves",
        "liquidity",
        "bank",
        "banking",
    ],
    "regulation": [
        "decree",
        "law",
        "regulation",
        "circular",
        "policy",
        "budget",
        "licensing",
        "customs",
        "tax",
        "tariff",
    ],
    "security": [
        "clashes",
        "blockade",
        "shutdown",
        "strike",
        "protest",
        "attack",
        "port closure",
        "pipeline shutdown",
    ],
    "power": [
        "power",
        "electricity",
        "grid",
        "generation",
        "blackout",
        "load shedding",
    ],
    "telecom": ["telecom", "mobile", "spectrum", "4g", "5g", "fiber", "regulator"],
    "projects": [
        "project",
        "investment",
        "infrastructure",
        "development",
        "approved",
        "planned",
    ],
    "governance": [
        "appointed",
        "dismissed",
        "cabinet",
        "minister",
        "governor",
        "government formation",
    ],
    "elections": [
        "election",
        "vote",
        "transition",
        "constitution",
        "referendum",
    ],
    "macro": ["macro", "sovereign", "fiscal", "budget", "debt"],
}


def _normalize_url(url: str) -> str:
    if not url:
        return url
    url = url.strip()
    parsed = urlparse(url)
    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or "").lower()
    path = parsed.path or ""
    query_items = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        if k.lower().startswith("utm_"):
            continue
        if k.lower() in _DROP_PARAMS:
            continue
        query_items.append((k, v))
    query = urlencode(query_items, doseq=True)
    rebuilt = urlunparse((scheme, netloc, path, "", query, ""))
    if rebuilt.endswith("/") and path != "/":
        rebuilt = rebuilt[:-1]
    return rebuilt


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _domain_from_url(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _keyword_match(tags: list[str], text: str) -> bool:
    if not tags or not text:
        return False
    text = text.lower()
    for tag in tags:
        for kw in _TAG_KEYWORDS.get(tag, []):
            if kw in text:
                return True
    return False


def _term_match(terms: list[str] | None, text: str) -> bool:
    if not terms or not text:
        return False
    text = text.lower()
    for term in terms:
        if term and term.lower() in text:
            return True
    return False


def _parse_seendate(val: str | None) -> str:
    if not val:
        return datetime.now(timezone.utc).isoformat()
    try:
        dt = datetime.strptime(val, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _load_topics() -> list[dict]:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _request_with_backoff(
    params: dict, remaining_backoff: list[int], topic_key: str
) -> tuple[dict | None, bool]:
    delay = 2
    saw_429 = False
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            fetch_start = time.monotonic()
            print(f"GDELT_FETCH_START topic={topic_key} url={API_URL}")
            resp = requests.get(
                API_URL, params=params, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT)
            )
            fetch_ms = int((time.monotonic() - fetch_start) * 1000)
            retry_after = resp.headers.get("retry-after")
            print(
                "GDELT_FETCH_DONE "
                f"topic={topic_key} status={resp.status_code} bytes={len(resp.content or b'')} "
                f"retry_after={retry_after or ''} elapsed_ms={fetch_ms}"
            )
            if resp.status_code == 200 and len(resp.content or b"") < 50:
                body = resp.text or ""
                snippet = body[:200].replace("\n", " ").replace("\r", " ")
                print(
                    "GDELT_TINY_BODY "
                    f"topic={topic_key} status=200 ct={resp.headers.get('content-type')} "
                    f"body=\"{snippet}\""
                )
                if DUMP_BODY_PATH:
                    try:
                        with open(DUMP_BODY_PATH, "w", encoding="utf-8") as f:
                            f.write(body)
                    except Exception as e:
                        print(
                            f"GDELT_DUMP_FAIL topic={topic_key} err={type(e).__name__} msg={str(e)[:200]}"
                        )
            if resp.status_code == 429:
                saw_429 = True
                retry_after = resp.headers.get("retry-after")
                if retry_after:
                    try:
                        ra = min(int(retry_after), 60)
                    except Exception:
                        ra = 15
                else:
                    ra = min(delay * 3, 60)
                if remaining_backoff[0] <= 0:
                    print(
                        f"GDELT_FAIL topic={topic_key} status=429 budget_exhausted=1 query={params.get('query')}"
                    )
                    return None, True
                sleep_for = min(ra, remaining_backoff[0])
                remaining_backoff[0] -= sleep_for
                print(f"GDELT_BACKOFF topic={topic_key} status=429 sleep={sleep_for}")
                time.sleep(sleep_for)
                delay = min(delay * 2, 30)
                continue
            if resp.status_code >= 500:
                raise RuntimeError(f"server_error:{resp.status_code}")
            if resp.status_code != 200:
                body = resp.text or ""
                snippet = body[:120].replace("\n", " ").replace("\r", " ")
                print(
                    f"GDELT_FAIL topic={topic_key} status={resp.status_code} ct={resp.headers.get('content-type')} body='{snippet}'"
                )
                return None, saw_429
            if "application/json" not in (resp.headers.get("content-type") or ""):
                body = resp.text or ""
                snippet = body[:120].replace("\n", " ").replace("\r", " ")
                print(
                    f"GDELT_FAIL topic={topic_key} status=200 ct={resp.headers.get('content-type')} body='{snippet}'"
                )
                return None, saw_429
            if not resp.text or not resp.text.strip():
                print(f"GDELT_FAIL topic={topic_key} status=200 empty_body=1")
                return None, saw_429
            return resp.json(), saw_429
        except Exception as e:
            print(
                f"GDELT_FETCH_ERROR topic={topic_key} err={type(e).__name__} msg={str(e)[:200]}"
            )
            if attempt == MAX_RETRIES:
                print(
                    f"GDELT_FAIL topic={topic_key} error={type(e).__name__} msg={str(e)[:200]}"
                )
                return None, saw_429
            time.sleep(delay)
            delay = min(delay * 2, 30)
    return None, saw_429


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--topics",
        help="Comma-separated topic keys to run",
        default="",
    )
    args = parser.parse_args()
    topics_filter = {t.strip() for t in args.topics.split(",") if t.strip()}

    sb = get_client()
    run_id = start_ingest_run(sb, "gdelt_ingest")
    run_start = time.monotonic()
    run_status = "ok"
    if _denylist_default_used:
        print(
            f"GDELT_DENYLIST default_used=1 domains={','.join(sorted(DOMAIN_DENYLIST))}"
        )
    elif DOMAIN_DENYLIST:
        print(
            f"GDELT_DENYLIST default_used=0 domains={','.join(sorted(DOMAIN_DENYLIST))}"
        )
    if DOMAIN_ALLOWLIST:
        print(f"GDELT_ALLOWLIST domains={','.join(sorted(DOMAIN_ALLOWLIST))}")
    stats = {
        "topics_attempted": 0,
        "items_found": 0,
        "items_processed": 0,
        "inserted_new": 0,
        "updated_existing": 0,
        "dedup_existing": 0,
        "skipped": 0,
        "skipped_duplicate": 0,
        "skipped_denylist": 0,
        "skipped_allowlist": 0,
        "skipped_not_relevant": 0,
        "skipped_missing_url": 0,
        "skipped_other": 0,
        "failed": 0,
        "by_topic": {},
    }
    topics = _load_topics()
    max_total = MAX_TOTAL
    seen_urls: set[str] = set()
    error_msg = None

    try:
        gdelt_source_id = get_source_id(sb, "gdelt")
    except Exception as e:
        error_msg = (
            "missing sources key='gdelt'; run "
            "migrations/20260204_gdelt_source.sql or deploy.sh migrations"
        )
        finish_ingest_run(sb, run_id, ok=False, stats=stats, error=error_msg)
        raise SystemExit(error_msg)

    topic_update_cache: dict[str, set[str]] = {}

    def _merge_topics_for_external_id(
        external_id: str, topic_key: str, tags: list[str], raw_url: str | None
    ) -> None:
        if not external_id:
            return
        cached = topic_update_cache.setdefault(external_id, set())
        if topic_key in cached:
            return
        cached.add(topic_key)
        try:
            res = (
                sb.table("feed_items")
                .select("raw")
                .eq("external_id", external_id)
                .limit(1)
                .execute()
            )
        except Exception:
            return
        if not res.data:
            return
        raw = res.data[0].get("raw") or {}
        if not isinstance(raw, dict):
            raw = {}
        gdelt = raw.get("gdelt") or {}
        if not isinstance(gdelt, dict):
            gdelt = {}
        topics_found = gdelt.get("topics_found") or []
        if not isinstance(topics_found, list):
            topics_found = []
        if topic_key not in topics_found:
            topics_found.append(topic_key)
        merged_tags = gdelt.get("tags") or []
        if not isinstance(merged_tags, list):
            merged_tags = []
        for tag in tags or []:
            if tag not in merged_tags:
                merged_tags.append(tag)
        raw_urls = gdelt.get("raw_urls") or []
        if not isinstance(raw_urls, list):
            raw_urls = []
        if raw_url and raw_url not in raw_urls:
            raw_urls.append(raw_url)
        gdelt["topics_found"] = topics_found
        gdelt["tags"] = merged_tags
        gdelt["raw_urls"] = raw_urls
        raw["gdelt"] = gdelt
        try:
            sb.table("feed_items").update({"raw": raw}).eq(
                "external_id", external_id
            ).execute()
        except Exception:
            return

    try:
        remaining_backoff = [BACKOFF_CAP_SEC]
        topics = sorted(
            topics, key=lambda t: int(t.get("priority", 50)) if t else 50
        )
        disabled_keys = []
        for t in topics:
            if not t.get("enabled", True):
                key = t.get("key") or "unknown"
                disabled_keys.append(key)
        if disabled_keys:
            print(
                "GDELT_TOPICS "
                f"disabled_keys={','.join(sorted(disabled_keys))} "
                "reason=disabled_in_config"
            )
        if topics_filter:
            topics = [t for t in topics if t.get("key") in topics_filter]
            if not topics:
                error_msg = f"no_topics_matched filter={','.join(sorted(topics_filter))}"
                finish_ingest_run(sb, run_id, ok=False, stats=stats, error=error_msg)
                raise SystemExit(error_msg)
            print(
                f"GDELT_TOPICS filtered={len(topics)} keys={','.join(sorted(topics_filter))}"
            )
        if RUN_LANG in {"en", "ar"}:
            topics = [t for t in topics if (t.get("lang") or "").lower() == RUN_LANG]
            if not topics:
                error_msg = f"no_topics_matched lang={RUN_LANG}"
                finish_ingest_run(sb, run_id, ok=False, stats=stats, error=error_msg)
                raise SystemExit(error_msg)
        print(
            "GDELT_RUN_START "
            f"lang={RUN_LANG} topics={len(topics)} max_total={max_total} "
            f"connect_timeout={CONNECT_TIMEOUT} read_timeout={REQUEST_TIMEOUT}"
        )
        consecutive_429 = 0
        for topic in topics:
            try:
                if not topic.get("enabled", True):
                    continue
                topic_key = topic.get("key") or "unknown"
                topic_stats = stats["by_topic"].setdefault(
                    topic_key,
                    {
                        "found": 0,
                        "processed": 0,
                        "inserted_new": 0,
                        "updated_existing": 0,
                        "dedup_existing": 0,
                        "failed": 0,
                        "skipped": 0,
                        "skipped_duplicate": 0,
                        "skipped_denylist": 0,
                        "skipped_allowlist": 0,
                        "skipped_not_relevant": 0,
                        "skipped_missing_url": 0,
                        "skipped_other": 0,
                    },
                )
                stats["topics_attempted"] += 1
                if max_total <= 0:
                    break

                query = topic.get("query") or ""
                if QUERY_OVERRIDE and (
                    not QUERY_OVERRIDE_TOPIC or QUERY_OVERRIDE_TOPIC == topic_key
                ):
                    print(f"GDELT_QUERY_OVERRIDE topic={topic_key}")
                    query = QUERY_OVERRIDE
                if not query:
                    continue

                params = {
                    "query": query,
                    "mode": "artlist",
                    "format": "json",
                    "maxrecords": int(topic.get("maxrecords") or 100),
                    "timespan": topic.get("timespan") or "24h",
                    "sort": "datedesc",
                }
                data, saw_429 = _request_with_backoff(
                    params, remaining_backoff, topic_key
                )
                if not data:
                    topic_stats["failed"] += 1
                    stats["failed"] += 1
                    if saw_429:
                        consecutive_429 += 1
                        if consecutive_429 >= STOP_AFTER_CONSEC_429:
                            print(
                                f"GDELT_STOP reason=consecutive_429 count={consecutive_429}"
                            )
                            break
                    continue
                consecutive_429 = 0

                articles = data.get("articles") or []
                topic_stats["found"] = len(articles)
                stats["items_found"] += len(articles)
                if not articles:
                    print(f"GDELT_EMPTY topic={topic_key} status=200")
                remaining_budget = max_total - stats["items_processed"]
                if remaining_budget <= 0:
                    print(
                        f"GDELT_TOPIC_SUMMARY topic={topic_key} found={topic_stats['found']} "
                        "processed=0 inserted_new=0 updated_existing=0 dedup_existing=0 "
                        "skipped=0 failed=0 note=budget_exhausted"
                    )
                    continue
                max_per_topic = int(topic.get("max_processed_per_topic") or 0)
                if max_per_topic > 0:
                    remaining_budget = min(remaining_budget, max_per_topic)
                docs_to_process = articles[:remaining_budget]
                skip_sampled = 0
                accepted_sampled = 0
                for item in docs_to_process:
                    url = item.get("url")
                    if not url:
                        topic_stats["skipped"] += 1
                        topic_stats["skipped_missing_url"] += 1
                        stats["skipped"] += 1
                        stats["skipped_missing_url"] += 1
                        if DEBUG and skip_sampled < 3:
                            skip_sampled += 1
                            print(
                                "GDELT_SKIP_SAMPLE "
                                f"topic={topic_key} reason=missing_url"
                            )
                        continue
                    topic_stats["processed"] += 1
                    stats["items_processed"] += 1
                    norm = _normalize_url(url)
                    domain = _domain_from_url(norm)
                    if DOMAIN_ALLOWLIST and domain and domain not in DOMAIN_ALLOWLIST:
                        topic_stats["skipped"] += 1
                        topic_stats["skipped_allowlist"] += 1
                        stats["skipped"] += 1
                        stats["skipped_allowlist"] += 1
                        if DEBUG and skip_sampled < 3:
                            skip_sampled += 1
                            print(
                                "GDELT_SKIP_SAMPLE "
                                f"topic={topic_key} reason=allowlist url={norm}"
                            )
                        continue
                    if domain and domain in DOMAIN_DENYLIST:
                        topic_stats["skipped"] += 1
                        topic_stats["skipped_denylist"] += 1
                        stats["skipped"] += 1
                        stats["skipped_denylist"] += 1
                        if DEBUG and skip_sampled < 3:
                            skip_sampled += 1
                            print(
                                "GDELT_SKIP_SAMPLE "
                                f"topic={topic_key} reason=denylist url={norm}"
                            )
                        continue
                    title = (item.get("title") or "").strip()
                    snippet = (item.get("snippet") or "").strip()
                    text_blob = f"{title} {snippet}".strip()
                    location_terms = topic.get("location_terms") or [
                        "libya",
                        "libyan",
                        "tripoli",
                        "benghazi",
                        "misrata",
                    ]
                    require_terms = topic.get("require_terms") or []
                    require_terms_any = topic.get("require_terms_any") or []
                    require_terms_any2 = topic.get("require_terms_any2") or []
                    preferred_domains = topic.get("preferred_domains") or []
                    preferred_hit = domain in preferred_domains
                    if not _term_match(location_terms, text_blob):
                        topic_stats["skipped"] += 1
                        topic_stats["skipped_not_relevant"] += 1
                        stats["skipped"] += 1
                        stats["skipped_not_relevant"] += 1
                        if DEBUG and skip_sampled < 3:
                            skip_sampled += 1
                            print(
                                "GDELT_SKIP_SAMPLE "
                                f"topic={topic_key} reason=not_relevant url={norm}"
                            )
                        continue
                    if require_terms_any and require_terms_any2:
                        if not (
                            _term_match(require_terms_any, text_blob)
                            and _term_match(require_terms_any2, text_blob)
                        ):
                            if not preferred_hit:
                                topic_stats["skipped"] += 1
                                topic_stats["skipped_not_relevant"] += 1
                                stats["skipped"] += 1
                                stats["skipped_not_relevant"] += 1
                                if DEBUG and skip_sampled < 3:
                                    skip_sampled += 1
                                    print(
                                        "GDELT_SKIP_SAMPLE "
                                        f"topic={topic_key} reason=not_relevant url={norm}"
                                    )
                                continue
                    elif require_terms and not _term_match(require_terms, text_blob):
                        if not preferred_hit:
                            topic_stats["skipped"] += 1
                            topic_stats["skipped_not_relevant"] += 1
                            stats["skipped"] += 1
                            stats["skipped_not_relevant"] += 1
                            if DEBUG and skip_sampled < 3:
                                skip_sampled += 1
                                print(
                                    "GDELT_SKIP_SAMPLE "
                                    f"topic={topic_key} reason=not_relevant url={norm}"
                                )
                            continue
                    elif not _keyword_match(topic.get("tags") or [], text_blob):
                        topic_stats["skipped"] += 1
                        topic_stats["skipped_not_relevant"] += 1
                        stats["skipped"] += 1
                        stats["skipped_not_relevant"] += 1
                        if DEBUG and skip_sampled < 3:
                            skip_sampled += 1
                            print(
                                "GDELT_SKIP_SAMPLE "
                                f"topic={topic_key} reason=not_relevant url={norm}"
                            )
                        continue
                    if not norm or norm in seen_urls:
                        if norm:
                            _merge_topics_for_external_id(
                                _sha1(norm), topic_key, topic.get("tags") or [], url
                            )
                        topic_stats["dedup_existing"] += 1
                        stats["dedup_existing"] += 1
                        topic_stats["skipped"] += 1
                        topic_stats["skipped_duplicate"] += 1
                        stats["skipped"] += 1
                        stats["skipped_duplicate"] += 1
                        if DEBUG and skip_sampled < 3:
                            skip_sampled += 1
                            print(
                                "GDELT_SKIP_SAMPLE "
                                f"topic={topic_key} reason=duplicate url={norm}"
                            )
                        continue
                    seen_urls.add(norm)
                    external_id = _sha1(norm)

                    try:
                        exists = (
                            sb.table("feed_items")
                            .select("id")
                            .eq("external_id", external_id)
                            .limit(1)
                            .execute()
                        )
                        if exists.data:
                            _merge_topics_for_external_id(
                                external_id, topic_key, topic.get("tags") or [], url
                            )
                            topic_stats["updated_existing"] += 1
                            stats["updated_existing"] += 1
                            topic_stats["skipped"] += 1
                            topic_stats["skipped_duplicate"] += 1
                            stats["skipped"] += 1
                            stats["skipped_duplicate"] += 1
                            if DEBUG and skip_sampled < 3:
                                skip_sampled += 1
                                print(
                                    "GDELT_SKIP_SAMPLE "
                                    f"topic={topic_key} reason=duplicate url={norm}"
                                )
                            continue
                    except Exception:
                        pass

                    published_at = _parse_seendate(item.get("seendate"))
                    lang = topic.get("lang") or item.get("language") or "unknown"
                    tags = topic.get("tags") or []
                    raw = {
                        "gdelt": {
                            "topic_key": topic_key,
                            "topics_found": [topic_key],
                            "tags": tags,
                            "raw_url": url,
                            "normalized_url": norm,
                            "raw_urls": [url],
                            "dedupe_key": external_id,
                            "seendate": item.get("seendate"),
                            "domain": domain or item.get("domain"),
                            "language": item.get("language"),
                            "sourcecountry": item.get("sourcecountry"),
                        }
                    }

                    feed_item = {
                        "source_id": gdelt_source_id,
                        "source_type": "article",
                        "external_id": external_id,
                        "url": norm,
                        "title": item.get("title") or "",
                        "summary": "",
                        "content": "",
                        "language": lang,
                        "published_at": published_at,
                        "raw": raw,
                    }

                    try:
                        upsert_feed_item(sb, feed_item)
                        topic_stats["inserted_new"] += 1
                        stats["inserted_new"] += 1
                        if DEBUG and accepted_sampled < 5:
                            accepted_sampled += 1
                            print(
                                "GDELT_ACCEPT_SAMPLE "
                                f"topic={topic_key} url={norm}"
                            )
                    except Exception as e:
                        topic_stats["failed"] += 1
                        stats["failed"] += 1
                        print(
                            f"GDELT_ITEM_FAIL topic={topic_key} err={type(e).__name__} msg={str(e)[:200]}"
                        )
                    if stats["items_processed"] >= max_total:
                        break

                print(
                    "GDELT_TOPIC_SUMMARY "
                    f"topic={topic_key} "
                    f"found={topic_stats['found']} "
                    f"processed={topic_stats['processed']} "
                    f"inserted_new={topic_stats['inserted_new']} "
                    f"updated_existing={topic_stats['updated_existing']} "
                    f"dedup_existing={topic_stats['dedup_existing']} "
                    f"skipped={topic_stats['skipped']} "
                    f"failed={topic_stats['failed']}"
                )
                print(
                    "GDELT_TOPIC_SKIPS "
                    f"topic={topic_key} "
                    f"duplicate={topic_stats['skipped_duplicate']} "
                    f"deny={topic_stats['skipped_denylist']} "
                    f"allow={topic_stats['skipped_allowlist']} "
                    f"not_relevant={topic_stats['skipped_not_relevant']} "
                    f"missing={topic_stats['skipped_missing_url']} "
                    f"other={topic_stats['skipped_other']}"
                )
                if stats["items_processed"] >= max_total:
                    print("GDELT_RUN_STOP reason=budget_exhausted")
                    break
                sleep_override = topic.get("sleep_override_sec")
                if sleep_override is None:
                    sleep_for = SLEEP_BASE + random.uniform(0, SLEEP_JITTER)
                else:
                    sleep_for = float(sleep_override)
                time.sleep(sleep_for)
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)[:200]}"
                run_status = "fail"
                print(f"GDELT_TOPIC_FAIL topic={topic_key} err={error_msg}")
                break
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)[:200]}"
        run_status = "fail"
        print(f"GDELT_RUN_FAIL err={error_msg}", file=sys.stderr)
    finally:
        elapsed_ms = int((time.monotonic() - run_start) * 1000)
        print(
            "GDELT_RUN_SKIPS "
            f"duplicate={stats['skipped_duplicate']} "
            f"deny={stats['skipped_denylist']} "
            f"allow={stats['skipped_allowlist']} "
            f"not_relevant={stats['skipped_not_relevant']} "
            f"missing={stats['skipped_missing_url']} "
            f"other={stats['skipped_other']}"
        )
        print(f"GDELT_RUN_END status={run_status} elapsed_ms={elapsed_ms}")

    finish_ingest_run(sb, run_id, ok=error_msg is None, stats=stats, error=error_msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

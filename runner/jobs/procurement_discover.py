import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import feedparser
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from backend.config import get_int
from backend.db import (
    finish_ingest_run,
    get_client,
    get_source_id,
    start_ingest_run,
    upsert_feed_item,
)


CONFIG_PATH = Path(__file__).resolve().parents[1] / "ingest" / "procurement_sources.json"
REQUEST_TIMEOUT = get_int("PROCUREMENT_TIMEOUT_SEC", 20) or 20
CONNECT_TIMEOUT = get_int("PROCUREMENT_CONNECT_TIMEOUT_SEC", 5) or 5
MAX_TOTAL = get_int("PROCUREMENT_MAX_TOTAL", 200) or 200
DEBUG = get_int("PROCUREMENT_DEBUG", 0) or 0

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


def _normalize_url(url: str, extra_drop: list[str] | None = None) -> str:
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
        if extra_drop and k.lower() in extra_drop:
            continue
        query_items.append((k, v))
    query = urlencode(query_items, doseq=True)
    rebuilt = urlunparse((scheme, netloc, path, "", query, ""))
    if rebuilt.endswith("/") and path != "/":
        rebuilt = rebuilt[:-1]
    return rebuilt


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _load_sources() -> list[dict]:
    if not CONFIG_PATH.exists():
        return []
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_datetime(val: str | None) -> str:
    if not val:
        return datetime.now(timezone.utc).isoformat()
    try:
        return datetime.fromisoformat(val).astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _fetch_html(
    url: str,
    headers: dict | None = None,
    cookies: dict | None = None,
    timeout_sec: int | None = None,
) -> str:
    timeout_sec = timeout_sec or REQUEST_TIMEOUT
    session = requests.Session()
    resp = session.get(
        url,
        headers=headers or {},
        cookies=cookies or {},
        timeout=(CONNECT_TIMEOUT, timeout_sec),
        allow_redirects=True,
    )
    resp.raise_for_status()
    return resp.text or ""


def _extract_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    links: list[str] = []
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("javascript:"):
            continue
        links.append(href)
    # Resolve relative URLs
    base = urlparse(base_url)
    resolved = []
    for href in links:
        parsed = urlparse(href)
        if parsed.scheme and parsed.netloc:
            resolved.append(href)
        else:
            resolved.append(urlunparse((base.scheme, base.netloc, href, "", "", "")))
    return resolved


def _extract_doc_links_with_text(html: str, base_url: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    out: list[tuple[str, str]] = []
    base = urlparse(base_url)
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if not href:
            continue
        low = href.lower()
        if not (".pdf" in low or ".doc" in low or ".docx" in low):
            continue
        parsed = urlparse(href)
        if parsed.scheme and parsed.netloc:
            full = href
        else:
            full = urlunparse((base.scheme, base.netloc, href, "", "", ""))
        text = " ".join(a.get_text(" ", strip=True).split())
        out.append((full, text))
    return out


def _probe_doc_links(pages: list[str]) -> None:
    all_links: list[tuple[str, str, str]] = []
    for page_url in pages:
        try:
            html = _fetch_html(page_url)
        except Exception:
            continue
        for href, text in _extract_doc_links_with_text(html, page_url):
            all_links.append((page_url, href, text))
            if len(all_links) >= 30:
                break
        if len(all_links) >= 30:
            break

    print(f"SIRTE_DOC_SUMMARY total_doc_links_found={len(all_links)}")
    prefixes: dict[str, int] = {}
    for _, href, _ in all_links:
        path = urlparse(href).path or ""
        parts = path.split("/")
        prefix = "/".join(parts[:4])
        prefixes[prefix] = prefixes.get(prefix, 0) + 1
    for prefix, count in sorted(prefixes.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"SIRTE_DOC_PREFIX count={count} prefix={prefix}")

    for page, href, text in all_links[:30]:
        safe_text = (text or "")[:120]
        print(f"SIRTE_DOC_CANDIDATE page={page} href={href} text=\"{safe_text}\"")


def _filter_links(
    links: list[str],
    allow_prefixes: list[str] | None,
    deny_prefixes: list[str] | None,
    drop_params: list[str] | None,
) -> list[str]:
    out: list[str] = []
    for link in links:
        norm = _normalize_url(link, drop_params)
        if not norm:
            continue
        if deny_prefixes and any(norm.startswith(p) for p in deny_prefixes):
            continue
        if allow_prefixes and not any(norm.startswith(p) for p in allow_prefixes):
            continue
        out.append(norm)
    return out


def _filter_must_contain_any_text(
    text: str,
    must_contain_any: list[str] | None,
) -> bool:
    if not must_contain_any:
        return True
    low = (text or "").lower()
    return any(tok in low for tok in must_contain_any)


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    text = soup.get_text(" ", strip=True)
    return " ".join(text.split())


def _filter_pdf_links(
    links: list[str],
    allow_contains: list[str] | None,
    deny_contains: list[str] | None,
) -> list[str]:
    if not allow_contains and not deny_contains:
        return links
    out: list[str] = []
    for link in links:
        low = (link or "").lower()
        is_pdf = ".pdf" in low
        if not is_pdf:
            out.append(link)
            continue
        if deny_contains and any(tok in low for tok in deny_contains):
            continue
        if allow_contains and not any(tok in low for tok in allow_contains):
            continue
        out.append(link)
    return out


def _filter_contains(
    links: list[str],
    allow_contains: list[str] | None,
    deny_contains: list[str] | None,
) -> list[str]:
    if not allow_contains and not deny_contains:
        return links
    out: list[str] = []
    for link in links:
        low = (link or "").lower()
        if deny_contains and any(tok in low for tok in deny_contains):
            continue
        if allow_contains and not any(tok in low for tok in allow_contains):
            continue
        out.append(link)
    return out


def _filter_regex(
    links: list[str],
    allow_regex: list[str] | None,
    deny_regex: list[str] | None,
    source_key: str | None = None,
) -> list[str]:
    if not allow_regex and not deny_regex:
        return links
    allow_compiled = [re.compile(p) for p in (allow_regex or [])]
    deny_compiled = [re.compile(p) for p in (deny_regex or [])]
    out: list[str] = []
    for link in links:
        if any(r.search(link or "") for r in deny_compiled):
            if DEBUG:
                print(
                    f"PROCUREMENT_DEBUG_DENY source={source_key or 'unknown'} url={link}"
                )
            continue
        if allow_compiled and not any(r.search(link or "") for r in allow_compiled):
            continue
        out.append(link)
    return out


def _is_cf_challenge(html: str) -> bool:
    markers = [
        "just a moment",
        "cf-browser-verification",
        "challenge-platform",
        "cloudflare",
    ]
    low = (html or "").lower()
    return any(m in low for m in markers)


def _playwright_fetch_links(base_url: str, cfg: dict) -> tuple[list[str], bool]:
    max_pages = int(cfg.get("max_pages") or 1)
    page_param = cfg.get("page_param") or ""
    start_page = int(cfg.get("start_page") or 1)
    max_links = int(cfg.get("max_links") or 40)
    wait_until = cfg.get("wait_until") or "domcontentloaded"
    timeout_ms = int(cfg.get("timeout_ms") or 30000)
    headless = bool(cfg.get("headless", True))
    links: list[str] = []
    blocked = False

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = context.new_page()
        for i in range(max_pages):
            page_index = start_page + i
            url = base_url
            if page_param:
                url = base_url.rstrip("/") + "/" + (page_param % page_index)
            print(f"PROCUREMENT_PW_START source=noc_tenders url={url}")
            try:
                page.goto(url, wait_until=wait_until, timeout=timeout_ms)
                html = page.content() or ""
                if _is_cf_challenge(html):
                    blocked = True
                    print("PROCUREMENT_PW_BLOCKED source=noc_tenders reason=cf_challenge")
                    break
                anchors = page.query_selector_all('a[href*="/en/tenders/"]')
                for a in anchors:
                    href = a.get_attribute("href") or ""
                    if not href:
                        continue
                    links.append(href)
                # jittered sleep
                time.sleep(2 + (i % 2))
            except Exception as e:
                print(
                    f"PROCUREMENT_PW_FAIL source=noc_tenders err={type(e).__name__} msg={str(e)[:200]}"
                )
                break
        page.close()
        context.close()
        browser.close()

    # normalize and cap
    normed = []
    for link in links:
        normed.append(_normalize_url(link))
        if len(normed) >= max_links:
            break
    return normed, blocked


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sources",
        help="Comma-separated source keys to run",
        default="",
    )
    parser.add_argument(
        "--probe-candidates",
        action="store_true",
        help="Probe candidate sources (candidate=true) without inserting",
    )
    parser.add_argument(
        "--max-enable",
        type=int,
        default=0,
        help="Show top N candidates by matched link count",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logs for fetch status and anchor samples",
    )
    args = parser.parse_args()
    only_keys = {k.strip() for k in args.sources.split(",") if k.strip()}
    if args.debug:
        global DEBUG
        DEBUG = 1

    sb = get_client()
    run_id = start_ingest_run(sb, "procurement_discover")
    stats = {"total": 0, "inserted": 0, "deduped": 0, "failed": 0, "by_source": {}}
    error_msg = None

    try:
        source_id = get_source_id(sb, "procurement")
    except Exception:
        error_msg = (
            "missing sources key='procurement'; run "
            "migrations/20260204_procurement_source.sql or deploy.sh migrations"
        )
        finish_ingest_run(sb, run_id, ok=False, stats=stats, error=error_msg)
        raise SystemExit(error_msg)

    sources = _load_sources()
    if not sources:
        print("PROCUREMENT_SOURCES empty=1")
        finish_ingest_run(sb, run_id, ok=True, stats=stats, error=None)
        return 0
    disabled_keys = [
        s.get("key") for s in sources if not s.get("enabled", True) and s.get("key")
    ]
    if disabled_keys:
        print(f"PROCUREMENT_SOURCES_DISABLED keys={','.join(disabled_keys)}")

    if args.probe_candidates:
        candidates = [s for s in sources if s.get("candidate") and s.get("key")]
        results: list[tuple[str, int]] = []
        for src in candidates:
            key = src.get("key")
            url = src.get("url")
            if not url:
                continue
            try:
                resp = requests.get(
                    url,
                    headers=src.get("headers") or {},
                    cookies=src.get("cookies") or {},
                    timeout=(CONNECT_TIMEOUT, src.get("timeout_sec") or REQUEST_TIMEOUT),
                    allow_redirects=True,
                )
                status = resp.status_code
                ct = resp.headers.get("content-type", "")
                html = resp.text or ""
                blocked = _is_cf_challenge(html)
                if blocked:
                    print(
                        f"PROCUREMENT_PROBE source={key} status={status} blocked=cf_challenge anchors=0 matched=0"
                    )
                    continue
                links = _extract_links(html, url)
                allow_prefixes = src.get("allow_prefixes")
                deny_prefixes = src.get("deny_prefixes")
                drop_params = [s.lower() for s in (src.get("drop_query_params") or [])]
                allow_contains = [
                    s.lower() for s in (src.get("allow_url_contains") or [])
                ]
                deny_contains = [s.lower() for s in (src.get("deny_url_contains") or [])]
                allow_regex = src.get("allow_url_regex") or []
                deny_regex = src.get("deny_url_regex") or []
                must_contain_any = [
                    s.lower() for s in (src.get("must_contain_any") or [])
                ]
                filter_stage = src.get("filter_stage") or ""
                allow_pdf = [s.lower() for s in (src.get("pdf_allow_contains") or [])]
                deny_pdf = [s.lower() for s in (src.get("pdf_deny_contains") or [])]
                filtered = _filter_links(
                    links, allow_prefixes, deny_prefixes, drop_params
                )
                filtered = _filter_contains(filtered, allow_contains, deny_contains)
                filtered = _filter_regex(filtered, allow_regex, deny_regex, key)
                filtered = _filter_must_contain_any(filtered, must_contain_any)
                filtered = _filter_pdf_links(filtered, allow_pdf, deny_pdf)
                matched = len(filtered)
                print(
                    f"PROCUREMENT_PROBE source={key} status={status} anchors={len(links)} matched={matched}"
                )
                for sample in filtered[:5]:
                    print(f"PROCUREMENT_PROBE_SAMPLE source={key} url={sample}")
                results.append((key, matched))
            except Exception as e:
                print(
                    f"PROCUREMENT_PROBE source={key} err={type(e).__name__} msg={str(e)[:200]}"
                )
        if args.max_enable:
            top = sorted(results, key=lambda x: x[1], reverse=True)[: args.max_enable]
            top_keys = ",".join([k for k, _ in top])
            print(f"PROCUREMENT_PROBE_TOP keys={top_keys}")
        finish_ingest_run(sb, run_id, ok=True, stats=stats, error=None)
        return 0

    seen: set[str] = set()
    budget = MAX_TOTAL

    for src in sources:
        if not src.get("enabled", True):
            continue
        if DEBUG and src.get("key") == "sirte_oil_docs":
            base = src.get("url") or "https://www.sirteoil.com.ly/"
            probe_pages = [
                base,
                base.rstrip("/") + "/en/",
                base.rstrip("/") + "/news/",
                base.rstrip("/") + "/en/news/",
                base.rstrip("/") + "/tenders/",
                base.rstrip("/") + "/en/tenders/",
                base.rstrip("/") + "/announcements/",
                base.rstrip("/") + "/en/announcements/",
                base.rstrip("/") + "/media/",
                base.rstrip("/") + "/downloads/",
                base.rstrip("/") + "/wp-sitemap.xml",
            ]
            _probe_doc_links(probe_pages)
        if only_keys and (src.get("key") not in only_keys):
            continue
        if budget <= 0:
            break

        key = src.get("key") or "unknown"
        stats["by_source"].setdefault(
            key, {"found": 0, "inserted": 0, "deduped": 0, "failed": 0}
        )
        sstats = stats["by_source"][key]
        stype = src.get("type")
        url = src.get("url")
        max_inserts = int(src.get("max_inserts_per_run") or 0)
        max_items = int(src.get("max_items_per_run") or 0)
        inserted_this_source = 0
        allow_pdf = [s.lower() for s in (src.get("pdf_allow_contains") or [])]
        deny_pdf = [s.lower() for s in (src.get("pdf_deny_contains") or [])]
        if not url:
            continue

        try:
            if stype == "rss":
                feed = feedparser.parse(url)
                entries = feed.entries or []
                sstats["found"] = len(entries)
                for entry in entries:
                    if budget <= 0:
                        break
                    link = entry.get("link")
                    if not link:
                        continue
                    drop_params = [s.lower() for s in (src.get("drop_query_params") or [])]
                    norm = _normalize_url(link, drop_params)
                    if not norm or norm in seen:
                        sstats["deduped"] += 1
                        stats["deduped"] += 1
                        continue
                    seen.add(norm)
                    if max_inserts and inserted_this_source >= max_inserts:
                        print(
                            f"PROCUREMENT_SOURCE_CAP source={key} max_inserts={max_inserts}"
                        )
                        break
                    external_id = _sha1(norm)
                    published_at = _parse_datetime(entry.get("published") or entry.get("updated"))
                    raw = {
                        "procurement": {
                            "source_key": key,
                            "source_name": src.get("name"),
                            "tags": src.get("tags") or ["tenders", "procurement"],
                            "doc_type": "html",
                        }
                    }
                    item = {
                        "source_id": source_id,
                        "source_type": "article",
                        "external_id": external_id,
                        "url": norm,
                        "title": entry.get("title") or "",
                        "summary": "",
                        "content": "",
                        "language": src.get("language") or "mixed",
                        "published_at": published_at,
                        "raw": raw,
                    }
                    upsert_feed_item(sb, item)
                    sstats["inserted"] += 1
                    stats["inserted"] += 1
                    stats["total"] += 1
                    budget -= 1
                    inserted_this_source += 1
            elif stype in {"listing_page", "sitemap"}:
                headers = src.get("headers") or {}
                cookies = src.get("cookies") or {}
                timeout_sec = src.get("timeout_sec")
                links = []
                blocked_cf = False
                filter_stage = src.get("filter_stage") or ""
                if src.get("browser_mode") == "playwright":
                    links, blocked_cf = _playwright_fetch_links(
                        url, src.get("playwright") or {}
                    )
                else:
                    if DEBUG:
                        session = requests.Session()
                        resp = session.get(
                            url,
                            headers=headers or {},
                            cookies=cookies or {},
                            timeout=(CONNECT_TIMEOUT, timeout_sec or REQUEST_TIMEOUT),
                            allow_redirects=True,
                        )
                        status = resp.status_code
                        ct = resp.headers.get("content-type", "")
                        html = resp.text or ""
                        resp.raise_for_status()
                    else:
                        html = _fetch_html(
                            url, headers=headers, cookies=cookies, timeout_sec=timeout_sec
                        )
                    links = _extract_links(html, url)
                    if DEBUG:
                        sample = ", ".join(links[:5])
                        print(
                            f"PROCUREMENT_DEBUG source={key} status={status} ct={ct} "
                            f"html_len={len(html)} anchors={len(links)} sample={sample[:400]}"
                        )
                if blocked_cf:
                    sstats["failed"] += 1
                    stats["failed"] += 1
                    print("PROCUREMENT_PW_BLOCKED source=noc_tenders reason=cf_challenge")
                    continue
                allow_prefixes = src.get("allow_prefixes")
                deny_prefixes = src.get("deny_prefixes")
                drop_params = [s.lower() for s in (src.get("drop_query_params") or [])]
                allow_contains = [s.lower() for s in (src.get("allow_url_contains") or [])]
                deny_contains = [s.lower() for s in (src.get("deny_url_contains") or [])]
                allow_regex = src.get("allow_url_regex") or []
                deny_regex = src.get("deny_url_regex") or []
                must_contain_any = [
                    s.lower() for s in (src.get("must_contain_any") or [])
                ]
                filtered = _filter_links(links, allow_prefixes, deny_prefixes, drop_params)
                filtered = _filter_contains(filtered, allow_contains, deny_contains)
                filtered = _filter_regex(filtered, allow_regex, deny_regex, key)
                if must_contain_any and filter_stage != "detail":
                    filtered = [
                        link
                        for link in filtered
                        if _filter_must_contain_any_text(link, must_contain_any)
                    ]
                filtered = _filter_pdf_links(filtered, allow_pdf, deny_pdf)
                sstats["found"] = len(filtered)
                for idx, link in enumerate(filtered):
                    if max_items and idx >= max_items:
                        print(
                            f"PROCUREMENT_SOURCE_MAX_ITEMS source={key} max_items={max_items}"
                        )
                        break
                    if budget <= 0:
                        break
                    if link in seen:
                        sstats["deduped"] += 1
                        stats["deduped"] += 1
                        continue
                    seen.add(link)
                    if max_inserts and inserted_this_source >= max_inserts:
                        print(
                            f"PROCUREMENT_SOURCE_CAP source={key} max_inserts={max_inserts}"
                        )
                        break
                    if must_contain_any and filter_stage == "detail":
                        try:
                            detail_html = _fetch_html(
                                link,
                                headers=headers,
                                cookies=cookies,
                                timeout_sec=timeout_sec,
                            )
                        except Exception:
                            sstats["failed"] += 1
                            stats["failed"] += 1
                            continue
                        detail_text = _extract_text(detail_html)
                        if not _filter_must_contain_any_text(
                            detail_text, must_contain_any
                        ):
                            sstats["deduped"] += 1
                            stats["deduped"] += 1
                            continue
                    external_id = _sha1(link)
                    raw = {
                        "procurement": {
                            "source_key": key,
                            "source_name": src.get("name"),
                            "tags": src.get("tags") or ["tenders", "procurement"],
                            "doc_type": "html",
                        }
                    }
                    item = {
                        "source_id": source_id,
                        "source_type": "article",
                        "external_id": external_id,
                        "url": link,
                        "title": "",
                        "summary": "",
                        "content": "",
                        "language": src.get("language") or "mixed",
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "raw": raw,
                    }
                    upsert_feed_item(sb, item)
                    sstats["inserted"] += 1
                    stats["inserted"] += 1
                    stats["total"] += 1
                    budget -= 1
                    inserted_this_source += 1
            elif stype == "pdf_listing":
                drop_params = [s.lower() for s in (src.get("drop_query_params") or [])]
                norm = _normalize_url(url, drop_params)
                if norm and norm not in seen:
                    seen.add(norm)
                    if max_inserts and inserted_this_source >= max_inserts:
                        print(
                            f"PROCUREMENT_SOURCE_CAP source={key} max_inserts={max_inserts}"
                        )
                        continue
                    external_id = _sha1(norm)
                    raw = {
                        "procurement": {
                            "source_key": key,
                            "source_name": src.get("name"),
                            "tags": src.get("tags") or ["tenders", "procurement"],
                            "doc_type": "pdf",
                        }
                    }
                    item = {
                        "source_id": source_id,
                        "source_type": "document",
                        "external_id": external_id,
                        "url": norm,
                        "title": "",
                        "summary": "",
                        "content": "",
                        "language": src.get("language") or "mixed",
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "raw": raw,
                    }
                    upsert_feed_item(sb, item)
                    sstats["inserted"] += 1
                    stats["inserted"] += 1
                    stats["total"] += 1
                    budget -= 1
                    inserted_this_source += 1
            else:
                continue
            fetcher = "playwright" if src.get("browser_mode") == "playwright" else "http"
            print(
                f"PROCUREMENT_OK source={key} fetcher={fetcher} "
                f"found={sstats['found']} inserted={sstats['inserted']} deduped={sstats['deduped']}"
            )
        except Exception as e:
            sstats["failed"] += 1
            stats["failed"] += 1
            if isinstance(e, requests.HTTPError) and e.response is not None:
                resp = e.response
                server = resp.headers.get("server", "")
                cf_ray = resp.headers.get("cf-ray", "")
                ct = resp.headers.get("content-type", "")
                location = resp.headers.get("location", "")
                body = (resp.text or "")[:200].replace("\n", " ").replace("\r", " ")
                print(
                    "PROCUREMENT_FAIL "
                    f"source={key} err=HTTPError status={resp.status_code} "
                    f"server={server} cf_ray={cf_ray} ct={ct} location={location} body='{body}'"
                )
            else:
                print(
                    f"PROCUREMENT_FAIL source={key} err={type(e).__name__} msg={str(e)[:200]}"
                )
        time.sleep(0.2)

    finish_ingest_run(sb, run_id, ok=error_msg is None, stats=stats, error=error_msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

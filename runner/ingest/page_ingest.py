import fcntl
import hashlib
import json
import os
import random
import re
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, urldefrag

from bs4 import BeautifulSoup, FeatureNotFound
import requests

from backend.db import (
    finish_ingest_run,
    get_source_id,
    should_extract_entities,
    start_ingest_run,
    get_client,
    upsert_feed_item,
    enqueue_fetch,
    is_source_in_cooldown,
    mark_source_blocked,
    get_article_id_by_url,
    upsert_entities_for_article,
)
from backend.ollama import extract_entities, is_ollama_healthy
from backend.config import get_bool, get_int
from .extract import HEADERS, extract_main_text, fetch_url

_article_col_cache: dict[str, bool] = {}


def _article_has_column(sb, column: str) -> bool:
    if column in _article_col_cache:
        return _article_col_cache[column]
    try:
        sb.table("articles").select(column).limit(1).execute()
        _article_col_cache[column] = True
    except Exception as e:
        msg = str(e)
        if "column" in msg and "does not exist" in msg:
            _article_col_cache[column] = False
        else:
            _article_col_cache[column] = False
    return _article_col_cache[column]


def _bump_err_bucket(bs: dict, err: str) -> None:
    if not err:
        return
    if err.startswith("blocked:"):
        code = err.split(":", 1)[1]
        if code in {"403", "429"}:
            bs[f"err_http_{code}"] = bs.get(f"err_http_{code}", 0) + 1
        else:
            bs["err_blocked"] = bs.get("err_blocked", 0) + 1
        return
    if err.startswith("request_error:"):
        code = err.split(":", 1)[1].lower()
        if code == "dns":
            bs["err_dns"] = bs.get("err_dns", 0) + 1
        elif "timeout" in code:
            bs["err_timeout"] = bs.get("err_timeout", 0) + 1
        elif "ssl" in code or "tls" in code:
            bs["err_tls"] = bs.get("err_tls", 0) + 1
        elif "connect" in code:
            bs["err_connect"] = bs.get("err_connect", 0) + 1
        elif code.startswith("http"):
            bs["err_http"] = bs.get("err_http", 0) + 1
        else:
            bs["err_other"] = bs.get("err_other", 0) + 1
        return
    bs["err_other"] = bs.get("err_other", 0) + 1


def _content_hash_str(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


_BLOCK_MARKERS = (
    "access denied",
    "request blocked",
    "enable javascript",
    "cloudflare",
    "captcha",
    "checking your browser",
    "bot detection",
    "incident id",
    "akamai",
    "incapsula",
    "perimeterx",
)


def _is_blocked_text(text: str | None) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(marker in lower for marker in _BLOCK_MARKERS)


def _quality_rank(kind: str | None) -> int:
    kind = (kind or "full").lower()
    if kind == "full":
        return 3
    if kind == "teaser":
        return 2
    if kind == "title_only":
        return 1
    return 0


def _get_existing_quality(sb, table: str, url: str) -> dict:
    try:
        res = (
            sb.table(table)
            .select("content_kind,verification_status,fetch_quality,content,title,summary")
            .eq("url", url)
            .limit(1)
            .execute()
        )
    except Exception:
        return {}
    if res.data:
        return res.data[0] or {}
    return {}


def _parse_ts(val: str | None):
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None

DENY_SUBSTRINGS = [
    "/category/",
    "/tag/",
    "/author/",
    "/page/",  # pagination
    "/wp-json",
    "/wp-admin",
    "/contact",
    "/privacy",
    "/terms",
    "/media-contacts",
    "/security-council-resolutions",
]

ARTICLE_HINT_RE = re.compile(
    r"(\d{4}/\d{1,2}/\d{1,2}|"
    r"/news/|/press/|/article/|/post/|/blog/|"
    r"/story/|/report/|/analysis/|/politic|/econom|/local/)",
    re.IGNORECASE,
)

BAD_EXT_RE = re.compile(r"\.(pdf|jpg|jpeg|png|gif|webp|svg|mp4|mp3|zip)$", re.IGNORECASE)
URL_RE = re.compile(r'https?://[^\s"\'<>]+', re.IGNORECASE)

MAX_PAGES_PER_SOURCE = get_int("MAX_PAGES_PER_SOURCE", 10) or 10
FOLLOW_LINKS = get_bool("PAGE_INGEST_FOLLOW_LINKS", False)
MAX_SOURCES = get_int("MAX_SOURCES", 0) or 0
PROGRESS_EVERY = get_int("PROGRESS_EVERY", 5) or 5
SKIP_BLOCKED_SOURCES = get_bool("SKIP_BLOCKED_SOURCES", True)
SOURCE_TIME_BUDGET_SEC = get_int("SOURCE_TIME_BUDGET_SEC", 420) or 420
FETCH_TIMING = get_bool("FETCH_TIMING", False)
ALLOW_UNSMIL_TEASER_FALLBACK = get_bool("UNSMIL_ALLOW_TEASER_FALLBACK", True)
UNSMIL_MIN_FULL_LEN = get_int("UNSMIL_MIN_FULL_LEN", 1800) or 1800
DO_SUMMARY = os.getenv("EXTRACT_SUMMARY", "0") == "1"
def parse_source_ids_env() -> set[str] | None:
    raw = os.getenv("SOURCE_IDS", "").strip()
    if not raw:
        return None
    return {s.strip() for s in raw.split(",") if s.strip()}

CBL_EXCLUDE_PREFIXES = (
    "/history/",
    "/en/history/",
    "/about/",
    "/en/about/",
    "/contact/",
    "/en/contact/",
    "/careers/",
    "/en/careers/",
    "/our-mission/",
    "/en/our-mission/",
    "/strategic-plan/",
    "/en/strategic-plan/",
    "/services/",
    "/en/services/",
    "/publications/",
    "/en/publications/",
    "/downloads/",
    "/en/downloads/",
    "/media/",
    "/en/media/",
    "/events/",
    "/en/events/",
    "/tenders/",
    "/en/tenders/",
    "/circulars/",
    "/en/circulars/",
    "/tag/",
    "/category/",
    "/author/",
    "/page/",
)


def _cbl_reject_reason(url: str) -> str | None:
    p = urlparse(url)
    if not p.netloc.endswith("cbl.gov.ly"):
        return "not_cbl_host"
    if p.query:
        return "query"
    path = (p.path or "/").lower()
    if any(x in path for x in ("/wp-content/", "/wp-json/", "/wp-admin/")):
        return "wp_internal"
    if path.startswith(("/tag/", "/category/", "/author/", "/page/", "/feed/")):
        return "taxonomy"
    if BAD_EXT_RE.search(path):
        return "extension"
    for pref in CBL_EXCLUDE_PREFIXES:
        if path.startswith(pref):
            return "static_section"
    return None


def _log_cbl_filter_report(urls: list[str], allowed: list[str]) -> None:
    allow_set = set(allowed)
    reasons: dict[str, int] = {}
    for u in urls:
        if u in allow_set:
            continue
        reason = _cbl_reject_reason(u) or "other"
        reasons[reason] = reasons.get(reason, 0) + 1
    rejected = len(urls) - len(allowed)
    top = ", ".join(f"{k}={v}" for k, v in sorted(reasons.items(), key=lambda x: -x[1]))
    print(
        f"CBL_FILTER_REPORT allowed={len(allowed)} rejected={rejected} "
        f"reasons=({top})"
    )


def load_sources(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def extract_title_from_soup(soup: BeautifulSoup) -> Optional[str]:
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True) or None
    return None


def has_article_meta(soup: BeautifulSoup) -> bool:
    og_type = soup.find("meta", attrs={"property": "og:type"})
    if og_type and (og_type.get("content") or "").strip().lower() == "article":
        return True
    if soup.find("meta", attrs={"property": "article:published_time"}):
        return True
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            t = item.get("@type") if isinstance(item, dict) else None
            if isinstance(t, list):
                types = [str(x).lower() for x in t]
            else:
                types = [str(t).lower()] if t else []
            if any(x in ("article", "newsarticle") for x in types):
                return True
    return False


def extract_published_at(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    meta_keys = [
        ("property", "article:published_time"),
        ("property", "og:published_time"),
        ("name", "pubdate"),
        ("name", "publishdate"),
        ("name", "publish_date"),
        ("name", "date"),
        ("name", "dc.date"),
        ("name", "dc.date.issued"),
    ]

    for attr, value in meta_keys:
        tag = soup.find("meta", attrs={attr: value})
        if tag and tag.get("content"):
            return tag["content"].strip()

    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        return time_tag["datetime"].strip()

    return None


def looks_like_index(url: str, deny_substrings: list[str]) -> bool:
    u = url.lower()
    if any(d in u for d in deny_substrings):
        return True
    if u.rstrip("/").endswith(("/blog", "/news", "/announcements", "/press-releases", "/press")):
        return True
    return False


def extract_internal_links(
    html: str,
    base_url: str,
    limit: int = 20,
    allow_substrings: Optional[list[str]] = None,
    deny_substrings: Optional[list[str]] = None,
) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    base = urlparse(base_url)
    seen = set()
    links = []
    keywords = (
        "news",
        "press",
        "release",
        "statement",
        "announcements",
        "media",
        "update",
        "blog",
        "article",
        "post",
    )

    allow = [s.lower() for s in (allow_substrings or []) if s]
    deny = [s.lower() for s in (deny_substrings or []) if s]

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith("#"):
            continue
        if href.startswith("mailto:") or href.startswith("javascript:"):
            continue

        abs_url = urljoin(base_url, href)
        parsed = urlparse(abs_url)
        if parsed.scheme not in ("http", "https"):
            continue
        if parsed.netloc != base.netloc:
            continue

        normalized = _normalize_url(abs_url)
        if normalized.rstrip("/") == _normalize_url(base_url).rstrip("/"):
            continue
        if looks_like_index(normalized, deny):
            continue
        if allow and not any(a in normalized.lower() for a in allow):
            continue
        if not allow and not any(keyword in parsed.path.lower() for keyword in keywords):
            continue
        if normalized in seen:
            continue

        seen.add(normalized)
        links.append(normalized)
        if len(links) >= limit:
            break

    return links


def discover_article_links(
    seed_url: str,
    html: str,
    max_links: int = 30,
    debug: bool = False,
) -> tuple[list[str], dict]:
    soup = BeautifulSoup(html, "html.parser")
    base = urlparse(seed_url)
    out = []
    seen = set()
    lo_prefixes = {
        "news",
        "inbrief",
        "economy",
        "sports",
        "variety",
        "cartoons",
        "art",
        "crimes",
        "culture",
        "life",
        "opinions",
        "politics",
    }
    stats = {
        "a_count": 0,
        "internal_count": 0,
        "unique_internal": 0,
    }
    internal_seen = set()
    is_lo = base.netloc == "libyaobserver.ly"

    for a in soup.select("a[href]"):
        stats["a_count"] += 1
        href = a.get("href", "").strip()
        if not href:
            continue
        if href.startswith("javascript:"):
            continue

        u = urljoin(seed_url, href)
        u = _normalize_url(u)
        p = urlparse(u)

        if p.scheme not in ("http", "https"):
            continue
        if p.netloc and p.netloc not in (base.netloc, "www.libyaobserver.ly"):
            continue
        if p.netloc == "www.libyaobserver.ly":
            u = u.replace("www.libyaobserver.ly", "libyaobserver.ly")
            p = urlparse(u)
        if p.path.startswith("/cdn-cgi/"):
            continue
        if BAD_EXT_RE.search(p.path):
            continue
        if p.path in ("", "/"):
            continue
        if p.path.lower().startswith(("/user", "/search")):
            continue

        if is_lo:
            stats["internal_count"] += 1
            if u not in internal_seen:
                internal_seen.add(u)
            parts = [s for s in p.path.split("/") if s]
            if len(parts) < 2:
                continue
            if parts[0].lower() not in lo_prefixes:
                continue
            if p.path.lower() in (
                "/news",
                "/inbrief",
                "/economy",
                "/sports",
                "/variety",
                "/cartoons",
                "/art",
                "/crimes",
                "/culture",
                "/life",
                "/opinions",
                "/politics",
            ):
                continue

        hint = bool(ARTICLE_HINT_RE.search(p.path))
        if not hint:
            if len(p.path.strip("/").split("/")) < 2:
                continue

        clean = u
        if clean in seen:
            continue
        seen.add(clean)
        out.append(clean)

        if len(out) >= max_links:
            break

    if is_lo:
        stats["unique_internal"] = len(internal_seen)
    return out, stats


def fetch_views_ajax_links(source: dict, base_url: str) -> tuple[list[str], dict]:
    cfg = source.get("views_ajax") or {}
    allow_prefixes = cfg.get("allow_prefixes") or []
    ajax_path = cfg.get("ajax_path") or "/views/ajax"
    view_name = cfg.get("view_name")
    view_display_id = cfg.get("view_display_id")
    if not view_name or not view_display_id:
        return [], {}

    parsed = urlparse(base_url)
    base_root = f"{parsed.scheme}://{parsed.netloc}"
    ajax_url = urljoin(base_root, ajax_path)
    payload = {
        "view_name": view_name,
        "view_display_id": view_display_id,
        "view_args": cfg.get("view_args") or "",
        "view_path": cfg.get("view_path") or "",
        "view_base_path": cfg.get("view_base_path") or "",
        "view_dom_id": cfg.get("view_dom_id") or "",
        "pager_element": cfg.get("pager_element") or 0,
    }
    headers = {
        "User-Agent": HEADERS.get("User-Agent", "Mozilla/5.0"),
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    try:
        response = requests.post(
            ajax_url,
            data=payload,
            headers=headers,
            timeout=(10, 30),
        )
    except requests.RequestException as e:
        return [], {"error": f"request_error:{type(e).__name__}"}
    if response.status_code in (401, 403, 429):
        return [], {"error": f"blocked:{response.status_code}"}
    response.raise_for_status()
    try:
        data = response.json()
    except ValueError:
        return [], {"error": "parse_error:views_json"}

    chunks = []
    for item in data:
        if isinstance(item, dict) and isinstance(item.get("data"), str):
            chunks.append(item["data"])
    html = "\n".join(chunks)
    teasers: dict[str, dict] = {}
    soup = BeautifulSoup(html, "html.parser")
    for article in soup.select("article"):
        a = article.find("a", href=True)
        if not a:
            continue
        href = a.get("href", "").strip()
        if not href:
            continue
        link = urljoin(base_url, href)
        if allow_prefixes:
            path = urlparse(link).path
            if not any(path.startswith(prefix) for prefix in allow_prefixes):
                continue
        title = a.get_text(" ", strip=True)
        desc_el = article.select_one(".uw-listing-result__description")
        summary = desc_el.get_text(" ", strip=True) if desc_el else ""
        teasers[link] = {"title": title, "summary": summary}
    return list(teasers.keys()), {"teasers": teasers, "html_bytes": len(html)}


def harvest_same_host_urls(seed_url: str, html: str, max_links: int = 50) -> list[str]:
    host = urlparse(seed_url).netloc.lower()
    out = []
    seen = set()

    for u in URL_RE.findall(html or ""):
        try:
            pu = urlparse(u)
            if pu.netloc.lower() != host:
                continue
            if BAD_EXT_RE.search(pu.path):
                continue
            norm = _normalize_url(u)
            if norm in seen:
                continue
            seen.add(norm)
            out.append(norm)
            if len(out) >= max_links:
                break
        except Exception:
            continue

    return out


def _extract_loc_urls(xml_text: str, max_links: int) -> list[str]:
    try:
        urls = []
        for loc in re.findall(r"<loc>([^<]+)</loc>", xml_text):
            urls.append(loc.strip())
            if len(urls) >= max_links:
                break
        return urls
    except Exception:
        return []

def _extract_loc_urls_with_total(xml_text: str, max_links: int) -> tuple[list[str], int]:
    try:
        locs = [loc.strip() for loc in re.findall(r"<loc>([^<]+)</loc>", xml_text)]
        total = len(locs)
        if total > max_links:
            locs = locs[:max_links]
        return locs, total
    except Exception:
        return [], 0

def is_cbl_candidate_article(url: str) -> bool:
    p = urlparse(url)
    path = (p.path or "/").rstrip("/") + "/"
    lower = path.lower()

    for pref in CBL_EXCLUDE_PREFIXES:
        if path.startswith(pref):
            return False

    if BAD_EXT_RE.search(path):
        return False

    if any(x in lower for x in ("/wp-content/", "/wp-json/", "/wp-admin/", "/feed/")):
        return False

    if path.startswith("/%"):
        return True

    if path.count("/") == 2 and len(path) >= 20 and not path.startswith("/en/"):
        return True

    return False


def filter_cbl_sitemap_urls(source: dict, urls: list[str]) -> list[str]:
    if source.get("id") != "cbl":
        return urls
    filtered = [u for u in urls if is_cbl_candidate_article(u)]
    _log_cbl_filter_report(urls, filtered)
    return filtered


def log_timing(
    url: str, t0: float, fetch_ms: int, parse_ms: int, summarize_ms: int, db_ms: int
) -> None:
    if not FETCH_TIMING:
        return
    total_ms = int((time.monotonic() - t0) * 1000)
    print(
        f"TIMING url={url} fetch_ms={fetch_ms} parse_ms={parse_ms} "
        f"summarize_ms={summarize_ms} db_ms={db_ms} total_ms={total_ms}"
    )


def discover_sitemap_links(seed_url: str, source: dict, max_links: int = 50) -> list[str]:
    base = urlparse(seed_url)
    candidates = []
    if source.get("sitemap_index"):
        candidates.append(source["sitemap_index"])
    candidates.extend(
        [
            f"{base.scheme}://{base.netloc}/sitemap.xml",
            f"{base.scheme}://{base.netloc}/sitemap_index.xml",
            f"{base.scheme}://{base.netloc}/wp-sitemap.xml",
        ]
    )
    include = source.get("sitemap_include") or []
    exclude_prefixes = source.get("sitemap_exclude_prefixes") or []
    sitemap_only = source.get("sitemap_only") or ["post-sitemap", "posts-sitemap", "news-sitemap"]

    out = []
    seen = set()

    for sitemap_url in candidates:
        xml_text, err = fetch_url(sitemap_url, HEADERS)
        if err or not xml_text:
            continue
        if "<sitemapindex" in xml_text:
            index_urls = _extract_loc_urls(xml_text, max_links=50)
            if sitemap_only:
                index_urls = [u for u in index_urls if any(x in u for x in sitemap_only)]
            for idx in index_urls:
                xml_text2, err2 = fetch_url(idx, HEADERS)
                if err2 or not xml_text2:
                    continue
                urls = _extract_loc_urls(xml_text2, max_links=max_links)
                if include:
                    urls = [u for u in urls if any(x in u for x in include)]
                if exclude_prefixes:
                    urls = [
                        u
                        for u in urls
                        if not any(urlparse(u).path.startswith(p) for p in exclude_prefixes)
                    ]
                urls = filter_cbl_sitemap_urls(source, urls)
                for u in urls:
                    if u in seen:
                        continue
                    seen.add(u)
                    out.append(u)
                    if len(out) >= max_links:
                        return out
        else:
            urls = _extract_loc_urls(xml_text, max_links=max_links)
            if include:
                urls = [u for u in urls if any(x in u for x in include)]
            if exclude_prefixes:
                urls = [
                    u
                    for u in urls
                    if not any(urlparse(u).path.startswith(p) for p in exclude_prefixes)
                ]
            urls = filter_cbl_sitemap_urls(source, urls)
            for u in urls:
                if u in seen:
                    continue
                seen.add(u)
                out.append(u)
                if len(out) >= max_links:
                    return out

    return out


def discover_sitemap_links_with_counts(
    seed_url: str, source: dict, max_links: int = 50
) -> tuple[list[str], int]:
    base = urlparse(seed_url)
    candidates = []
    if source.get("sitemap_index"):
        candidates.append(source["sitemap_index"])
    candidates.extend(
        [
            f"{base.scheme}://{base.netloc}/sitemap.xml",
            f"{base.scheme}://{base.netloc}/sitemap_index.xml",
            f"{base.scheme}://{base.netloc}/wp-sitemap.xml",
        ]
    )
    include = source.get("sitemap_include") or []
    exclude_prefixes = source.get("sitemap_exclude_prefixes") or []
    sitemap_only = source.get("sitemap_only") or ["post-sitemap", "posts-sitemap", "news-sitemap"]

    out = []
    seen = set()
    raw_count_total = 0

    for sitemap_url in candidates:
        xml_text, err = fetch_url(sitemap_url, HEADERS)
        if err or not xml_text:
            continue
        if "<sitemapindex" in xml_text:
            index_urls = _extract_loc_urls(xml_text, max_links=50)
            if sitemap_only:
                index_urls = [u for u in index_urls if any(x in u for x in sitemap_only)]
            for idx in index_urls:
                xml_text2, err2 = fetch_url(idx, HEADERS)
                if err2 or not xml_text2:
                    continue
                urls, raw_count = _extract_loc_urls_with_total(xml_text2, max_links=max_links)
                raw_count_total += raw_count
                if include:
                    urls = [u for u in urls if any(x in u for x in include)]
                if exclude_prefixes:
                    urls = [
                        u
                        for u in urls
                        if not any(urlparse(u).path.startswith(p) for p in exclude_prefixes)
                    ]
                urls = filter_cbl_sitemap_urls(source, urls)
                for u in urls:
                    if u in seen:
                        continue
                    seen.add(u)
                    out.append(u)
                    if len(out) >= max_links:
                        return out, raw_count_total
        else:
            urls, raw_count = _extract_loc_urls_with_total(xml_text, max_links=max_links)
            raw_count_total += raw_count
            if include:
                urls = [u for u in urls if any(x in u for x in include)]
            if exclude_prefixes:
                urls = [
                    u
                    for u in urls
                    if not any(urlparse(u).path.startswith(p) for p in exclude_prefixes)
                ]
            urls = filter_cbl_sitemap_urls(source, urls)
            for u in urls:
                if u in seen:
                    continue
                seen.add(u)
                out.append(u)
                if len(out) >= max_links:
                    return out, raw_count_total

    return out, raw_count_total


def filter_source_urls(source: dict, urls: list[str]) -> list[str]:
    include_prefixes = source.get("sitemap_include_prefixes") or []
    include_non_en = bool(source.get("sitemap_include_non_en"))
    exclude_prefixes = source.get("sitemap_exclude_prefixes") or []
    link_allow = source.get("link_allow") or []
    link_deny = source.get("link_deny") or []
    out = []
    for u in urls:
        if source.get("id") == "libya_observer":
            p = urlparse(u)
            parts = [s for s in p.path.split("/") if s]
            if len(parts) < 2:
                continue
            if parts[0].lower() in {"news", "inbrief", "economy", "politics", "sports", "opinions"}:
                pass
            else:
                continue
            if p.path.lower() in (
                "/news",
                "/inbrief",
                "/economy",
                "/politics",
                "/sports",
                "/opinions",
            ):
                continue
            if any(x in p.path.lower() for x in ("/user/", "/tag/", "/search", "/contact", "/about", "/privacy", "/terms")):
                continue
            if len(parts[-1]) < 3:
                continue
        if source.get("id") == "libya_review":
            p = urlparse(u)
            if p.netloc not in {"libyareview.com", "www.libyareview.com"}:
                continue
            if p.query:
                continue
            parts = [s for s in p.path.split("/") if s]
            if len(parts) < 2:
                continue
            if any(x in p.path.lower() for x in ("/tag/", "/author/", "/page/")):
                continue
            if p.path.rstrip("/") in ("/news", "/category/libya", "/category/news"):
                continue
            # Accept WP-style dates or sluggy paths.
            if not ARTICLE_HINT_RE.search(p.path) and len(parts[-1]) < 5:
                continue
        path = urlparse(u).path
        if include_prefixes:
            if not any(path.startswith(p) for p in include_prefixes):
                if not (include_non_en and not path.startswith("/en/")):
                    continue
        if exclude_prefixes and any(path.startswith(p) for p in exclude_prefixes):
            continue
        if link_allow and not any(tok in path for tok in link_allow):
            continue
        if link_deny and any(tok in path for tok in link_deny):
            continue
        out.append(u)
    return out


def infer_language(url: str) -> str:
    if "/en/" in url:
        return "en"
    if "/ar/" in url:
        return "ar"
    return "unknown"


def infer_credibility(source_type: Optional[str]) -> int:
    if source_type == "official":
        return 9
    if source_type == "institutional":
        return 8
    return 5


def is_article_like(html: str, content: str, title: Optional[str], url: str | None = None) -> bool:
    h = (html or "").lower()
    t = (title or "").strip()
    c = (content or "").strip()

    if not t or t.startswith("http"):
        return False
    if url:
        path = urlparse(url).path.rstrip("/")
        if path in ("", "/en", "/ar"):
            return False

    has_article_meta = (
        'property="og:type"' in h and "article" in h
    ) or "article:published_time" in h
    has_article_tag = "<article" in h
    url_hint = bool(url and ARTICLE_HINT_RE.search(urlparse(url).path))

    if has_article_meta or has_article_tag or url_hint:
        return len(c) >= 200

    return len(c) >= 600


def _normalize_url(u: str) -> str:
    clean = urldefrag(u).url
    p = urlparse(clean)
    scheme = (p.scheme or "https").lower()
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = p.path or "/"
    if path.endswith("/amp"):
        path = path[: -len("/amp")] or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    drop_keys = {
        "fbclid",
        "gclid",
        "yclid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "ref",
        "ref_src",
    }
    keep_params = []
    for part in p.query.split("&"):
        if not part:
            continue
        key = part.split("=", 1)[0].lower()
        if key.startswith("utm_"):
            continue
        if key in drop_keys:
            continue
        if key == "amp":
            continue
        keep_params.append(part)
    query = "&".join(keep_params)
    return f"{scheme}://{host}{path}{'?' + query if query else ''}"


def _get_existing_urls(sb, urls: list[str]) -> set[str]:
    existing = set()
    chunk_size = 200
    for i in range(0, len(urls), chunk_size):
        chunk = urls[i : i + chunk_size]
        res = None
        try:
            res = sb.table("articles").select("url,canonical_url").in_("url", chunk).execute()
        except Exception:
            res = sb.table("articles").select("url").in_("url", chunk).execute()
        for row in res.data or []:
            url = row.get("url")
            if url:
                existing.add(_normalize_url(url))
            canon = row.get("canonical_url") if isinstance(row, dict) else None
            if canon:
                existing.add(_normalize_url(canon))
    return existing


def _get_existing_refresh_info(sb, urls: list[str]) -> dict[str, dict]:
    info: dict[str, dict] = {}
    chunk_size = 200
    for i in range(0, len(urls), chunk_size):
        chunk = urls[i : i + chunk_size]
        res = None
        try:
            res = (
                sb.table("articles")
                .select("url,last_seen_at,content_hash,content")
                .in_("url", chunk)
                .execute()
            )
        except Exception:
            try:
                res = sb.table("articles").select("url,content").in_("url", chunk).execute()
            except Exception:
                res = sb.table("articles").select("url").in_("url", chunk).execute()
        for row in res.data or []:
            url = row.get("url")
            if not url:
                continue
            nu = _normalize_url(url)
            info[nu] = {
                "last_seen_at": row.get("last_seen_at"),
                "content_hash": row.get("content_hash"),
                "content": row.get("content"),
            }
    return info


def _touch_last_seen(sb, urls: list[str]):
    if not urls:
        return
    if not _article_has_column(sb, "last_seen_at"):
        return
    chunk_size = 200
    now_iso = datetime.now(timezone.utc).isoformat()
    for i in range(0, len(urls), chunk_size):
        chunk = urls[i : i + chunk_size]
        try:
            sb.table("articles").update({"last_seen_at": now_iso}).in_("url", chunk).execute()
        except Exception:
            continue


def is_libya_observer_index(url: str) -> bool:
    p = urlparse(url)
    parts = [s.lower() for s in p.path.split("/") if s]
    if len(parts) != 1:
        return False
    return parts[0] in {"news", "inbrief", "economy", "politics", "sports", "opinions"}


def _lo_bucket(url: str) -> str:
    try:
        path = urlparse(url).path.strip("/")
    except Exception:
        return "other"
    if not path:
        return "other"
    first = path.split("/", 1)[0].lower()
    if first in {"news", "inbrief"}:
        return first
    return "other"


def _lo_spread(urls: list[str]) -> tuple[int, int, int]:
    news = 0
    inbrief = 0
    other = 0
    for u in urls:
        bucket = _lo_bucket(u)
        if bucket == "news":
            news += 1
        elif bucket == "inbrief":
            inbrief += 1
        else:
            other += 1
    return news, inbrief, other


class SourceStrategy:
    def __init__(
        self,
        key: str,
        log_prefix: str,
        seed_urls_fn,
        incremental: bool = False,
        seed_raw_total_fn=None,
        bucket_fn=None,
        min_scan: int = 10,
        stop_on_stale: int = 20,
        max_total_new: int = 30,
        max_new_per_section: int = 15,
    ):
        self.key = key
        self.log_prefix = log_prefix
        self.seed_urls_fn = seed_urls_fn
        self.incremental = incremental
        self.seed_raw_total_fn = seed_raw_total_fn
        self.bucket_fn = bucket_fn
        self.min_scan = min_scan
        self.stop_on_stale = stop_on_stale
        self.max_total_new = max_total_new
        self.max_new_per_section = max_new_per_section


def _libya_observer_seed_urls(_source: dict, _page_url: str) -> list[str]:
    return [f"https://libyaobserver.ly/news?page={i}" for i in range(0, 2)] + [
        f"https://libyaobserver.ly/inbrief?page={i}" for i in range(0, 2)
    ]


def _robots_sitemaps(base_url: str) -> list[str]:
    robots_url = base_url.rstrip("/") + "/robots.txt"
    text, err = fetch_url(robots_url, HEADERS)
    if err or not text:
        return []
    sitemaps = []
    for line in text.splitlines():
        if line.lower().startswith("sitemap:"):
            sitemaps.append(line.split(":", 1)[1].strip())
    return sitemaps


def _extract_sitemap_urls(xml_text: str, sitemap_only: list[str]) -> list[str]:
    if not xml_text:
        return []
    try:
        soup = BeautifulSoup(xml_text, "xml")
    except FeatureNotFound:
        soup = BeautifulSoup(xml_text, "html.parser")
    if soup.find("sitemapindex"):
        locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
        if sitemap_only:
            locs = [u for u in locs if any(x in u for x in sitemap_only)]
        return locs
    if soup.find("urlset"):
        return [loc.get_text(strip=True) for loc in soup.find_all("loc")]
    return []


def _extract_urlset_with_lastmod(xml_text: str) -> list[tuple[str, str | None]]:
    if not xml_text:
        return []
    try:
        soup = BeautifulSoup(xml_text, "xml")
    except FeatureNotFound:
        soup = BeautifulSoup(xml_text, "html.parser")
    if not soup.find("urlset"):
        return []
    out: list[tuple[str, str | None]] = []
    for url in soup.find_all("url"):
        loc = url.find("loc")
        if not loc or not loc.get_text(strip=True):
            continue
        lastmod = None
        lm = url.find("lastmod")
        if lm and lm.get_text(strip=True):
            lastmod = lm.get_text(strip=True)
        out.append((loc.get_text(strip=True), lastmod))
    return out


def _discover_sitemap_candidates(sitemap_urls: list[str]) -> list[str]:
    sitemap_only = ["post-sitemap", "posts-sitemap", "news-sitemap", "news", "post"]
    recent_limit = 300
    for sitemap_url in sitemap_urls:
        xml_text, err = fetch_url(sitemap_url, HEADERS)
        if err or not xml_text:
            continue
        locs = _extract_sitemap_urls(xml_text, sitemap_only)
        if not locs:
            continue
        # if index, fetch the first matching child sitemap
        if "<sitemapindex" in xml_text:
            for child in locs:
                child_xml, child_err = fetch_url(child, HEADERS)
                if child_err or not child_xml:
                    continue
                url_pairs = _extract_urlset_with_lastmod(child_xml)
                if url_pairs:
                    with_lastmod = [p for p in url_pairs if p[1]]
                    if with_lastmod:
                        with_lastmod.sort(key=lambda p: p[1], reverse=True)
                        urls = [u for u, _ in with_lastmod]
                    else:
                        urls = [u for u, _ in url_pairs]
                else:
                    urls = _extract_sitemap_urls(child_xml, [])
                if urls:
                    if len(urls) > recent_limit:
                        urls = urls[:recent_limit]
                    return urls
            continue
        if len(locs) > recent_limit:
            locs = locs[:recent_limit]
        return locs
    return []


def _discover_sitemap_candidates_with_meta(
    sitemap_urls: list[str], recent_limit: int
) -> tuple[list[str], int]:
    sitemap_only = ["post-sitemap", "posts-sitemap", "news-sitemap", "news", "post"]
    total_count = 0
    for sitemap_url in sitemap_urls:
        xml_text, err = fetch_url(sitemap_url, HEADERS)
        if err or not xml_text:
            continue
        locs = _extract_sitemap_urls(xml_text, sitemap_only)
        if not locs:
            continue
        if "<sitemapindex" in xml_text:
            for child in locs:
                child_xml, child_err = fetch_url(child, HEADERS)
                if child_err or not child_xml:
                    continue
                url_pairs = _extract_urlset_with_lastmod(child_xml)
                if url_pairs:
                    total_count = len(url_pairs)
                    with_lastmod = [p for p in url_pairs if p[1]]
                    if with_lastmod:
                        with_lastmod.sort(key=lambda p: p[1], reverse=True)
                        urls = [u for u, _ in with_lastmod]
                    else:
                        urls = [u for u, _ in url_pairs]
                else:
                    urls = _extract_sitemap_urls(child_xml, [])
                    total_count = len(urls)
                if urls:
                    if len(urls) > recent_limit:
                        urls = urls[:recent_limit]
                    return urls, total_count
            continue
        total_count = len(locs)
        if len(locs) > recent_limit:
            locs = locs[:recent_limit]
        return locs, total_count
    return [], 0


def _extract_rss_urls(xml_text: str) -> list[str]:
    if not xml_text:
        return []
    try:
        soup = BeautifulSoup(xml_text, "xml")
    except FeatureNotFound:
        soup = BeautifulSoup(xml_text, "html.parser")
    urls = []
    for item in soup.find_all("item"):
        link = item.find("link")
        if link and link.get_text(strip=True):
            urls.append(link.get_text(strip=True))
    return urls


def _libya_review_seed_urls(source: dict, page_url: str) -> list[str]:
    base = (source.get("url") or page_url or "https://libyareview.com").rstrip("/")
    recent_limit = int(os.getenv("LR_RECENT_LIMIT") or 300)
    sitemap_urls = _robots_sitemaps(base)
    if sitemap_urls:
        sitemap_candidates, total_count = _discover_sitemap_candidates_with_meta(
            sitemap_urls, recent_limit
        )
        if sitemap_candidates:
            print(
                f"LR_SEED_MODE mode=sitemap urls={total_count} recent_limit={recent_limit} "
                f"used={len(sitemap_candidates)}"
            )
            source["_seed_mode"] = "sitemap"
            source["_seed_raw_total"] = total_count
            return sitemap_candidates
    for rss_path in ("/feed", "/rss", "/rss.xml"):
        rss_url = base + rss_path
        xml_text, err = fetch_url(rss_url, HEADERS)
        if err:
            continue
        rss_urls = _extract_rss_urls(xml_text)
        if rss_urls:
            print(f"LR_SEED_MODE mode=rss urls={len(rss_urls)}")
            source["_seed_mode"] = "rss"
            source["_seed_raw_total"] = len(rss_urls)
            return rss_urls
    seeds = []
    for path in ("/category/libya", "/news"):
        for i in range(0, 2):
            seeds.append(f"{base}{path}?page={i}")
    print(f"LR_SEED_MODE mode=category pages={len(seeds)}")
    source["_seed_mode"] = "category"
    return seeds


def _incremental_select(
    candidates: list[str],
    existing: set[str],
    stop_on_stale: int,
    min_scan: int,
    max_total_new: int,
) -> tuple[list[str], bool]:
    to_fetch = []
    stale = 0
    scanned = 0
    stopped = False
    for u in candidates:
        scanned += 1
        if u in existing:
            stale += 1
        else:
            stale = 0
            to_fetch.append(u)
            if len(to_fetch) >= max_total_new:
                break
        if scanned >= min_scan and stale >= stop_on_stale:
            stopped = True
            break
    return to_fetch, stopped


def _incremental_select_bucketed(
    candidates: list[str],
    existing: set[str],
    bucket_fn,
    stop_on_stale: int,
    max_new_per_section: int,
    max_total_new: int,
) -> tuple[list[str], bool]:
    buckets: dict[str, list[str]] = {}
    for u in candidates:
        bucket = bucket_fn(u) if bucket_fn else "other"
        buckets.setdefault(bucket, []).append(u)
    stopped = False
    selected: dict[str, list[str]] = {}
    for bucket, items in buckets.items():
        stale = 0
        picked = []
        for u in items:
            if u in existing:
                stale += 1
            else:
                stale = 0
                picked.append(u)
                if len(picked) >= max_new_per_section:
                    break
            if stale >= stop_on_stale:
                stopped = True
                break
        selected[bucket] = picked
    to_fetch = []
    bucket_order = [b for b in ("news", "inbrief") if b in selected]
    bucket_order += [b for b in selected.keys() if b not in bucket_order]
    idx = {b: 0 for b in selected}
    while len(to_fetch) < max_total_new:
        progressed = False
        for b in bucket_order:
            i = idx[b]
            if i < len(selected[b]):
                to_fetch.append(selected[b][i])
                idx[b] += 1
                progressed = True
                if len(to_fetch) >= max_total_new:
                    break
        if not progressed:
            break
    return to_fetch, stopped


STRATEGIES = {
    "libya_observer": SourceStrategy(
        key="libya_observer",
        log_prefix="LO",
        seed_urls_fn=_libya_observer_seed_urls,
        incremental=True,
        bucket_fn=_lo_bucket,
        min_scan=10,
        stop_on_stale=20,
        max_total_new=30,
        max_new_per_section=15,
    ),
    "libya_review": SourceStrategy(
        key="libya_review",
        log_prefix="LR",
        seed_urls_fn=_libya_review_seed_urls,
        incremental=True,
        seed_raw_total_fn=lambda s: int(s.get("_seed_raw_total") or 0),
        bucket_fn=None,
        min_scan=10,
        stop_on_stale=20,
        max_total_new=30,
        max_new_per_section=15,
    ),
}


def _finalize_incremental(
    strategy: SourceStrategy,
    source_key: str,
    source: dict,
    bs: dict,
    candidates: list[str],
    sb,
    debug_lo: bool,
    seed_total: int,
    seed_mode: str,
    remaining_global: int,
    pending_links: list[str],
    visited: set[str],
    max_pages: int,
):
    debug_lr = os.getenv("DEBUG_LR") == "1"
    uniq = []
    seen = set()
    for u in candidates:
        nu = _normalize_url(u)
        if nu in seen:
            continue
        seen.add(nu)
        uniq.append(nu)
    kept_candidates = uniq
    prefix = strategy.log_prefix
    seed_raw = strategy.seed_raw_total_fn(source) if strategy.seed_raw_total_fn else 0
    if seed_raw:
        bs["discovered_total"] = seed_raw
        bs["discovered_total_raw"] = seed_raw
    else:
        bs["discovered_total"] = len(candidates)
    bs["kept_total"] = len(kept_candidates)
    print(
        f"{prefix}_DISCOVERY_TOTAL seed_pages={bs.get('seed_pages_fetched', 0)} "
        f"a_total={bs.get('a_count_total', 0)} "
        f"internal_total={bs.get('internal_total', 0)} "
        f"unique_internal_total={bs.get('unique_internal', 0)} "
        f"kept_total={len(kept_candidates)}"
    )
    if debug_lo and prefix == "LO":
        news_d, inbrief_d, other_d = _lo_spread(kept_candidates)
        print(
            f"{prefix}_SPREAD_DISCOVERY news={news_d} inbrief={inbrief_d} "
            f"other={other_d} total={len(kept_candidates)}"
        )
        news_sample = [u for u in kept_candidates if _lo_bucket(u) == "news"][:5]
        inbrief_sample = [u for u in kept_candidates if _lo_bucket(u) == "inbrief"][:5]
        print(f"{prefix}_BUCKET_SAMPLE news={news_sample}")
        print(f"{prefix}_BUCKET_SAMPLE inbrief={inbrief_sample}")
    unique_total = bs.get("unique_internal", 0) or 0
    if unique_total:
        kept_ratio = len(kept_candidates) / float(unique_total)
        if kept_ratio < 0.08:
            bs["degraded"] = True
            bs["degraded_reason"] = "low_kept_ratio"
            bs["discovery_degraded"] = True
            print(
                f"DISCOVERY_DEGRADED kept_ratio={kept_ratio:.3f} "
                f"kept={len(kept_candidates)} unique={unique_total}"
            )
    deduped = 0
    existing = set()
    try:
        existing = _get_existing_urls(sb, kept_candidates)
        new_candidates = [u for u in kept_candidates if u not in existing]
        deduped = len(kept_candidates) - len(new_candidates)
        bs["deduped_existing"] = deduped
        bs["new_candidates"] = len(new_candidates)
        bs["kept_candidates"] = len(kept_candidates)
        bs["dedup_new"] = len(new_candidates)
        print(
            f"DEDUP existing={deduped} kept={len(kept_candidates)} new={len(new_candidates)}"
        )
    except Exception:
        print("DEDUP skipped db_error")
        new_candidates = kept_candidates
    effective_existing = existing
    if strategy.key == "libya_observer":
        refresh_allowed = set()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
        info = _get_existing_refresh_info(sb, kept_candidates)
        for u, row in info.items():
            ts = _parse_ts(row.get("last_seen_at"))
            if ts and ts < cutoff:
                refresh_allowed.add(u)
        if refresh_allowed:
            print(
                f"LO_REFRESH_REVISIT allowed={len(refresh_allowed)} reason=age>72h"
            )
        effective_existing = existing - refresh_allowed
    max_total_new = strategy.max_total_new
    if strategy.log_prefix == "LR":
        try:
            max_total_new = int(os.getenv("LR_MAX_NEW") or max_total_new)
        except Exception:
            max_total_new = strategy.max_total_new
    if remaining_global >= 0:
        max_total_new = min(max_total_new, remaining_global)
    to_fetch = []
    stopped_on_stale = False
    if seed_mode in {"sitemap", "rss"}:
        # Sitemap/RSS: trust candidates, select directly from DB-deduped list.
        to_fetch = new_candidates[:max_total_new]
        top_n = 50
        top_existing = 0
        for u in kept_candidates[:top_n]:
            if u in effective_existing:
                top_existing += 1
        print(f"{prefix}_TOP_EXISTING top_existing={top_existing} top_n={top_n}")
        if top_existing >= 45 and len(new_candidates) == 0:
            to_fetch = []
        print(
            f"{prefix}_INCREMENTAL seed_mode={seed_mode} seed_urls={len(kept_candidates)} "
            f"kept={len(kept_candidates)} existing={deduped} new={len(new_candidates)} "
            f"selected={len(to_fetch)} stopped_on_stale=null max_stale=0"
        )
        print(
            f"{prefix}_DEBUG_COUNTS kept={len(kept_candidates)} existing={deduped} "
            f"new={len(new_candidates)} selected={len(to_fetch)}"
        )
        if len(new_candidates) > 0 and len(to_fetch) == 0 and max_total_new > 0:
            msg = f"{prefix}_SELECTOR_BUG new={len(new_candidates)} selected=0 seed_mode={seed_mode}"
            if debug_lr and prefix == "LR":
                raise RuntimeError(msg)
            print(msg)
    else:
        if strategy.bucket_fn:
            to_fetch, stopped_on_stale = _incremental_select_bucketed(
                kept_candidates,
                effective_existing,
                strategy.bucket_fn,
                strategy.stop_on_stale,
                strategy.max_new_per_section,
                max_total_new,
            )
        else:
            to_fetch, stopped_on_stale = _incremental_select(
                kept_candidates,
                effective_existing,
                strategy.stop_on_stale,
                strategy.min_scan,
                max_total_new,
            )
        print(
            f"{prefix}_INCREMENTAL seed_mode=category seed_pages={seed_total} "
            f"candidates={len(kept_candidates)} existing={deduped} new={len(new_candidates)} "
            f"selected={len(to_fetch)} "
            f"stopped_on_stale={str(stopped_on_stale).lower()} max_stale={strategy.stop_on_stale}"
        )
    if not to_fetch:
        print(
            f"NO_NEW_CANDIDATES source={source_key} kept={len(kept_candidates)} "
            f"existing={deduped}"
        )
        return 0
    for u in to_fetch[:max_pages]:
        if u not in visited:
            pending_links.append(u)
    return len(to_fetch)


def main():
    sb = get_client()
    lock_path = "/tmp/libyaintel_page_ingest.lock"
    try:
        lock_fd = open(lock_path, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        print("JOB_LOCKED exit=1")
        return 1
    run_id = start_ingest_run(sb, "page_ingest")
    debug_lo = os.getenv("DEBUG_LO") == "1"
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
    requested = parse_source_ids_env()
    if requested is not None:
        sources = [s for s in sources if s.get("id") in requested]
        print(f"SOURCE_FILTER ids={','.join(sorted(requested))}")
    else:
        has_active_flag = any("is_active" in s for s in sources)
        if has_active_flag:
            # Treat is_active as an override, but don't exclude sources that only use enabled.
            sources = [s for s in sources if s.get("is_active", s.get("enabled", False))]
        else:
            sources = [s for s in sources if s.get("enabled")]

    seen_urls = set()
    blocked_sources = set()
    processed = 0
    started_ts = time.monotonic()
    finished = False
    error_msg = None
    aborted = False
    terminate = False
    remaining_global = get_int("MAX_NEW_GLOBAL", 200)
    delay_min_ms = get_int("MIN_DOMAIN_DELAY_MS", 0)
    delay_max_ms = get_int("MAX_DOMAIN_DELAY_MS", delay_min_ms)

    def _maybe_delay():
        if delay_min_ms or delay_max_ms:
            lo = min(delay_min_ms, delay_max_ms)
            hi = max(delay_min_ms, delay_max_ms)
            time.sleep(random.uniform(lo, hi) / 1000.0)

    def _handle_term(signum, frame):
        nonlocal terminate
        terminate = True

    signal.signal(signal.SIGTERM, _handle_term)
    signal.signal(signal.SIGINT, _handle_term)

    def _bs(source_key: str) -> dict:
        return stats["by_source"].setdefault(
            source_key,
            {
                "total": 0,
                # discovery stats
                "seed_pages_fetched": 0,
                "a_count_total": 0,
                "internal_total": 0,
                "unique_internal": 0,
                "kept_candidates": 0,
                "deduped_existing": 0,
                "new_candidates": 0,
                "discovered_total": 0,
                "kept_total": 0,
                "dedup_new": 0,
                # fetch stats
                "attempted": 0,
                "saved": 0,
                "updated_existing": 0,
                "skipped_existing_fetch": 0,
                "skipped_non_article": 0,
                "skipped": 0,
                "failed": 0,
                "blocked": 0,
                "llm_failed": 0,
                "summary_skipped": 0,
                "summary_failed": 0,
                "junk_saved": 0,
                "discovery_degraded": False,
                "fetch_degraded": False,
                "caught_up": False,
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

    lo_debug_printed = False
    seed_cache: dict[str, tuple[float, list[str]]] = {}
    seed_cache_ttl = 6 * 60 * 60

    try:
        source_count = 0
        for source in sources:
            if remaining_global <= 0:
                break

            source_key = source.get("id")
            if source_key in blocked_sources:
                continue
            if SKIP_BLOCKED_SOURCES and is_source_in_cooldown(sb, source_key):
                continue
            page_url = source.get("seed_url") or source.get("url")
            if not page_url:
                continue
            source_started = time.monotonic()
            source_count += 1
            if MAX_SOURCES and source_count > MAX_SOURCES:
                break

            link_allow = source.get("link_allow") or []
            link_deny = DENY_SUBSTRINGS + (source.get("link_deny") or [])

            if FOLLOW_LINKS:
                norm_link = _normalize_url(link)
                try:
                    page_html, page_err = fetch_url(page_url, HEADERS)
                    if page_err:
                        stats["failed"] += 1
                        bs = _bs(source_key)
                        bs["failed"] += 1
                        bs["last_error"] = page_err
                        _bump_err_bucket(bs, page_err)
                        if page_err == "request_error:dns":
                            print(f"Failed seed: {page_url} reason=dns", file=sys.stderr)
                        if page_err.startswith("blocked:"):
                            stats["blocked"] += 1
                            bs["blocked"] += 1
                            blocked_sources.add(source_key)
                        print(f"Failed to load source page: {page_url} -> {page_err}", file=sys.stderr)
                        continue
                    links = extract_internal_links(
                        page_html or "",
                        page_url,
                        limit=20,
                        allow_substrings=link_allow,
                        deny_substrings=link_deny,
                    )
                except Exception as e:
                    stats["failed"] += 1
                    bs = _bs(source_key)
                    bs["failed"] += 1
                    bs["last_error"] = f"source_page_error:{type(e).__name__}"
                    print(f"Failed to load source page: {page_url} -> {e}", file=sys.stderr)
                    continue
            else:
                strategy = STRATEGIES.get(source_key)
                if strategy:
                    links = strategy.seed_urls_fn(source, page_url)
                elif source.get("sitemap_seed_only"):
                    if source.get("id") == "cbl":
                        raw_count = 0
                        links, raw_count = discover_sitemap_links_with_counts(
                            page_url, source, max_links=30
                        )
                        links = filter_source_urls(source, links)
                        bs = _bs(source_key)
                        bs["discovered_total"] += raw_count
                        bs["discovered_total_raw"] = bs.get("discovered_total_raw", 0) + raw_count
                        bs["kept_total"] += len(links)
                        bs["kept_candidates"] += len(links)
                    else:
                        links = filter_source_urls(
                            source, discover_sitemap_links(page_url, source, max_links=30)
                        )
                elif source.get("seed_pages"):
                    seeds = []
                    page_range = source.get("seed_page_range") or [0, 0]
                    try:
                        start, end = int(page_range[0]), int(page_range[1])
                    except Exception:
                        start, end = 0, 0
                    for tmpl in source.get("seed_pages") or []:
                        for i in range(start, end + 1):
                            seeds.append(tmpl.format(page=i))
                    links = seeds
                elif source.get("seed_paths"):
                    seeds = []
                    seed_pages = int(source.get("seed_pages") or 0)
                    base_root = source.get("url") or page_url
                    for path in source.get("seed_paths") or []:
                        if not path.startswith("/"):
                            path = "/" + path
                        seeds.append(f"{base_root.rstrip('/')}{path}")
                        for i in range(seed_pages):
                            seeds.append(f"{base_root.rstrip('/')}{path}?page={i}")
                    links = seeds
                else:
                    links = [] if looks_like_index(page_url, link_deny) else [page_url]

            # Normalize and de-dupe seed links early to reduce duplicate work.
            links = [_normalize_url(u) for u in links if u]
            links = list(dict.fromkeys(links))

            visited = set()
            strategy = STRATEGIES.get(source_key)
            pending_links = list(links)
            seed_norms = { _normalize_url(u) for u in links if u }
            sitemap_urls = set()
            saved_count = 0
            processed_count = 0
            existing_url_cache: set[str] = set()
            missing_url_cache: set[str] = set()
            views_teasers: dict[str, dict] = {}
            sitemap_seeded = bool(source.get("sitemap_seed_only"))
            if sitemap_seeded:
                sitemap_urls.update(links)
            strat_seed_urls = set(links) if strategy and strategy.incremental else set()
            strat_seed_total = len(strat_seed_urls)
            strat_candidates: list[str] = []
            strat_seed_done = False
            strat_existing_info: dict[str, dict] = {}
            strat_existing_set: set[str] = set()
            strat_refresh_allowed: set[str] = set()

            if source.get("views_ajax"):
                links, views_meta = fetch_views_ajax_links(source, source.get("url") or page_url)
                views_teasers = views_meta.get("teasers") or {}
                if views_meta.get("error"):
                    bs = _bs(source_key)
                    bs["last_error"] = views_meta["error"]
                if links:
                    links = filter_source_urls(source, links)
                    bs = _bs(source_key)
                    bs["discovered_total"] += len(links)
                    bs["kept_total"] += len(links)
                    bs["kept_candidates"] += len(links)
                    pending_links = list(links)
                    seed_norms = { _normalize_url(u) for u in links if u }

            if strategy and strategy.incremental and source.get("_seed_mode") in {"sitemap", "rss"}:
                pending_links = []
                bs = _bs(source_key)
                bs["seed_pages_fetched"] += 1
                strat_candidates = list(links)
                strat_seed_done = True
                selected_count = _finalize_incremental(
                    strategy,
                    source_key,
                    source,
                    bs,
                    strat_candidates,
                    sb,
                    debug_lo,
                    strat_seed_total or len(links),
                    source.get("_seed_mode") or "sitemap",
                    remaining_global,
                    pending_links,
                    visited,
                    MAX_PAGES_PER_SOURCE,
                )
                remaining_global = max(0, remaining_global - selected_count)

            while pending_links and processed_count < MAX_PAGES_PER_SOURCE:
                if terminate:
                    aborted = True
                    error_msg = "terminated"
                    break
                if SOURCE_TIME_BUDGET_SEC and time.monotonic() - source_started > SOURCE_TIME_BUDGET_SEC:
                    bs = _bs(source_key)
                    bs["last_error"] = "source_time_budget"
                    print(f"Source time budget reached: {source_key}")
                    break
                link = pending_links.pop(0)
                if link in visited:
                    continue
                visited.add(link)
                processed_count += 1
                if link in seen_urls:
                    stats["skipped"] += 1
                    bs = _bs(source_key)
                    bs["skipped"] += 1
                    print(f"Skipped: {link}")
                    continue

                seen_urls.add(link)
                stats["total"] += 1
                bs = _bs(source_key)
                bs["total"] += 1
                bs.setdefault("updated_existing", 0)
                stats.setdefault("updated_existing", 0)
                processed += 1
                is_seed_page = False
                if _normalize_url(link) in seed_norms:
                    is_seed_page = True
                if strategy and strategy.incremental and source.get("_seed_mode") == "category":
                    if link in strat_seed_urls:
                        is_seed_page = True
                elif not strategy and looks_like_index(link, link_deny):
                    is_seed_page = True

                t0 = time.monotonic()
                fetch_ms = 0
                parse_ms = 0
                summarize_ms = 0
                db_ms = 0
                phase_ts = t0
                try:
                    # Count every fetch attempt (even seed pages) to keep saved <= attempted.
                    bs["attempted"] += 1
                    html, err = fetch_url(link, HEADERS)
                    fetch_ms = int((time.monotonic() - phase_ts) * 1000)
                    phase_ts = time.monotonic()
                    if err:
                        stats["failed"] += 1
                        bs["failed"] += 1
                        bs["last_error"] = err
                        _bump_err_bucket(bs, err)
                        if err == "request_error:dns" and link in links:
                            print(f"Failed seed: {link} reason=dns", file=sys.stderr)
                            cached = seed_cache.get(link)
                            if cached:
                                ts, cached_links = cached
                                if time.time() - ts <= seed_cache_ttl:
                                    for u in cached_links:
                                        if u not in visited:
                                            pending_links.append(u)
                                            sitemap_urls.add(u)
                                    stats.setdefault("discovered", 0)
                                    bs.setdefault("discovered", 0)
                                    stats["discovered"] += len(cached_links)
                                    bs["discovered"] += len(cached_links)
                                    print(
                                        f"Seed cache used: {link} -> discovered {len(cached_links)} links"
                                    )
                                    log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                                    continue
                        if err.startswith("blocked:"):
                            stats["blocked"] += 1
                            bs["blocked"] += 1
                            blocked_sources.add(source_key)
                            mark_source_blocked(sb, source_key)
                        print(f"Failed: {link} -> {err}", file=sys.stderr)
                        if err.startswith("blocked:"):
                            log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                            break
                        log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                        continue
                    used_teaser = False
                    teaser = views_teasers.get(link)
                    if html is not None and not html.strip() and teaser and ALLOW_UNSMIL_TEASER_FALLBACK:
                        title = teaser.get("title") or ""
                        content = teaser.get("summary") or ""
                        article_meta = True
                        link_count = 0
                        text_len = len((content or "").strip())
                        link_density = 0.0
                        parse_ms = int((time.monotonic() - phase_ts) * 1000)
                        used_teaser = True
                        stats["used_teaser"] = stats.get("used_teaser", 0) + 1
                        bs["used_teaser"] = bs.get("used_teaser", 0) + 1
                    else:
                        soup = BeautifulSoup(html or "", "html.parser")
                        if (
                            source_key == "libya_observer"
                            and is_libya_observer_index(link)
                            and not lo_debug_printed
                            and debug_lo
                        ):
                            urls = []
                            seen = set()
                            for a in soup.find_all("a", href=True):
                                href = a.get("href", "").strip()
                                if not href:
                                    continue
                                abs_url = urljoin(link, href)
                                if abs_url in seen:
                                    continue
                                seen.add(abs_url)
                                urls.append(abs_url)
                            print(
                                f"LO_DEBUG status=200 bytes={len((html or '').encode('utf-8'))} "
                                f"a_count={len(soup.find_all('a'))}"
                            )
                            for u in urls[:20]:
                                print(f"LO_DEBUG_URL {u}")
                            lo_debug_printed = True
                        title = extract_title_from_soup(soup)
                        article_meta = has_article_meta(soup)
                    if (
                        link in sitemap_urls
                        and not article_meta
                        and source_key != "libya_observer"
                        and not is_seed_page
                        and not source.get("allow_seed_without_article_meta")
                        and not source.get("allow_without_article_meta")
                    ):
                        stats["skipped"] += 1
                        bs["skipped"] += 1
                        print(f"Skipped (not article meta): {link}")
                        log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                        continue
                    if not used_teaser:
                        content = extract_main_text(html or "")
                        parse_ms = int((time.monotonic() - phase_ts) * 1000)
                        link_count = len(soup.find_all("a"))
                        text_len = len((content or "").strip())
                        link_density = (link_count / max(text_len, 1)) if text_len else 1.0
                except Exception as e:
                    stats["failed"] += 1
                    bs["failed"] += 1
                    msg = str(e).strip().replace("\n", " ")
                    msg = msg[:120] if msg else ""
                    suffix = f":{msg}" if msg else ""
                    bs["last_error"] = f"parse_error:{type(e).__name__}{suffix}"
                    print(f"Failed: {link} -> {e}", file=sys.stderr)
                    log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                    continue

                article_like = is_article_like(html, content, title, link)
                if is_seed_page and not article_meta:
                    article_like = False
                if used_teaser:
                    article_like = True
                if source_key == "libya_observer" and is_libya_observer_index(link):
                    article_like = False
                if not article_like:
                    prefer_sitemap = bool(source.get("prefer_sitemap"))
                    disable_harvest = bool(source.get("disable_harvest"))

                    discovered = []
                    if prefer_sitemap and not sitemap_seeded:
                        raw_count = 0
                        if source.get("id") == "cbl":
                            discovered, raw_count = discover_sitemap_links_with_counts(
                                link, source, max_links=30
                            )
                            discovered = filter_source_urls(source, discovered)
                            bs["discovered_total"] += raw_count
                            bs["discovered_total_raw"] = bs.get("discovered_total_raw", 0) + raw_count
                            bs["kept_total"] += len(discovered)
                            bs["kept_candidates"] += len(discovered)
                        else:
                            discovered = filter_source_urls(
                                source, discover_sitemap_links(link, source, max_links=30)
                            )
                        sitemap_seeded = True
                        if discovered:
                            print(f"Sitemap candidates: {len(discovered)} after filter")
                            if source_key != "libya_observer" or debug_lo:
                                print(
                                    f"Seed page (not article): {link} -> sitemap {len(discovered)} URLs"
                                )
                    if not discovered:
                        disc_limit = 30
                        if source.get("link_allow") or source.get("link_deny"):
                            disc_limit = 200
                        discovered, disc_stats = discover_article_links(
                            link, html, max_links=disc_limit, debug=source_key == "libya_observer"
                        )
                        discovered = filter_source_urls(source, discovered)
                    if not discovered and not disable_harvest:
                        discovered = filter_source_urls(
                            source, harvest_same_host_urls(link, html, max_links=50)
                        )
                        if discovered:
                            if source_key != "libya_observer" or debug_lo:
                                print(
                                    f"Seed page (not article): {link} -> harvested {len(discovered)} same-host URLs"
                                )
                    if not discovered and not sitemap_seeded:
                        raw_count = 0
                        if source.get("id") == "cbl":
                            discovered, raw_count = discover_sitemap_links_with_counts(
                                link, source, max_links=30
                            )
                            discovered = filter_source_urls(source, discovered)
                            bs["discovered_total"] += raw_count
                            bs["discovered_total_raw"] = bs.get("discovered_total_raw", 0) + raw_count
                            bs["kept_total"] += len(discovered)
                            bs["kept_candidates"] += len(discovered)
                        else:
                            discovered = filter_source_urls(
                                source, discover_sitemap_links(link, source, max_links=30)
                            )
                        sitemap_seeded = True
                        if discovered:
                            print(f"Sitemap candidates: {len(discovered)} after filter")
                            if source_key != "libya_observer" or debug_lo:
                                print(
                                    f"Seed page (not article): {link} -> sitemap {len(discovered)} URLs"
                                )
                    if discovered:
                        kept_candidates = discovered
                        if source_key == "libya_observer":
                            news_bucket = [u for u in kept_candidates if "/news/" in u]
                            other_bucket = [u for u in kept_candidates if u not in news_bucket]
                            kept_candidates = news_bucket + other_bucket
                        new_candidates = kept_candidates
                        deduped = 0
                        if link in links and source_key == "libya_observer" and debug_lo:
                            sample_kept = ", ".join(kept_candidates[:5])
                            print(
                                f"LO_DISCOVERY a_count={disc_stats.get('a_count', 0)} "
                                f"internal_count={disc_stats.get('internal_count', 0)} "
                                f"unique_internal={disc_stats.get('unique_internal', 0)} "
                                f"kept_candidates={len(kept_candidates)} "
                                f"sample_kept={sample_kept}"
                            )
                        if link in links:
                            seed_cache[link] = (time.time(), list(kept_candidates))
                        if strategy and strategy.incremental and link in strat_seed_urls:
                            strat_candidates.extend(kept_candidates)
                            bs["seed_pages_fetched"] += 1
                            bs["a_count_total"] += disc_stats.get("a_count", 0)
                            bs["internal_total"] += disc_stats.get("internal_count", 0)
                            bs["unique_internal"] += disc_stats.get("unique_internal", 0)
                            if not strategy or debug_lo:
                                print(
                                    f"Seed page (not article): {link} -> discovered {len(discovered)} links"
                                )
                            log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                            if not (
                                not strat_seed_done
                                and strat_candidates
                                and bs.get("seed_pages_fetched", 0) >= strat_seed_total
                            ):
                                continue
                            strat_seed_done = True
                            selected_count = _finalize_incremental(
                                strategy,
                                source_key,
                                source,
                                bs,
                                strat_candidates,
                                sb,
                                debug_lo,
                                strat_seed_total,
                                source.get("_seed_mode") or "category",
                                remaining_global,
                                pending_links,
                                visited,
                                MAX_PAGES_PER_SOURCE,
                            )
                            remaining_global = max(0, remaining_global - selected_count)
                            continue
                        try:
                            normalized = [_normalize_url(u) for u in kept_candidates]
                            existing = _get_existing_urls(sb, normalized)
                            new_candidates = [u for u in normalized if u not in existing]
                            deduped = len(normalized) - len(new_candidates)
                            bs = _bs(source_key)
                            bs["deduped_existing"] = deduped
                            bs["new_candidates"] = len(new_candidates)
                            bs["kept_candidates"] = len(normalized)
                            print(
                                f"DEDUP existing={deduped} kept={len(normalized)} new={len(new_candidates)}"
                            )
                        except Exception:
                            print("DEDUP skipped db_error")
                        if not new_candidates:
                            if source_key == "libya_observer":
                                print(
                                    f"NO_NEW_CANDIDATES source={source_key} kept={len(kept_candidates)} "
                                    f"existing={deduped}"
                                )
                            log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                            continue
                        for u in new_candidates:
                            if u not in visited:
                                pending_links.append(u)
                                sitemap_urls.add(u)
                        if source_key != "libya_observer" or debug_lo:
                            print(
                                f"Seed page (not article): {link} -> discovered {len(discovered)} links"
                            )
                    else:
                        print(f"Skipped (not article): {link} (no links found)")
                    log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                    continue

                if source_key == "cbl" and len((content or "").strip()) < 800:
                    stats["skipped"] += 1
                    bs["skipped"] += 1
                    bs["last_error"] = "thin_content"
                    print(f"Skipped (thin content): {link}")
                    log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                    continue

                if (
                    not article_meta
                    and source_key != "libya_observer"
                    and not source.get("allow_seed_without_article_meta")
                    and not source.get("allow_without_article_meta")
                ):
                    stats["skipped"] += 1
                    bs["skipped"] += 1
                    print(f"Skipped (not article meta): {link}")
                    log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                    continue

                junk = False
                if not title or len((content or "").strip()) < 600:
                    junk = True
                if link_density > 0.02 and len((content or "").strip()) < 1200:
                    junk = True
                allow_partial = source_key == "unsmil"
                blocked_text = allow_partial and (
                    _is_blocked_text(html) or _is_blocked_text(content)
                )
                if junk and not used_teaser:
                    if allow_partial and (blocked_text or not (content or "").strip()):
                        junk = False
                    else:
                        stats["skipped"] += 1
                        bs["skipped"] += 1
                        bs["junk_saved"] += 1
                        print(f"Skipped (junk): {link}")
                        log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                        continue

                try:
                    source_uuid = get_source_id(sb, source_key)
                except ValueError:
                    stats["skipped"] += 1
                    bs["skipped"] += 1
                    print(f"Skipped (source not seeded in sources table): {source_key}")
                    log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                    continue

                if not title:
                    slug = urlparse(link).path.rstrip("/").split("/")[-1]
                    title = slug.replace("-", " ").strip() or link

                content_len = len((content or "").strip())
                content_kind = "full"
                verification_status = "full"
                fetch_quality = 80
                if used_teaser:
                    content_kind = "teaser"
                    verification_status = "partial"
                    fetch_quality = 30
                elif blocked_text:
                    content_kind = "title_only"
                    verification_status = "blocked"
                    fetch_quality = 0
                elif not (content or "").strip():
                    content_kind = "title_only"
                    verification_status = "blocked"
                    fetch_quality = 0

                summary = ""
                print(f"SUMMARY_GATE do_summary={DO_SUMMARY} url={link}")

                norm_link = _normalize_url(link)
                existing_feed = {}
                if _quality_rank(content_kind) < 3:
                    existing_feed = _get_existing_quality(sb, "feed_items", norm_link)
                    existing_kind = existing_feed.get("content_kind")
                    existing_content = existing_feed.get("content") or ""
                    existing_short = source_key == "unsmil" and (
                        len(existing_content.strip()) < UNSMIL_MIN_FULL_LEN
                        or _is_blocked_text(existing_content)
                    )
                    if (
                        _quality_rank(existing_kind) > _quality_rank(content_kind)
                        and not existing_short
                    ):
                        content_kind = existing_feed.get("content_kind") or content_kind
                        verification_status = (
                            existing_feed.get("verification_status") or verification_status
                        )
                        fetch_quality = existing_feed.get("fetch_quality") or fetch_quality
                        if existing_content:
                            content = existing_content
                        if existing_feed.get("title"):
                            title = existing_feed.get("title")
                        if existing_feed.get("summary"):
                            summary = existing_feed.get("summary") or summary

                feed_item = {
                    "source_id": source_uuid,
                    "source_type": "article",
                    "external_id": norm_link,
                    "url": norm_link,
                    "title": title,
                    "summary": summary,
                    "content": content,
                    "content_kind": content_kind,
                    "verification_status": verification_status,
                    "fetch_quality": fetch_quality,
                    "language": infer_language(link),
                    "published_at": extract_published_at(html),
                    "raw": {"source": source, "url": link},
                }

                phase_ts = time.monotonic()
                upsert_feed_item(sb, feed_item)
                db_ms = int((time.monotonic() - phase_ts) * 1000)

                try:
                    content_hash_val = _content_hash_str(content)
                    content_changed = True
                    is_existing = False
                    if source_key == "libya_observer":
                        is_existing = norm_link in strat_existing_set
                        existing_info = strat_existing_info.get(norm_link)
                        if existing_info:
                            existing_hash = existing_info.get("content_hash")
                            if not existing_hash and existing_info.get("content") is not None:
                                existing_hash = _content_hash_str(existing_info.get("content") or "")
                            if existing_hash and existing_hash == content_hash_val:
                                content_changed = False
                    set_summary_pending = True
                    if source_key == "libya_observer" and is_existing and not content_changed:
                        set_summary_pending = False
                    article_id = None
                    existing_article = {}
                    if _quality_rank(content_kind) < 3:
                        existing_article = _get_existing_quality(sb, "articles", norm_link)
                        existing_kind = existing_article.get("content_kind")
                        existing_content = existing_article.get("content") or ""
                        existing_short = source_key == "unsmil" and (
                            len(existing_content.strip()) < UNSMIL_MIN_FULL_LEN
                            or _is_blocked_text(existing_content)
                        )
                        if (
                            _quality_rank(existing_kind) > _quality_rank(content_kind)
                            and not existing_short
                        ):
                            content_kind = existing_article.get("content_kind") or content_kind
                            verification_status = (
                                existing_article.get("verification_status") or verification_status
                            )
                            fetch_quality = existing_article.get("fetch_quality") or fetch_quality
                            if existing_content:
                                content = existing_content
                            if existing_article.get("title"):
                                title = existing_article.get("title")
                            if existing_article.get("summary"):
                                summary = existing_article.get("summary") or summary

                    article_row = {
                        "source": source_key,
                        "title": title,
                        "url": norm_link,
                        "published_at": feed_item.get("published_at"),
                        "content": content,
                        "summary": summary,
                        "language": feed_item.get("language"),
                        "credibility": infer_credibility(source.get("type")),
                    }
                    if _article_has_column(sb, "source_id"):
                        article_row["source_id"] = source_key
                    if _article_has_column(sb, "source_name"):
                        article_row["source_name"] = source.get("name") or source_key
                    if _article_has_column(sb, "content_kind"):
                        article_row["content_kind"] = content_kind
                    if _article_has_column(sb, "verification_status"):
                        article_row["verification_status"] = verification_status
                    if _article_has_column(sb, "fetch_quality"):
                        article_row["fetch_quality"] = fetch_quality
                    if set_summary_pending:
                        article_row["summary_status"] = "PENDING"
                    if source_key == "libya_observer" and _article_has_column(sb, "last_seen_at"):
                        article_row["last_seen_at"] = datetime.now(timezone.utc).isoformat()
                    if source_key == "libya_observer" and _article_has_column(sb, "content_hash"):
                        article_row["content_hash"] = content_hash_val
                    if source_key == "unsmil" and content_kind != "full":
                        enqueue_fetch(sb, source_uuid, norm_link, "blocked_html")

                    # Detect insert vs update for accurate per-run accounting.
                    exists = norm_link in existing_url_cache
                    if not exists and norm_link not in missing_url_cache:
                        try:
                            existing = _get_existing_urls(sb, [norm_link])
                            exists = norm_link in existing
                        except Exception:
                            exists = False
                        if exists:
                            existing_url_cache.add(norm_link)
                        else:
                            missing_url_cache.add(norm_link)

                    sb.table("articles").upsert(article_row, on_conflict="url").execute()
                    article_id = get_article_id_by_url(sb, norm_link)
                    if exists:
                        stats["updated_existing"] = stats.get("updated_existing", 0) + 1
                        bs["updated_existing"] = bs.get("updated_existing", 0) + 1
                    else:
                        stats["saved"] += 1
                        bs["saved"] += 1
                        saved_count += 1
                        existing_url_cache.add(norm_link)
                except Exception as e:
                    stats["failed"] += 1
                    bs["failed"] += 1
                    bs["last_error"] = f"article_upsert_error:{type(e).__name__}"
                    print(f"Article upsert failed for {link}: {e}")
                    continue
                stats["summary_skipped"] += 1
                bs["summary_skipped"] += 1
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
                if exists:
                    print(f"Updated: {link}")
                else:
                    print(f"Saved: {link}")
                if PROGRESS_EVERY and processed % PROGRESS_EVERY == 0:
                    elapsed = int(time.monotonic() - started_ts)
                    print(
                        f"Progress {processed}: saved={stats['saved']} "
                        f"failed={stats['failed']} blocked={stats['blocked']} "
                        f"llm_failed={stats['llm_failed']} elapsed={elapsed}s"
                    )
                log_timing(link, t0, fetch_ms, parse_ms, summarize_ms, db_ms)
                _maybe_delay()
            if terminate:
                aborted = True
                error_msg = "terminated"
                break
            bs = _bs(source_key)
            blocked_rate = 0.0
            if bs.get("attempted", 0):
                blocked_rate = bs.get("blocked", 0) / float(bs.get("attempted", 0))
            blocked_thresh = float(os.getenv("BLOCKED_RATE_THRESHOLD") or 0.3)
            if bs.get("new_candidates", 0) > 0 and bs.get("attempted", 0) == 0:
                bs["degraded"] = True
                bs["degraded_reason"] = "fetch_no_attempts"
                bs["fetch_degraded"] = True
                print(
                    f"FETCH_DEGRADED source={source_key} reason=no_attempts_with_new "
                    f"new={bs.get('new_candidates', 0)}"
                )
            elif blocked_rate >= blocked_thresh and bs.get("attempted", 0) >= 5:
                bs["degraded"] = True
                bs["degraded_reason"] = "blocked_rate"
                bs["fetch_degraded"] = True
                print(
                    f"FETCH_DEGRADED source={source_key} blocked_rate={blocked_rate:.2f} "
                    f"attempted={bs.get('attempted', 0)} blocked={bs.get('blocked', 0)}"
                )
            print(
                f"FETCH_STATS source={source_key} attempted={bs.get('attempted', 0)} "
                f"saved={bs.get('saved', 0)} failed={bs.get('failed', 0)} "
                f"blocked={bs.get('blocked', 0)} skipped_non_article={bs.get('skipped_non_article', 0)} "
                f"skipped_existing_fetch={bs.get('skipped_existing_fetch', 0)}"
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

    if "discovered" in stats:
        ratio = stats["discovered"] / max(1, stats["total"])
        print(f"DISCOVERY RATIO: {ratio}")
    return 0 if not aborted else 130


if __name__ == "__main__":
    raise SystemExit(main())

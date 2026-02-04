import os
import random
import time
from typing import Optional
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from runner.ingest.extract import extract_main_text


SOURCE_KEY = os.getenv("UNSMIL_SOURCE_KEY", "unsmil")
MAX_ATTEMPTS = int(os.getenv("UNSMIL_PW_MAX_ATTEMPTS", "5"))
MIN_CONTENT_LEN = int(os.getenv("UNSMIL_PW_MIN_CONTENT_LEN", "2000"))
MIN_ACCEPT_LEN = int(os.getenv("UNSMIL_PW_MIN_ACCEPT_LEN", "600"))
MAX_JOBS = int(os.getenv("UNSMIL_PW_MAX_JOBS", "10"))
SLEEP_SEC = float(os.getenv("UNSMIL_PW_SLEEP_SEC", "2"))
PAGE_TIMEOUT_MS = int(os.getenv("UNSMIL_PW_PAGE_TIMEOUT_MS", "45000"))

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


def _get_db_url() -> str:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL missing")
    return dsn


def _get_key_column(cur) -> str:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'sources'
        """
    )
    rows = cur.fetchall()
    cols = {
        (row["column_name"] if isinstance(row, dict) else row[0]) for row in rows
    }
    if "key" in cols:
        return "key"
    if "source_key" in cols:
        return "source_key"
    raise RuntimeError("sources table missing key/source_key")


def _get_source_id(cur, key: str) -> str:
    key_col = _get_key_column(cur)
    cur.execute(f"SELECT id FROM sources WHERE {key_col} = %s LIMIT 1", (key,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Unknown source key: {key}")
    return row["id"] if isinstance(row, dict) else row[0]


def _claim_next(cur, source_id: str) -> Optional[dict]:
    cur.execute(
        """
        UPDATE fetch_queue
        SET status = 'running',
            attempts = attempts + 1
        WHERE id = (
            SELECT id
            FROM fetch_queue
            WHERE status = 'queued'
              AND next_run_at <= now()
              AND source_id = %s
            ORDER BY next_run_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, url, attempts
        """,
        (source_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    if isinstance(row, dict):
        return {
            "id": row["id"],
            "url": row["url"],
            "attempts": row["attempts"],
        }
    return {"id": row[0], "url": row[1], "attempts": row[2]}


def _update_queue_done(cur, queue_id: int) -> None:
    cur.execute(
        "UPDATE fetch_queue SET status = 'done', last_error = NULL WHERE id = %s",
        (queue_id,),
    )


def _update_queue_fail(cur, queue_id: int, attempts: int, error: str) -> None:
    if attempts >= MAX_ATTEMPTS:
        cur.execute(
            "UPDATE fetch_queue SET status = 'dead', last_error = %s WHERE id = %s",
            (error, queue_id),
        )
        return
    backoffs = [600, 1800, 7200, 43200]
    delay = backoffs[min(attempts - 1, len(backoffs) - 1)]
    if error.startswith("blocked_html:retry_after="):
        try:
            ra = int(error.split("=", 1)[1])
            delay = max(ra, 3600)
        except Exception:
            delay = max(delay, 3600)
    cur.execute(
        """
        UPDATE fetch_queue
        SET status = 'queued',
            last_error = %s,
            next_run_at = now() + (%s || ' seconds')::interval
        WHERE id = %s
        """,
        (error, delay, queue_id),
    )


def _update_content(cur, url: str, title: str, content: str, quality: int) -> None:
    cur.execute(
        """
        UPDATE feed_items
        SET content = %s,
            title = COALESCE(NULLIF(title, ''), %s),
            content_kind = 'full',
            verification_status = 'full',
            fetch_quality = %s
        WHERE url = %s
        """,
        (content, title, quality, url),
    )
    cur.execute(
        """
        UPDATE articles
        SET content = %s,
            title = COALESCE(NULLIF(title, ''), %s),
            content_kind = 'full',
            verification_status = 'full',
            fetch_quality = %s
        WHERE url = %s
        """,
        (content, title, quality, url),
    )


def _fetch_with_playwright(url: str) -> tuple[str, str, str, dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--no-zygote",
            ],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0",
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        req_html = ""
        meta = {"status": None, "retry_after": None, "content_length": None}
        try:
            resp = context.request.get(url, timeout=PAGE_TIMEOUT_MS)
            meta["status"] = resp.status
            meta["retry_after"] = resp.headers.get("retry-after")
            meta["content_length"] = resp.headers.get("content-length")
            if resp.ok:
                req_html = resp.text()
        except Exception:
            req_html = ""

        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT_MS)
        html = page.content()
        if len(req_html) > len(html):
            html = req_html
        title = page.title()
        body_text = ""
        for selector in (
            "div.field--name-body",
            "div.field--type-text-with-summary",
            "div.field--type-text-long",
            "article",
            "main",
        ):
            try:
                locator = page.locator(selector)
                if locator.count() > 0:
                    candidate = locator.first.inner_text(timeout=2000)
                    if candidate and candidate.strip():
                        body_text = candidate
                        break
            except Exception:
                continue
        try:
            print(f"UNSMIL_PW_BROWSER path={browser.executable_path}")
        except Exception:
            pass
        browser.close()
    return html, title, body_text, meta


def _extract_title(soup: BeautifulSoup, fallback: str) -> str:
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(" ", strip=True)
        if text:
            return text
    return fallback or ""


def _is_blocked_text(text: str | None) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(marker in lower for marker in _BLOCK_MARKERS)


def run_once() -> int:
    dsn = _get_db_url()
    with psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            source_id = _get_source_id(cur, SOURCE_KEY)

    with psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        conn.autocommit = True
        handled = 0
        while True:
            if handled >= MAX_JOBS:
                break
            with conn.cursor() as cur:
                job = _claim_next(cur, source_id)
            if not job:
                break

            url = job["url"]
            attempts = int(job["attempts"])
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT content_kind FROM articles WHERE url = %s LIMIT 1",
                        (url,),
                    )
                    row = cur.fetchone()
                    if row and row.get("content_kind") == "full":
                        _update_queue_done(cur, job["id"])
                        print(f"UNSMIL_PW_SKIP_FULL url={url}")
                        handled += 1
                        time.sleep(SLEEP_SEC + random.uniform(0, 1))
                        continue
                html, page_title, pw_body, meta = _fetch_with_playwright(url)
                if (
                    meta.get("content_length") in ("0", 0)
                    and meta.get("retry_after")
                ):
                    raise RuntimeError(
                        f"blocked_html:retry_after={meta.get('retry_after')}"
                    )
                if not html or _is_blocked_text(html):
                    raise RuntimeError("blocked_html")
                soup = BeautifulSoup(html, "html.parser")
                title = _extract_title(soup, page_title)
                content = extract_main_text(html)
                if pw_body and len(pw_body) > len(content):
                    content = pw_body
                body = (
                    soup.select_one("div.field--name-body")
                    or soup.select_one("div.field--type-text-with-summary")
                    or soup.select_one("div.field--type-text-long")
                    or soup.select_one("article")
                )
                body_text = body.get_text(" ", strip=True) if body else ""
                if len(body_text) > len(content):
                    content = body_text
                content_len = len(content.strip())
                if _is_blocked_text(content):
                    raise RuntimeError("blocked_html")
                if content_len < MIN_ACCEPT_LEN:
                    print(
                        f"UNSMIL_PW_SHORT url={url} html={len(html)} body={len(body_text)} pw_body={len(pw_body or '')} extract={len(content)}"
                    )
                    raise RuntimeError("content_too_short")
                quality = 90 if content_len >= MIN_CONTENT_LEN else 70
                with conn.cursor() as cur:
                    _update_content(cur, url, title, content, quality)
                    _update_queue_done(cur, job["id"])
                    print(
                        f"UNSMIL_PW_OK url={url} bytes={len(content.encode('utf-8'))}"
                    )
            except Exception as e:
                with conn.cursor() as cur:
                    _update_queue_fail(cur, job["id"], attempts, str(e)[:200])
                    print(
                        f"UNSMIL_PW_FAIL url={url} err={str(e)[:120]} attempts={attempts}"
                    )
            handled += 1
            time.sleep(SLEEP_SEC + random.uniform(0, 1))
    return 0


if __name__ == "__main__":
    raise SystemExit(run_once())

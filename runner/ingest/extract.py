import hashlib
import os
import re
import time
import subprocess
from urllib.parse import urlparse
from typing import Optional

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
    "Referer": "https://libyaobserver.ly/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "close",
}

DEFAULT_CONNECT_TIMEOUT = float(os.getenv("FETCH_CONNECT_TIMEOUT", "10"))
DEFAULT_READ_TIMEOUT = float(os.getenv("FETCH_READ_TIMEOUT", "20"))
FETCH_LOG = os.getenv("FETCH_LOG", "0") == "1"
USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
]

_session = None


def _get_session() -> requests.Session:
    global _session
    if _session is not None:
        return _session

    session = requests.Session()
    retries = Retry(
        total=0,
        status_forcelist=[],
        allowed_methods=["GET", "HEAD"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    _session = session
    return _session


def _rotate_headers(url: str, headers: dict) -> dict:
    merged = dict(headers or {})
    if USER_AGENTS:
        merged["User-Agent"] = USER_AGENTS[hash(url) % len(USER_AGENTS)]
    return merged


RETRY_BACKOFFS = [0, 1, 3, 8]
RETRY_STATUSES = {403, 429, 500, 502, 503, 504}
DNS_BACKOFFS = [2, 4, 8, 15, 30, 60]
FETCH_FALLBACK_CURL = os.getenv("FETCH_FALLBACK_CURL", "0") == "1"


def _resolve_doh(host: str) -> str | None:
    try:
        r = requests.get(
            "https://cloudflare-dns.com/dns-query",
            params={"name": host, "type": "A"},
            headers={"Accept": "application/dns-json"},
            timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
        )
        r.raise_for_status()
        data = r.json()
        for ans in data.get("Answer") or []:
            ip = ans.get("data")
            if ip and re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
                return ip
    except Exception:
        return None
    return None


def _fetch_with_curl_resolve(url: str, host: str, ip: str) -> str | None:
    ua = HEADERS.get("User-Agent")
    args = [
        "curl",
        "-sS",
        "--compressed",
        "-H",
        f"User-Agent: {ua}",
        "-H",
        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "-H",
        "Accept-Language: en-US,en;q=0.9,ar;q=0.8",
        "-H",
        f"Referer: https://{host}/",
        "--resolve",
        f"{host}:443:{ip}",
        url,
    ]
    try:
        proc = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=DEFAULT_READ_TIMEOUT + DEFAULT_CONNECT_TIMEOUT,
        )
        if proc.returncode == 0:
            return proc.stdout
    except Exception:
        return None
    return None


def _is_dns_error(err: Exception) -> bool:
    msg = str(err).lower()
    return "temporary failure in name resolution" in msg or "errno -3" in msg


def fetch_url(url: str, headers: dict) -> tuple[Optional[str], Optional[str]]:
    try:
        session = _get_session()
        merged_headers = _rotate_headers(url, headers)
        last_response = None
        last_exception = None
        for attempt, delay in enumerate(RETRY_BACKOFFS):
            if delay:
                time.sleep(delay)
            start_ts = time.monotonic()
            try:
                response = session.get(
                    url,
                    timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
                    headers=merged_headers,
                )
            except requests.exceptions.RequestException as e:
                last_exception = e
                if _is_dns_error(e):
                    for dns_delay in DNS_BACKOFFS:
                        time.sleep(dns_delay)
                        try:
                            response = session.get(
                                url,
                                timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT),
                                headers=merged_headers,
                            )
                            last_exception = None
                            break
                        except requests.exceptions.RequestException as e2:
                            last_exception = e2
                            if not _is_dns_error(e2):
                                break
                    if last_exception is not None:
                        if FETCH_FALLBACK_CURL:
                            host = urlparse(url).hostname or ""
                            if host:
                                ip = _resolve_doh(host)
                                if ip:
                                    text = _fetch_with_curl_resolve(url, host, ip)
                                    if text is not None:
                                        return text, None
                        return None, "request_error:dns"
                else:
                    if FETCH_LOG:
                        print(f"GET {url} status=error err={type(e).__name__}")
                    return None, f"request_error:{type(e).__name__}"
            last_response = response
            elapsed_ms = int((time.monotonic() - start_ts) * 1000)
            if response.status_code in RETRY_STATUSES:
                retry_after = response.headers.get("retry-after")
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except ValueError:
                        pass
                if FETCH_LOG:
                    print(
                        f"GET {url} status={response.status_code} "
                        f"content-type={response.headers.get('content-type')} "
                        f"bytes={len(response.content)} elapsed={elapsed_ms}ms "
                        f"head={response.text[:120].replace('\\n',' ')}"
                    )
                if attempt < len(RETRY_BACKOFFS) - 1:
                    continue
            if response.status_code in (401, 403, 429):
                return None, f"blocked:{response.status_code}"
            response.raise_for_status()
            if FETCH_LOG:
                print(
                    f"GET {url} status={response.status_code} "
                    f"content-type={response.headers.get('content-type')} "
                    f"bytes={len(response.content)} elapsed={elapsed_ms}ms "
                    f"head={response.text[:120].replace('\\n',' ')}"
                )
            return response.text, None
        if last_response is not None:
            return None, f"request_error:HTTP{last_response.status_code}"
        return None, "request_error:unknown"
    except requests.exceptions.Timeout:
        if FETCH_LOG:
            print(f"GET {url} status=timeout")
        return None, "request_error:timeout"
    except requests.exceptions.RequestException as e:
        if FETCH_LOG:
            print(f"GET {url} status=error err={type(e).__name__}")
        return None, f"request_error:{type(e).__name__}"


def extract_main_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def simple_dedupe_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

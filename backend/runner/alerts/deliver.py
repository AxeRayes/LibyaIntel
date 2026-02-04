import os
import sys
import time
import json
import traceback
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
import psycopg2
import psycopg2.extras


def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and not v:
        raise RuntimeError(f"missing env {name}")
    return v or ""


def read_version() -> str:
    version_file = Path(os.getenv("LIBYAINTEL_VERSION_FILE", "/opt/libyaintel/VERSION"))
    candidates = [version_file]
    try:
        repo_root = Path(__file__).resolve().parents[3]
        candidates.append(repo_root / "VERSION")
    except Exception:
        pass
    for path in candidates:
        try:
            if path.is_file():
                return path.read_text().strip()[:64]
        except Exception:
            continue
    return "unknown"


DB_TIMER = None
HEARTBEAT_FILE = Path(os.getenv("ALERTS_HEARTBEAT_FILE", "/var/lib/libyaintel/alerts_last_ok.txt"))


def touch_heartbeat() -> None:
    try:
        HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = HEARTBEAT_FILE.with_suffix(HEARTBEAT_FILE.suffix + ".tmp")
        tmp_path.write_text(str(int(time.time())))
        os.replace(tmp_path, HEARTBEAT_FILE)
    except Exception:
        pass


def normalize_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
        scheme = parts.scheme.lower() or "http"
        netloc = parts.netloc.lower()
        if ":" in netloc:
            host, port = netloc.rsplit(":", 1)
            if (scheme == "http" and port == "80") or (scheme == "https" and port == "443"):
                netloc = host
        path = parts.path.rstrip("/")
        qs = parse_qsl(parts.query, keep_blank_values=True)
        filtered = []
        for k, v in qs:
            lk = k.lower()
            if lk.startswith("utm_") or lk in {"fbclid", "gclid"}:
                continue
            filtered.append((k, v))
        filtered.sort()
        query = urlencode(filtered, doseq=True)
        return urlunsplit((scheme, netloc, path, query, ""))
    except Exception:
        return raw


def normalize_title(raw: str) -> str:
    raw = (raw or "").lower()
    cleaned = []
    last_space = False
    for ch in raw:
        if ch.isalnum():
            cleaned.append(ch)
            last_space = False
        else:
            if not last_space:
                cleaned.append(" ")
                last_space = True
    return " ".join("".join(cleaned).split())


def compute_dedupe_key(item: Dict[str, Any]) -> Tuple[str, str]:
    url = normalize_url(item.get("url") or "")
    if url:
        return url, url
    source = (item.get("source_name") or item.get("source") or "").strip().lower()
    title = normalize_title(item.get("title") or "")
    ts = item.get("ts") or item.get("published_at") or item.get("created_at")
    day = ""
    try:
        if ts:
            day = str(ts.date())
    except Exception:
        day = ""
    parts = [p for p in [source, title, day] if p]
    key = "|".join(parts) if parts else f"unknown:{item.get('id')}"
    return key, ""


def db_exec(cur, sql: str, params=None) -> None:
    start = time.time()
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)
    dur_ms = int((time.time() - start) * 1000)
    if DB_TIMER:
        DB_TIMER(dur_ms)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def connect_db():
    dsn = env("DATABASE_URL", required=True)
    # psycopg2 understands postgres:// and postgresql://
    return psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor)


def has_column(cur, table: str, col: str) -> bool:
    db_exec(
        cur,
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s AND column_name=%s
        """,
        (table, col),
    )
    return cur.fetchone() is not None


def has_table(cur, table: str) -> bool:
    db_exec(
        cur,
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema='public' AND table_name=%s
        """,
        (table,),
    )
    return cur.fetchone() is not None


def pick_article_ts_expr(has_published_at: bool) -> str:
    if has_published_at:
        return "COALESCE(a.published_at, a.created_at)"
    return "a.created_at"


def pick_search_sql(
    *,
    has_search_tsv: bool,
    ts_expr: str,
    has_category_guess: bool,
    has_category: bool,
    has_source_id: bool,
    has_source: bool,
    has_source_name: bool,
    has_summary: bool,
    has_content_clean: bool,
    has_content: bool,
    has_translated_content: bool,
    has_sources_table: bool,
) -> str:
    """
    Returns SQL for matching articles for a saved search.
    Filters supported:
      - query (FTS if search_tsv exists, else ILIKE)
      - days (lookback window)
      - category (assumes articles.category_guess)
      - source (matches source name or source_id text if needed)
    """
    search_cols: List[str] = []
    if has_summary:
        search_cols.append("a.summary")
    if has_content_clean:
        search_cols.append("a.content_clean")
    if has_content:
        search_cols.append("a.content")
    if has_translated_content:
        search_cols.append("a.translated_content")
    if has_search_tsv:
        # use tsvector
        where_q = "a.search_tsv @@ websearch_to_tsquery('english', %(q)s)"
        rank = "ts_rank_cd(a.search_tsv, websearch_to_tsquery('english', %(q)s))"
        order = f"ORDER BY {rank} DESC, {ts_expr} DESC"
    else:
        if search_cols:
            or_parts = [f"{col} ILIKE %(q_like)s" for col in search_cols]
            where_q = "(" + " OR ".join(or_parts) + ")"
        else:
            where_q = "TRUE"
        order = f"ORDER BY {ts_expr} DESC"

    if has_category_guess:
        category_expr = "a.category_guess"
    elif has_category:
        category_expr = "a.category"
    else:
        category_expr = None

    source_filters: List[str] = []
    if has_source_name:
        source_filters.append("a.source_name ILIKE %(source_like)s")
    if has_source:
        source_filters.append("a.source ILIKE %(source_like)s")
    if has_source_id:
        source_filters.append("a.source_id::text ILIKE %(source_like)s")
    if has_sources_table and has_source_id:
        source_filters.append("s.name ILIKE %(source_like)s")

    if source_filters:
        source_filter_expr = "(" + " OR ".join(source_filters) + ")"
    else:
        source_filter_expr = "TRUE"

    if has_sources_table and has_source_id:
        join_sources = "LEFT JOIN public.sources s ON s.id::text = a.source_id::text"
        source_name_expr = "s.name"
    else:
        join_sources = ""
        source_name_expr = "NULL::text"

    summary_expr = "a.summary" if has_summary else "NULL::text"
    category_select_expr = (
        "a.category_guess"
        if has_category_guess
        else ("a.category" if has_category else "NULL::text")
    )

    if has_search_tsv:
        pass

    category_clause = (
        f"(%(category)s IS NULL OR {category_expr} = %(category)s)"
        if category_expr
        else "(%(category)s IS NULL OR TRUE)"
    )

    return f"""
    SELECT
      a.id,
      a.title,
      a.url,
      {summary_expr} AS summary,
      {category_select_expr} AS category_guess,
      {ts_expr} AS ts,
      {source_name_expr} AS source_name
    FROM public.articles a
    {join_sources}
    WHERE
      {ts_expr} >= COALESCE(%(start_ts)s, (now() - (%(days)s || ' days')::interval))
      AND (%(q_present)s = false OR {where_q})
      AND {category_clause}
      AND (
        %(source)s IS NULL
        OR {source_filter_expr}
      )
    {order}
    LIMIT %(limit)s
    """


def send_email_resend(to_email: str, subject: str, text: str) -> Tuple[bool, str]:
    api_key = env("RESEND_API_KEY", required=True)
    from_email = env("ALERTS_FROM_EMAIL", default="alerts@libyaintel.com", required=True)

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        data=json.dumps({"from": from_email, "to": [to_email], "subject": subject, "text": text}),
        timeout=20,
    )
    if 200 <= resp.status_code < 300:
        return True, ""
    return False, f"resend_status={resp.status_code} body={resp.text[:200]}"


def send_telegram(chat_id: str, text: str) -> Tuple[bool, str]:
    token = env("TELEGRAM_BOT_TOKEN", required=True)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=20)
    if 200 <= resp.status_code < 300:
        return True, ""
    return False, f"telegram_status={resp.status_code} body={resp.text[:200]}"


ADMIN_NOTIFY_STATE_FILE = Path("/var/lib/libyaintel/alerts_admin_notify.json")


def admin_notify_cooldown_sec() -> int:
    return int(os.getenv("ALERTS_ADMIN_NOTIFY_COOLDOWN_SEC", "3600"))


def admin_notify_giveup_cooldown_sec() -> int:
    return int(os.getenv("ALERTS_ADMIN_NOTIFY_GIVEUP_COOLDOWN_SEC", "3600"))


def admin_notify_backlog_cooldown_sec() -> int:
    return int(os.getenv("ALERTS_ADMIN_NOTIFY_BACKLOG_COOLDOWN_SEC", "1800"))


def admin_notify_enabled() -> bool:
    return bool(
        (os.getenv("ALERTS_ADMIN_TELEGRAM_BOT_TOKEN") or "").strip()
        and (os.getenv("ALERTS_ADMIN_TELEGRAM_CHAT_ID") or "").strip()
    ) or bool((os.getenv("ALERTS_ADMIN_EMAILS") or "").strip())


def admin_should_notify(key: str) -> bool:
    cooldown = admin_notify_cooldown_sec()
    now = int(time.time())

    try:
        data = json.loads(ADMIN_NOTIFY_STATE_FILE.read_text())
    except Exception:
        data = {}

    last = int(data.get(key, 0))
    if now - last < cooldown:
        return False

    data[key] = now
    ADMIN_NOTIFY_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    ADMIN_NOTIFY_STATE_FILE.write_text(json.dumps(data))
    return True


def admin_should_notify_with_cooldown(key: str, cooldown: int) -> bool:
    now = int(time.time())
    try:
        data = json.loads(ADMIN_NOTIFY_STATE_FILE.read_text())
    except Exception:
        data = {}

    last = int(data.get(key, 0))
    if now - last < cooldown:
        return False

    data[key] = now
    ADMIN_NOTIFY_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    ADMIN_NOTIFY_STATE_FILE.write_text(json.dumps(data))
    return True


def send_admin_telegram(text: str) -> Tuple[bool, str]:
    token = (os.getenv("ALERTS_ADMIN_TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("ALERTS_ADMIN_TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        return False, "not_configured"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=10)
    if 200 <= resp.status_code < 300:
        return True, ""
    return False, f"status_{resp.status_code}"


def send_admin_email(subject: str, text: str) -> Tuple[bool, str]:
    emails_raw = (os.getenv("ALERTS_ADMIN_EMAILS") or "").strip()
    if not emails_raw:
        return False, "not_configured"
    emails = [e.strip() for e in emails_raw.split(",") if e.strip()]
    if not emails:
        return False, "not_configured"
    prefix = (os.getenv("ALERTS_ADMIN_EMAIL_SUBJECT_PREFIX") or "").strip()
    full_subject = f"{prefix} {subject}".strip()
    errors = []
    for email in emails:
        ok, err = send_email_resend(email, full_subject, text)
        if not ok:
            errors.append(err)
    if errors:
        return False, "send_failed"
    return True, ""


def classify_giveup_error(error: str) -> str:
    if not error:
        return "unknown"
    if "send_exception=" in error:
        # format: send_exception=Type:message
        tail = error.split("send_exception=", 1)[1]
        return (tail.split(":", 1)[0] or "exception").strip()
    return (error.split(":", 1)[0] or "error").strip()


def notify_admin_giveup(delivery: Dict[str, Any], error: str, attempt_count: int) -> None:
    if not admin_notify_enabled():
        return
    err_class = classify_giveup_error(error)
    channel = (delivery.get("channel") or "unknown").strip()
    cooldown_key = f"giveup:{channel}:{err_class}"
    if not admin_should_notify_with_cooldown(cooldown_key, admin_notify_giveup_cooldown_sec()):
        return

    target = (delivery.get("target") or "").strip()
    message = (
        "ALERTS_DELIVERY_GIVEUP\n"
        f"alert_id={delivery.get('alert_id')}\n"
        f"user_id={delivery.get('user_id')}\n"
        f"channel={delivery.get('channel')}\n"
        f"target={target}\n"
        f"attempts={attempt_count}\n"
        f"error={error[:200]}\n"
        "check: journalctl -u libyaintel-alerts.service -n 200"
    )

    try:
        ok, err = send_admin_telegram(message)
        print(f"ALERTS_ADMIN_NOTIFY channel=telegram ok={1 if ok else 0} err={err}")
    except Exception:
        print("ALERTS_ADMIN_NOTIFY channel=telegram ok=0 err=exception")
    try:
        ok, err = send_admin_email("ALERTS_DELIVERY_GIVEUP", message)
        print(f"ALERTS_ADMIN_NOTIFY channel=email ok={1 if ok else 0} err={err}")
    except Exception:
        print("ALERTS_ADMIN_NOTIFY channel=email ok=0 err=exception")


def notify_admin_backlog(
    pending_count: int,
    pending_due_count: int,
    oldest_age_sec: int,
    next_due_in_sec: int,
    queued_at_estimated: int,
    warn_count: int,
    warn_age_sec: int,
) -> None:
    if not admin_notify_enabled():
        return
    cooldown_key = "backlog:global"
    if not admin_should_notify_with_cooldown(cooldown_key, admin_notify_backlog_cooldown_sec()):
        return

    message = (
        "ALERTS_BACKLOG_WARN\n"
        f"pending={pending_count}\n"
        f"pending_due={pending_due_count}\n"
        f"oldest_age_sec={oldest_age_sec}\n"
        f"next_due_in_sec={next_due_in_sec}\n"
        f"queued_at_estimated={queued_at_estimated}\n"
        f"warn_count={warn_count}\n"
        f"warn_age_sec={warn_age_sec}\n"
        "check: journalctl -u libyaintel-alerts.service -n 200"
    )
    try:
        ok, err = send_admin_telegram(message)
        print(f"ALERTS_ADMIN_NOTIFY channel=telegram ok={1 if ok else 0} err={err}")
    except Exception:
        print("ALERTS_ADMIN_NOTIFY channel=telegram ok=0 err=exception")
    try:
        ok, err = send_admin_email("ALERTS_BACKLOG_WARN", message)
        print(f"ALERTS_ADMIN_NOTIFY channel=email ok={1 if ok else 0} err={err}")
    except Exception:
        print("ALERTS_ADMIN_NOTIFY channel=email ok=0 err=exception")

def format_message(item: Dict[str, Any]) -> str:
    # Keep it simple and readable.
    title = (item.get("title") or "").strip()
    source = (item.get("source_name") or "").strip()
    cat = (item.get("category_guess") or "").strip()
    url = (item.get("url") or "").strip()
    summary = (item.get("summary") or "").strip()

    lines = []
    lines.append(title)
    meta = " | ".join([x for x in [source, cat] if x])
    if meta:
        lines.append(meta)
    if summary:
        lines.append("")
        lines.append(summary[:600])
    if url:
        lines.append("")
        lines.append(url)
    return "\n".join(lines).strip()


def format_digest(items: List[Dict[str, Any]]) -> str:
    header = f"{len(items)} new items from LibyaIntel"
    blocks = [format_message(item) for item in items]
    body = "\n\n---\n\n".join(blocks)
    return f"{header}\n\n{body}".strip()


def format_grouped_digest(groups: Dict[str, List[Dict[str, Any]]]) -> str:
    total_items = sum(len(items) for items in groups.values())
    header = f"{total_items} new items from LibyaIntel"
    blocks = []
    for items in groups.values():
        first = items[0]
        title = (first.get("title") or "").strip()
        url = (first.get("url") or "").strip()
        summary = (first.get("summary") or "").strip()
        sources = []
        seen_sources = set()
        for item in items:
            src = (item.get("source_name") or "").strip()
            key = src.lower()
            if src and key not in seen_sources:
                seen_sources.add(key)
                sources.append(src)
        lines = [title] if title else []
        if sources:
            shown = sources[:3]
            extra = len(sources) - len(shown)
            src_line = "Sources: " + ", ".join(shown)
            if extra > 0:
                src_line += f" (+{extra} more)"
            lines.append(src_line)
        if summary:
            lines.append("")
            lines.append(summary[:600])
        if url:
            lines.append("")
            lines.append(url)
        blocks.append("\n".join(lines).strip())
    body = "\n\n---\n\n".join(blocks)
    return f"{header}\n\n{body}".strip()


def log_delivery_giveup(delivery: Dict[str, Any], error: str, attempt_count: int) -> None:
    target = (delivery.get("target") or "").strip()
    print(
        "ALERTS_DELIVERY_GIVEUP alert_id=%s user_id=%s channel=%s target=%s attempts=%d error=%s"
        % (
            delivery.get("alert_id"),
            delivery.get("user_id"),
            delivery.get("channel"),
            target,
            attempt_count,
            error[:200],
        )
    )
    try:
        notify_admin_giveup(delivery, error, attempt_count)
    except Exception:
        pass


def pick_article_by_id_sql(
    *,
    has_category_guess: bool,
    has_category: bool,
    has_source_id: bool,
    has_source: bool,
    has_source_name: bool,
    has_summary: bool,
    has_content_clean: bool,
    has_content: bool,
    has_translated_content: bool,
    has_sources_table: bool,
) -> str:
    if has_sources_table and has_source_id:
        join_sources = "LEFT JOIN public.sources s ON s.id::text = a.source_id::text"
        source_name_expr = "s.name"
    else:
        join_sources = ""
        source_name_expr = "NULL::text"

    summary_expr = "a.summary" if has_summary else "NULL::text"
    category_select_expr = (
        "a.category_guess"
        if has_category_guess
        else ("a.category" if has_category else "NULL::text")
    )

    return f"""
    SELECT
      a.id,
      a.title,
      a.url,
      {summary_expr} AS summary,
      {category_select_expr} AS category_guess,
      {source_name_expr} AS source_name
    FROM public.articles a
    {join_sources}
    WHERE a.id = %(article_id)s
    """


def compute_backoff_seconds(attempt_count: int, base: int, max_sec: int) -> int:
    return min(max_sec, base * (2 ** max(attempt_count - 1, 0)))


def priority_for_search(search: Dict[str, Any], p1_categories: List[str]) -> str:
    # TODO: move priority into data model (e.g., saved_search.priority).
    if (search.get("query") or "").strip():
        return "P0"
    category = (search.get("category") or "").strip().lower()
    if category and category in p1_categories:
        return "P1"
    return "P2"


def get_user_prefs(cur, user_id: str, env_p1_categories: List[str]) -> Tuple[Dict[str, Any], str]:
    db_exec(
        cur,
        """
        SELECT
          dedupe_window_sec,
          immediate_priorities,
          digest_priorities,
          priority_categories,
          digest_schedule,
          channels_enabled
        FROM public.user_alert_prefs
        WHERE user_id = %s
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if row:
        prefs = {
            "dedupe_window_sec": int(row.get("dedupe_window_sec") or 21600),
            "immediate_priorities": row.get("immediate_priorities") or ["P0"],
            "digest_priorities": row.get("digest_priorities") or ["P1", "P2"],
            "priority_categories": row.get("priority_categories") or env_p1_categories,
            "digest_schedule": row.get("digest_schedule") or "daily",
            "channels_enabled": row.get("channels_enabled") or ["email"],
        }
        return prefs, "db"
    prefs = {
        "dedupe_window_sec": 21600,
        "immediate_priorities": ["P0"],
        "digest_priorities": ["P1", "P2"],
        "priority_categories": env_p1_categories,
        "digest_schedule": "daily",
        "channels_enabled": ["email"],
    }
    return prefs, "env"


def status_allows_pending(cur) -> bool:
    db_exec(
        cur,
        """
        SELECT pg_get_constraintdef(oid) AS def
        FROM pg_constraint
        WHERE conrelid = 'public.alert_deliveries'::regclass
          AND contype = 'c'
        """
    )
    rows = cur.fetchall()
    for row in rows:
        definition = row.get("def") or ""
        if "status" in definition:
            return "PENDING" in definition
    return True


def mark_delivery_failed(
    *,
    cur,
    delivery: Dict[str, Any],
    error: str,
    retry_base_sec: int,
    retry_max_sec: int,
    max_attempts: int,
) -> None:
    new_attempt = int(delivery["attempt_count"]) + 1
    backoff = compute_backoff_seconds(new_attempt, retry_base_sec, retry_max_sec)
    db_exec(
        cur,
        """
        UPDATE public.alert_deliveries
        SET status='FAILED',
            error=%s,
            attempt_count=attempt_count+1,
            last_attempt_at=now(),
            next_attempt_at=now() + (%s || ' seconds')::interval
        WHERE alert_id=%s AND article_id=%s AND channel=%s
        """,
        (
            error[:500],
            backoff,
            delivery["alert_id"],
            delivery["article_id"],
            delivery["channel"],
        ),
    )
    if new_attempt >= max_attempts:
        log_delivery_giveup(delivery, error, new_attempt)


def mark_delivery_sent(*, cur, delivery: Dict[str, Any]) -> None:
    db_exec(
        cur,
        """
        UPDATE public.alert_deliveries
        SET status='SENT',
            error=NULL,
            delivered_at=now(),
            attempt_count=attempt_count+1,
            last_attempt_at=now(),
            next_attempt_at=now()
        WHERE alert_id=%s AND article_id=%s AND channel=%s
        """,
        (delivery["alert_id"], delivery["article_id"], delivery["channel"]),
    )


def run_once() -> int:
    start = time.time()
    sent = 0
    failed = 0
    checked_alerts = 0
    queued = 0
    skipped = 0
    send_ms = 0
    db_ms = 0

    max_per_user = int(env("ALERTS_MAX_PER_USER", "25"))
    max_attempts = int(env("ALERTS_MAX_ATTEMPTS", "5"))
    retry_base_sec = int(env("ALERTS_RETRY_BASE_SEC", "60"))
    retry_max_sec = int(env("ALERTS_RETRY_MAX_SEC", "3600"))
    cursor_overlap_sec = int(env("ALERTS_CURSOR_OVERLAP_SEC", "86400"))
    advisory_lock_key = int(env("ALERTS_ADVISORY_LOCK_KEY", "743829113"))
    max_items_per_email = int(env("ALERTS_MAX_ITEMS_PER_EMAIL", "50"))
    backlog_warn_count = int(env("ALERTS_BACKLOG_WARN_COUNT", "200"))
    backlog_warn_age_sec = int(env("ALERTS_BACKLOG_WARN_AGE_SEC", "3600"))
    dedupe_window_sec = int(env("ALERTS_DEDUPE_WINDOW_SEC", "21600"))
    p1_categories = [
        x.strip().lower()
        for x in env("ALERTS_PRIORITY_CATEGORIES", "").split(",")
        if x.strip()
    ]
    match_limit = max(max_per_user, max_items_per_email)

    try:
        with connect_db() as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                def add_db_ms(delta_ms: int) -> None:
                    nonlocal db_ms
                    db_ms += delta_ms

                global DB_TIMER
                DB_TIMER = add_db_ms

                db_exec(cur, "SELECT pg_try_advisory_lock(%s) AS locked", (advisory_lock_key,))
                got_lock = cur.fetchone()
                if not got_lock or not got_lock.get("locked"):
                    print("ALERTS_SKIP reason=advisory_lock_busy")
                    return 0

                # Detect columns safely
                has_published_at = has_column(cur, "articles", "published_at")
                has_search_tsv = has_column(cur, "articles", "search_tsv")
                has_category_guess = has_column(cur, "articles", "category_guess")
                has_category = has_column(cur, "articles", "category")
                has_source_id = has_column(cur, "articles", "source_id")
                has_source = has_column(cur, "articles", "source")
                has_source_name = has_column(cur, "articles", "source_name")
                has_summary = has_column(cur, "articles", "summary")
                has_content_clean = has_column(cur, "articles", "content_clean")
                has_content = has_column(cur, "articles", "content")
                has_translated_content = has_column(cur, "articles", "translated_content")
                has_sources_table = has_table(cur, "sources")

                # Ensure minimum columns exist
                for needed in ["id", "created_at"]:
                    if not has_column(cur, "articles", needed):
                        raise RuntimeError(f"articles.{needed} missing")

                # Ensure delivery cursor table exists
                if not has_table(cur, "alert_delivery_cursors"):
                    raise RuntimeError("alert_delivery_cursors table missing (run migration)")

                # Ensure retry columns exist
                for needed in [
                    "attempt_count",
                    "last_attempt_at",
                    "next_attempt_at",
                    "queued_at",
                    "queued_at_is_estimated",
                    "created_at",
                    "dedupe_key",
                    "dedupe_group",
                    "normalized_url",
                    "priority",
                ]:
                    if not has_column(cur, "alert_deliveries", needed):
                        raise RuntimeError(f"alert_deliveries.{needed} missing (run migration)")

                # Ensure queued_at is fully backfilled
                db_exec(
                    cur,
                    """
                    SELECT COUNT(*) AS nulls
                    FROM public.alert_deliveries
                    WHERE queued_at IS NULL
                    """,
                )
                null_row = cur.fetchone() or {}
                if int(null_row.get("nulls") or 0) > 0:
                    raise RuntimeError(
                        "alert_deliveries.queued_at has NULLs. "
                        "Run: psql \"$DATABASE_URL\" -f /home/akram/libyaintel/alerts_delivery_migration.sql "
                        "then: psql \"$DATABASE_URL\" -f /home/akram/libyaintel/scripts/verify_alerts_schema.sql"
                    )

                if not status_allows_pending(cur):
                    raise RuntimeError("alert_deliveries.status does not allow PENDING (run migration)")

                ts_expr = pick_article_ts_expr(has_published_at)
                search_sql = pick_search_sql(
                    has_search_tsv=has_search_tsv,
                    ts_expr=ts_expr,
                    has_category_guess=has_category_guess,
                    has_category=has_category,
                    has_source_id=has_source_id,
                    has_source=has_source,
                    has_source_name=has_source_name,
                    has_summary=has_summary,
                    has_content_clean=has_content_clean,
                    has_content=has_content,
                    has_translated_content=has_translated_content,
                    has_sources_table=has_sources_table,
                )
                article_by_id_sql = pick_article_by_id_sql(
                    has_category_guess=has_category_guess,
                    has_category=has_category,
                    has_source_id=has_source_id,
                    has_source=has_source,
                    has_source_name=has_source_name,
                    has_summary=has_summary,
                    has_content_clean=has_content_clean,
                    has_content=has_content,
                    has_translated_content=has_translated_content,
                    has_sources_table=has_sources_table,
                )

                # Fetch active alerts joined with saved search
                db_exec(
                    cur,
                    """
                    SELECT
                      a.id AS alert_id,
                      a.user_id,
                      a.channel,
                      a.target,
                      a.saved_search_id,
                      ss.name,
                      COALESCE(ss.query,'') AS query,
                      COALESCE(ss.days, 7) AS days,
                      NULLIF(ss.category,'') AS category,
                      NULLIF(ss.source,'') AS source
                    FROM public.alerts a
                    JOIN public.saved_searches ss ON ss.id = a.saved_search_id
                    WHERE a.active = true
                    ORDER BY a.id
                    """
                )
                alerts = cur.fetchall()
                checked_alerts = len(alerts)
                priority_counts = {"P0": 0, "P1": 0, "P2": 0}
                user_prefs_cache: Dict[str, Tuple[Dict[str, Any], str]] = {}

                per_user_attempts: Dict[str, int] = {}
                for al in alerts:
                    user_id = str(al["user_id"])
                    if user_id not in user_prefs_cache:
                        prefs, prefs_source = get_user_prefs(cur, user_id, p1_categories)
                        user_prefs_cache[user_id] = (prefs, prefs_source)
                        print(
                            "ALERTS_PREFS user_id=%s source=%s dedupe_window=%d immediate=%s digest=%s categories=%s"
                            % (
                                user_id,
                                prefs_source,
                                prefs["dedupe_window_sec"],
                                ",".join(prefs["immediate_priorities"]),
                                ",".join(prefs["digest_priorities"]),
                                ",".join(prefs["priority_categories"] or []),
                            )
                        )
                    prefs, _ = user_prefs_cache[user_id]
                    q = (al["query"] or "").strip()
                    q_present = bool(q)
                    db_exec(
                        cur,
                        """
                        SELECT last_ts
                        FROM public.alert_delivery_cursors
                        WHERE alert_id = %s
                        """,
                        (al["alert_id"],),
                    )
                    cursor_row = cur.fetchone()
                    start_ts = None
                    if cursor_row and cursor_row.get("last_ts"):
                        start_ts = cursor_row["last_ts"] - timedelta(seconds=cursor_overlap_sec)
                    params = {
                        "q": q,
                        "q_like": f"%{q}%",
                        "q_present": q_present,
                        "days": int(al["days"] or 7),
                        "category": al["category"],
                        "source": al["source"],
                        "source_like": f"%{al['source']}%" if al["source"] else None,
                        "limit": match_limit,
                        "start_ts": start_ts,
                    }

                    db_exec(cur, search_sql, params)
                    matches = cur.fetchall()
                    max_ts = None

                    search_priority = priority_for_search(al, prefs["priority_categories"])
                    dedupe_rows: List[Tuple[Dict[str, Any], str, str]] = []
                    for item in matches:
                        item_ts = item.get("ts")
                        if item_ts and (max_ts is None or item_ts > max_ts):
                            max_ts = item_ts
                        dedupe_key, normalized_url = compute_dedupe_key(item)
                        if env("ALERTS_DEDUPE_DEBUG", "0") == "1" and len(dedupe_rows) < 5:
                            print(
                                "ALERTS_DEDUPE_KEY_PREVIEW article_id=%s dedupe_key=%s"
                                % (item.get("id"), dedupe_key[:160])
                            )
                        dedupe_rows.append((item, dedupe_key, normalized_url))

                    recent_keys = set()
                    if dedupe_rows:
                        keys = [k for _, k, _ in dedupe_rows]
                        print("ALERTS_DEDUPE_KEYS count=%d" % len(keys))
                        db_exec(
                            cur,
                            """
                            SELECT dedupe_key
                            FROM public.alert_deliveries
                            WHERE user_id = %s
                              AND channel = %s
                              AND dedupe_key = ANY(%s)
                              AND created_at >= now() - (%s || ' seconds')::interval
                              AND status IN ('PENDING','FAILED','SENT')
                            """,
                            (al["user_id"], al["channel"], keys, prefs["dedupe_window_sec"]),
                        )
                        recent_keys = {row["dedupe_key"] for row in cur.fetchall()}

                    for item, dedupe_key, normalized_url in dedupe_rows:
                        if dedupe_key in recent_keys:
                            print(
                                "ALERTS_DEDUPE_SKIP user_id=%s channel=%s dedupe_key=%s reason=recent_duplicate"
                                % (al["user_id"], al["channel"], dedupe_key[:120])
                            )
                            continue
                        if al["channel"] not in prefs["channels_enabled"]:
                            print(
                                "ALERTS_CHANNEL_SKIP user_id=%s channel=%s reason=disabled"
                                % (al["user_id"], al["channel"])
                            )
                            continue
                        db_exec(
                            cur,
                            """
                            INSERT INTO public.alert_deliveries (
                              alert_id, user_id, article_id, channel, status,
                              attempt_count, next_attempt_at, queued_at,
                              dedupe_key, dedupe_group, normalized_url, priority, created_at
                            )
                            VALUES (%s, %s, %s, %s, 'PENDING', 0, now(), now(),
                                    %s, %s, %s, %s, now())
                            ON CONFLICT (alert_id, article_id, channel) DO NOTHING
                            RETURNING id
                            """,
                            (
                                al["alert_id"],
                                al["user_id"],
                                item["id"],
                                al["channel"],
                                dedupe_key,
                                dedupe_key,
                                normalized_url or None,
                                search_priority,
                            ),
                        )
                        claimed = cur.fetchone()
                        if not claimed:
                            skipped += 1
                            continue
                        queued += 1
                        priority_counts[search_priority] = priority_counts.get(search_priority, 0) + 1

                    conn.commit()
                    if max_ts:
                        db_exec(
                            cur,
                            """
                            INSERT INTO public.alert_delivery_cursors (alert_id, last_ts, updated_at)
                            VALUES (%s, %s, now())
                            ON CONFLICT (alert_id)
                            DO UPDATE SET last_ts = GREATEST(alert_delivery_cursors.last_ts, EXCLUDED.last_ts),
                                          updated_at = now()
                            """,
                            (al["alert_id"], max_ts),
                        )
                        conn.commit()

                    db_exec(
                        cur,
                        """
                        SELECT d.alert_id, d.user_id, d.article_id, d.channel, d.attempt_count, a.target,
                               d.dedupe_key, d.priority
                        FROM public.alert_deliveries d
                        JOIN public.alerts a ON a.id = d.alert_id
                        WHERE d.alert_id = %s
                          AND d.channel = %s
                          AND d.status IN ('PENDING','FAILED')
                          AND d.next_attempt_at <= now()
                          AND d.attempt_count < %s
                        ORDER BY d.next_attempt_at ASC
                        LIMIT %s
                        """,
                        (al["alert_id"], al["channel"], max_attempts, max_per_user),
                    )
                    due = cur.fetchall()
                    filtered_due: List[Dict[str, Any]] = []
                    for d in due:
                        user_id = str(d["user_id"])
                        per_user_attempts.setdefault(user_id, 0)
                        if per_user_attempts[user_id] >= max_per_user:
                            continue
                        per_user_attempts[user_id] += 1
                        filtered_due.append(d)

                    alert_name = (al.get("name") or "Saved search").strip()

                    if al["channel"] == "email":
                        deliveries_with_items: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
                        for d in filtered_due:
                            if d.get("priority") not in prefs["immediate_priorities"] and d.get("priority") not in prefs[
                                "digest_priorities"
                            ]:
                                continue
                            db_exec(cur, article_by_id_sql, {"article_id": d["article_id"]})
                    item = cur.fetchone()
                            if not item:
                                failed += 1
                                mark_delivery_failed(
                                    cur=cur,
                                    delivery=d,
                                    error="missing_article",
                                    retry_base_sec=retry_base_sec,
                                    retry_max_sec=retry_max_sec,
                                    max_attempts=max_attempts,
                                )
                                conn.commit()
                                continue
                            deliveries_with_items.append((d, item))

                        for i in range(0, len(deliveries_with_items), max_items_per_email):
                            batch = deliveries_with_items[i : i + max_items_per_email]
                            grouped: Dict[str, List[Dict[str, Any]]] = {}
                            for d, item in batch:
                                key = d.get("dedupe_key") or str(item.get("id"))
                                grouped.setdefault(key, []).append(item)
                            batch_items = [items[0] for items in grouped.values()]
                            msg = format_grouped_digest(grouped)
                            print("ALERTS_GROUPED groups=%d items=%d" % (len(grouped), len(batch)))
                            subject = f"LibyaIntel alert: {alert_name} ({len(batch_items)} items)"

                            ok = True
                            err = ""
                            send_start = time.time()
                            try:
                                ok, err = send_email_resend(batch[0][0]["target"], subject, msg)
                            except Exception as e:
                                ok, err = False, f"send_exception={type(e).__name__}:{e}"
                            send_ms += int((time.time() - send_start) * 1000)

                            if ok:
                                for d, _ in batch:
                                    sent += 1
                                    mark_delivery_sent(cur=cur, delivery=d)
                                conn.commit()
                            else:
                                for d, _ in batch:
                                    failed += 1
                                    mark_delivery_failed(
                                        cur=cur,
                                        delivery=d,
                                        error=err,
                                        retry_base_sec=retry_base_sec,
                                        retry_max_sec=retry_max_sec,
                                        max_attempts=max_attempts,
                                    )
                                conn.commit()
                    else:
                        for d in filtered_due:
                            if d.get("priority") not in prefs["immediate_priorities"] and d.get("priority") not in prefs[
                                "digest_priorities"
                            ]:
                                continue
                            db_exec(cur, article_by_id_sql, {"article_id": d["article_id"]})
                            item = cur.fetchone()
                            if not item:
                                failed += 1
                                mark_delivery_failed(
                                    cur=cur,
                                    delivery=d,
                                    error="missing_article",
                                    retry_base_sec=retry_base_sec,
                                    retry_max_sec=retry_max_sec,
                                    max_attempts=max_attempts,
                                )
                                conn.commit()
                                continue

                            msg = format_message(item)
                            subject = f"LibyaIntel alert: {item.get('title','').strip()[:80]}"

                            ok = True
                            err = ""
                            send_start = time.time()
                            try:
                                if d["channel"] == "telegram":
                                    ok, err = send_telegram(d["target"], msg)
                                else:
                                    ok, err = False, f"unsupported_channel={d['channel']}"
                            except Exception as e:
                                ok, err = False, f"send_exception={type(e).__name__}:{e}"
                            send_ms += int((time.time() - send_start) * 1000)

                            if ok:
                                sent += 1
                                mark_delivery_sent(cur=cur, delivery=d)
                                conn.commit()
                            else:
                                failed += 1
                                mark_delivery_failed(
                                    cur=cur,
                                    delivery=d,
                                    error=err,
                                    retry_base_sec=retry_base_sec,
                                    retry_max_sec=retry_max_sec,
                                    max_attempts=max_attempts,
                                )
                                conn.commit()

        pending_count = 0
        pending_due_count = 0
        oldest_age_sec = 0
        next_due_in_sec = 0
        queued_at_estimated = 0
        giveup_count = 0
        db_exec(
            cur,
            """
            SELECT
              COUNT(*) FILTER (WHERE status IN ('PENDING','FAILED') AND attempt_count < %s) AS pending_count,
              COUNT(*) FILTER (
                WHERE status IN ('PENDING','FAILED')
                  AND attempt_count < %s
                  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
              ) AS pending_due_count,
              COALESCE(
                EXTRACT(
                  EPOCH FROM (
                    now() - MIN(queued_at) FILTER (
                      WHERE status IN ('PENDING','FAILED')
                        AND attempt_count < %s
                        AND queued_at_is_estimated = false
                    )
                  )
                ),
                0
              ) AS oldest_age_sec,
              COALESCE(
                EXTRACT(
                  EPOCH FROM (
                    MIN(next_attempt_at) FILTER (
                      WHERE status IN ('PENDING','FAILED')
                        AND attempt_count < %s
                        AND next_attempt_at > now()
                    ) - now()
                  )
                ),
                0
              ) AS next_due_in_sec,
              COUNT(*) FILTER (
                WHERE status IN ('PENDING','FAILED')
                  AND attempt_count < %s
                  AND queued_at_is_estimated = true
              ) AS queued_at_estimated,
              COUNT(*) FILTER (WHERE status = 'FAILED' AND attempt_count >= %s) AS giveup_count
            FROM public.alert_deliveries
            """,
            (max_attempts, max_attempts, max_attempts, max_attempts, max_attempts, max_attempts),
        )
        row = cur.fetchone() or {}
        pending_count = int(row.get("pending_count") or 0)
        pending_due_count = int(row.get("pending_due_count") or 0)
        oldest_age_sec = int(row.get("oldest_age_sec") or 0)
        next_due_in_sec = int(row.get("next_due_in_sec") or 0)
        queued_at_estimated = int(row.get("queued_at_estimated") or 0)
        giveup_count = int(row.get("giveup_count") or 0)

        if pending_due_count >= backlog_warn_count or oldest_age_sec >= backlog_warn_age_sec:
            print(
                "ALERTS_BACKLOG_WARN pending=%d pending_due=%d oldest_age_sec=%d next_due_in_sec=%d queued_at_estimated=%d warn_count=%d warn_age_sec=%d"
                % (
                    pending_count,
                    pending_due_count,
                    oldest_age_sec,
                    next_due_in_sec,
                    queued_at_estimated,
                    backlog_warn_count,
                    backlog_warn_age_sec,
                )
            )
            notify_admin_backlog(
                pending_count,
                pending_due_count,
                oldest_age_sec,
                next_due_in_sec,
                queued_at_estimated,
                backlog_warn_count,
                backlog_warn_age_sec,
            )

        total_ms = int((time.time() - start) * 1000)
        other_ms = max(total_ms - send_ms - db_ms, 0)
        print(
            "ALERTS_HEALTH ok=1 checked=%d queued=%d pending=%d pending_due=%d next_due_in_sec=%d queued_at_estimated=%d giveup=%d oldest_age_sec=%d send_ms=%d db_ms=%d other_ms=%d total_ms=%d"
            % (
                checked_alerts,
                queued,
                pending_count,
                pending_due_count,
                next_due_in_sec,
                queued_at_estimated,
                giveup_count,
                oldest_age_sec,
                send_ms,
                db_ms,
                other_ms,
                total_ms,
            )
        )
        print(
            "ALERTS_PRIORITY_COUNTS p0=%d p1=%d p2=%d"
            % (
                priority_counts.get("P0", 0),
                priority_counts.get("P1", 0),
                priority_counts.get("P2", 0),
            )
        )
        if giveup_count > 0:
            print("ALERTS_GIVEUP_WARN giveup=%d" % giveup_count)
        print(
            "ALERTS_OK checked=%d queued=%d skipped=%d sent=%d failed=%d dur_ms=%d"
            % (checked_alerts, queued, skipped, sent, failed, total_ms)
        )
        touch_heartbeat()
        return 0

    except Exception as e:
        dur_ms = int((time.time() - start) * 1000)
        reason = f"{type(e).__name__}:{e}"
        print(f"ALERTS_FAIL checked={checked_alerts} sent={sent} failed={failed} dur_ms={dur_ms} reason={reason}")
        return 1
    finally:
        global DB_TIMER
        DB_TIMER = None


def main() -> int:
    poll_interval_sec = int(env("ALERTS_POLL_INTERVAL_SEC", "300"))
    print(f"ALERTS_START version={read_version()}")
    while True:
        exit_code = run_once()
        if exit_code != 0:
            # Avoid tight crash loops; systemd handles restarts, but keep a small backoff.
            time.sleep(min(30, max(3, poll_interval_sec // 10)))
        else:
            time.sleep(poll_interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())

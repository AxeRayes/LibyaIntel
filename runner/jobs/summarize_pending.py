import os
import time
import re
import argparse
import fcntl
import hashlib

from datetime import datetime, timezone, timedelta

from backend.db import get_client
from requests.exceptions import ReadTimeout, ConnectionError


import runner.process.summarize as summarize_mod
from runner.process.summarize import (
    clean_text,
    summarize,
    WallClockTimeout,
    LLMUnavailable,
    LLMError,
)


BATCH = int(os.getenv("SUMMARY_BATCH", "3"))
CONCURRENCY = int(os.getenv("SUMMARY_CONCURRENCY", "1"))
MAX_ITEMS = BATCH
FETCH_LIMIT = int(os.getenv("SUMMARY_FETCH_LIMIT", str(MAX_ITEMS * 3)))
RETRY_DELAYS = [0.5, 2.0]
MAX_CONSECUTIVE_ERRORS = max(int(os.getenv("SUMMARY_MAX_CONSEC_ERRORS", "5")), 5)
TIMEOUT_COOLDOWN_MIN = int(os.getenv("SUMMARY_TIMEOUT_COOLDOWN_MIN", "30"))
DEFERRED_LONG_THRESHOLD = int(os.getenv("SUMMARY_DEFER_LONG_THRESHOLD", "12000"))
MODEL_MIN_CHARS = int(os.getenv("SUMMARY_MODEL_MIN_CHARS", "2200"))
FASTPATH_MAX_CHARS = int(os.getenv("SUMMARY_FASTPATH_MAX_CHARS", "320"))
FASTPATH_FIRST_SENTENCES = int(os.getenv("SUMMARY_FASTPATH_SENTENCES", "2"))
MIN_CONTENT_CHARS = int(os.getenv("SUMMARY_MIN_CONTENT_CHARS", "600"))
MIN_CONTENT_WORDS = int(os.getenv("SUMMARY_MIN_CONTENT_WORDS", "80"))
MAX_PER_SOURCE = int(os.getenv("SUMMARY_MAX_PER_SOURCE", "30"))
SUMMARY_MAX_WORDS = int(os.getenv("SUMMARY_MAX_WORDS", "700"))
SUMMARY_MAX_CHARS = int(os.getenv("SUMMARY_MAX_CHARS", "5000"))
ERROR_WINDOW_SEC = int(os.getenv("SUMMARY_ERROR_WINDOW_SEC", "300"))
ERROR_THRESHOLD = int(os.getenv("SUMMARY_ERROR_THRESHOLD", "3"))
DEGRADE_SECONDS = int(os.getenv("SUMMARY_DEGRADE_SECONDS", "600"))
SUMMARY_COOLDOWN_MS = int(os.getenv("SUMMARY_COOLDOWN_MS", "0"))
SUMMARY_USE_LLM = os.getenv("SUMMARY_USE_LLM", "0").lower() in ("1", "true", "yes")
DB_WRITE_RETRIES = int(os.getenv("SUMMARY_DB_WRITE_RETRIES", "2"))
DB_WRITE_BACKOFF_MS = int(os.getenv("SUMMARY_DB_WRITE_BACKOFF_MS", "300"))
BRIEF_PATH_MARKERS = ("/inbrief/", "/brief/", "/short/", "/newsbrief/", "/bulletin/")

_ARTICLE_COLUMNS: set[str] | None = None


def _get_article_columns(sb) -> set[str]:
    global _ARTICLE_COLUMNS
    if _ARTICLE_COLUMNS is not None:
        return _ARTICLE_COLUMNS
    try:
        cols = sb.rpc("get_columns", {"p_table": "articles"}).execute().data or []
        _ARTICLE_COLUMNS = {c.get("column_name") for c in cols if c.get("column_name")}
    except Exception:
        _ARTICLE_COLUMNS = set()
    return _ARTICLE_COLUMNS


def _has_column(sb, name: str) -> bool:
    return name in _get_article_columns(sb)


def _get_pending(sb, limit: int, mode: str) -> list[dict]:
    has_next_attempt = _has_column(sb, "summary_next_attempt_at")
    has_attempts = _has_column(sb, "summary_attempts")
    select_cols = [
        "id",
        "content",
        "title",
        "url",
        "source",
        "summary_status",
        "summary",
        "summary_updated_at",
        "summary_error",
        "created_at",
    ]
    if has_next_attempt:
        select_cols.append("summary_next_attempt_at")
    if has_attempts:
        select_cols.append("summary_attempts")
    if _has_column(sb, "content_hash"):
        select_cols.append("content_hash")
    if _has_column(sb, "summary_hash"):
        select_cols.append("summary_hash")
    if mode == "slow":
        statuses = ["PENDING", "TIMEOUT", "DEFERRED_LONG"]
    else:
        statuses = ["PENDING", "TIMEOUT"]
    res = (
        sb.table("articles")
        .select(",".join(select_cols))
        .in_("summary_status", statuses)
        .limit(limit)
        .execute()
    )
    items = res.data or []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=TIMEOUT_COOLDOWN_MIN)
    pending = []
    for item in items:
        status = item.get("summary_status")
        if status == "PENDING" or (mode == "slow" and status == "DEFERRED_LONG"):
            pending.append(item)
            continue
        if status == "TIMEOUT":
            if has_next_attempt:
                next_attempt = item.get("summary_next_attempt_at")
                try:
                    next_dt = (
                        datetime.fromisoformat(next_attempt.replace("Z", "+00:00"))
                        if next_attempt
                        else None
                    )
                except Exception:
                    next_dt = None
                if not next_dt or next_dt <= now:
                    pending.append(item)
            else:
                updated = item.get("summary_updated_at")
                try:
                    updated_dt = (
                        datetime.fromisoformat(updated.replace("Z", "+00:00"))
                        if updated
                        else None
                    )
                except Exception:
                    updated_dt = None
                if not updated_dt or updated_dt <= cutoff:
                    pending.append(item)
    pending.sort(key=lambda x: len(x.get("content") or ""))
    return pending


def _fast_summary(text: str, max_chars: int = FASTPATH_MAX_CHARS) -> str:
    parts = re.split(r"(?<=[\.\!\?؟])\s+", text)
    if parts:
        summary = " ".join(parts[:FASTPATH_FIRST_SENTENCES]).strip()
        if summary:
            return summary[:max_chars].strip()
    return text[:max_chars].strip()


def _looks_arabic(text: str) -> bool:
    return any("\u0600" <= ch <= "\u06FF" for ch in (text or ""))


def _is_brief_url(url: str | None) -> bool:
    u = (url or "").lower()
    return any(marker in u for marker in BRIEF_PATH_MARKERS)


def _extractive_summary(text: str, title: str | None = None) -> str:
    t = " ".join((text or "").split())
    if not t:
        return (title or "").strip()
    parts = re.split(r"(?<=[\.\!\?؟])\s+", t)
    lead = " ".join(parts[:3]).strip()
    lead = lead[:800].strip()
    why_en = "Why it matters: it may affect Libya’s political, economic, or security situation."
    why_ar = "الأهمية: قد يؤثر ذلك على الوضع السياسي أو الاقتصادي أو الأمني في ليبيا."
    why = why_ar if _looks_arabic(t) else why_en
    if title:
        return f"{title.strip()}\n\n- {lead}\n\n{why}"
    return f"- {lead}\n\n{why}"


def _truncate_words(text: str, max_words: int) -> str:
    if max_words <= 0:
        return text
    parts = text.split()
    if len(parts) <= max_words:
        return text
    return " ".join(parts[:max_words])


def _truncate_chars(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return text
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _summary_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _is_junk(text: str) -> bool:
    if not text:
        return True
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return True
    if len(lines) >= 5:
        unique = len(set(lines))
        dup_ratio = 1.0 - (unique / len(lines))
        if dup_ratio > 0.5:
            return True
    low = text.lower()
    if any(m in low for m in ("cookie", "privacy", "subscribe", "newsletter")) and len(text) < 1200:
        return True
    return False


def _update_with_retry(sb, payload: dict, lookup_col: str, lookup_val: str) -> bool:
    for attempt in range(DB_WRITE_RETRIES + 1):
        try:
            sb.table("articles").update(payload).eq(lookup_col, lookup_val).execute()
            return True
        except Exception:
            if attempt < DB_WRITE_RETRIES:
                time.sleep(DB_WRITE_BACKOFF_MS / 1000.0)
                continue
            return False


def _next_attempt_error(err: str | None, attempts: int) -> str:
    base = err or ""
    return f"{base} attempt={attempts}"


def _parse_attempts(err: str | None) -> int:
    if not err:
        return 0
    try:
        parts = err.split("attempt=")
        if len(parts) >= 2:
            return int(parts[-1].strip())
    except Exception:
        return 0
    return 0


def _is_fastpath_candidate(source: str, title: str, url: str, text: str) -> bool:
    if source != "cbl":
        return False
    if "بيان" in (title or ""):
        return True
    if "%d8%a8%d9%8a%d8%a7%d9%86" in (url or "").lower():
        return True
    head = (text or "")[:50]
    if "بيان" in head:
        return True
    return False


def _percentile(values: list[int], pct: float) -> int:
    if not values:
        return 0
    values = sorted(values)
    idx = int(round((pct / 100.0) * (len(values) - 1)))
    return values[max(0, min(idx, len(values) - 1))]


def main() -> int:
    lock_path = "/tmp/libyaintel_summarize_pending.lock"
    try:
        lock_fd = open(lock_path, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        print("JOB_LOCKED exit=1")
        return 1
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["fast", "slow"], default="slow")
    args = parser.parse_args()
    mode = args.mode
    timeout_seconds = int(os.getenv("OLLAMA_TIMEOUT", "15"))
    summarize_mod.OLLAMA_TIMEOUT = timeout_seconds
    summarize_mod.OLLAMA_WALL_TIMEOUT = int(os.getenv("OLLAMA_WALL_TIMEOUT", str(timeout_seconds)))
    summary_model = os.getenv("SUMMARY_MODEL") or os.getenv("OLLAMA_MODEL")
    if summary_model:
        summarize_mod.OLLAMA_MODEL = summary_model

    sb = get_client()
    has_attempts = _has_column(sb, "summary_attempts")
    has_next_attempt = _has_column(sb, "summary_next_attempt_at")
    items = _get_pending(sb, FETCH_LIMIT, mode)
    processed = 0
    consecutive_errors = 0
    error_ts = []
    degrade_until = None
    per_source_count: dict[str, int] = {}
    per_source_stats: dict[str, dict] = {}
    hash_skips = 0
    done = 0
    fastpath_done = 0
    junk_skips = 0
    failed = 0
    clamped_reason_count = 0
    started_ts = time.monotonic()

    model_name = os.getenv("SUMMARY_MODEL") or os.getenv("OLLAMA_MODEL") or getattr(summarize_mod, "OLLAMA_MODEL", None) or "unknown"
    model_display = model_name if SUMMARY_USE_LLM else "OFF"
    llm_enabled = 1 if SUMMARY_USE_LLM else 0
    per_source_cap = min(MAX_PER_SOURCE, max(5, BATCH // 3))
    print(
        "SUMMARY_RUN "
        f"start={datetime.now(timezone.utc).isoformat()} "
        f"batch_size={BATCH} concurrency={CONCURRENCY} max_per_source={per_source_cap} "
        f"min_chars={MIN_CONTENT_CHARS} min_words={MIN_CONTENT_WORDS} "
        f"cooldown_ms={SUMMARY_COOLDOWN_MS} error_window_sec={ERROR_WINDOW_SEC} "
        f"error_threshold={ERROR_THRESHOLD} degrade_seconds={DEGRADE_SECONDS} "
        f"degrade_mode={1 if degrade_until else 0} llm_enabled={llm_enabled} "
        f"model={model_display} workers=1"
    )
    if degrade_until:
        print(f"SUMMARY_BREAKER degrade_mode=1 until={datetime.fromtimestamp(degrade_until, tz=timezone.utc).isoformat()}")
    for item in items:
        if processed >= MAX_ITEMS:
            break
        content = item.get("content") or ""
        if not content:
            continue
        title = item.get("title") or ""
        source = item.get("source") or ""
        url = item.get("url") or ""
        per_source_count[source] = per_source_count.get(source, 0)
        if per_source_count[source] >= per_source_cap:
            continue
        src_stats = per_source_stats.setdefault(
            source,
            {
                "selected": 0,
                "done": 0,
                "fastpath": 0,
                "junk": 0,
                "hash_skip": 0,
                "failed": 0,
                "elapsed_ms": [],
            },
        )

        cleaned = clean_text(content)
        def _count_words(text: str) -> int:
            if not text:
                return 0
            collapsed = " ".join(text.split())
            if not collapsed:
                return 0
            return len(collapsed.split(" "))

        words = _count_words(cleaned)
        has_content_hash = _has_column(sb, "content_hash")
        has_summary_hash = _has_column(sb, "summary_hash")
        content_hash = _content_hash(cleaned) if has_content_hash else None
        stored_hash = item.get("content_hash") if has_content_hash else None
        if item.get("summary_status") == "DONE" and stored_hash and stored_hash == content_hash:
            src_stats["selected"] += 1
            src_stats["hash_skip"] += 1
            hash_skips += 1
            if src_stats["hash_skip"] <= 3:
                print(
                    f"SUMMARY_ITEM source={source} action=HASH_SKIP elapsed_ms=0 "
                    f"text_chars={len(cleaned)} text_words={words} url={url}"
                )
            continue
        if item.get("summary_status") == "PENDING" and item.get("summary") and stored_hash and stored_hash == content_hash:
            src_stats["selected"] += 1
            payload = {
                "summary_status": "DONE",
                "summary_updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if has_summary_hash:
                payload["summary_hash"] = _summary_hash(item.get("summary") or "")
            ok = _update_with_retry(sb, payload, "id", item["id"])
            if not ok:
                elapsed_ms = int((time.monotonic() - start_ts) * 1000)
                failed += 1
                src_stats["failed"] += 1
                if src_stats["failed"] <= 3:
                    print(
                        f"SUMMARY_ITEM source={source} action=FAILED reason=db_update_failed "
                        f"text_chars={len(cleaned)} text_words={words} elapsed_ms={elapsed_ms} url={url}"
                    )
                continue
            processed += 1
            src_stats["done"] += 1
            done += 1
            if src_stats["done"] <= 3:
                print(
                    f"SUMMARY_ITEM source={source} action=DONE elapsed_ms=0 "
                    f"text_chars={len(cleaned)} text_words={words} url={url}"
                )
            continue
        src_stats["selected"] += 1
        summary = ""
        status = "ERROR"
        error_msg = None
        action_reason = ""
        start_ts = time.monotonic()
        try:
            now_ts = time.time()
            degrade_mode_active = bool(degrade_until and now_ts < degrade_until)
            text_chars = len(cleaned)
            text_words = words

            action = None
            reason = None

            if _is_brief_url(url):
                action = "FASTPATH"
                reason = "BRIEF_EXTRACTIVE"
            elif not cleaned or not cleaned.strip():
                action = "FASTPATH"
                reason = "NO_TEXT"
            elif degrade_mode_active:
                action = "FASTPATH"
                reason = "DEGRADED"
            else:
                short_chars = text_chars < MIN_CONTENT_CHARS
                short_words = text_words < MIN_CONTENT_WORDS
                if short_chars and short_words:
                    action = "FASTPATH"
                    reason = "SHORT_BOTH"
                else:
                    action = "DONE"
                    reason = "LLM"

            if action is None:
                raise RuntimeError("summary action unset")

            if action == "DONE" and _is_junk(cleaned):
                action = "JUNK"
                if "cookie" in cleaned.lower() or "privacy" in cleaned.lower():
                    reason = "JUNK_COOKIE"
                elif len(set([l.strip() for l in cleaned.splitlines() if l.strip()])) < max(
                    1, int(len(cleaned.splitlines()) * 0.5)
                ):
                    reason = "JUNK_REPEATED_LINES"
                else:
                    reason = "JUNK_OTHER"
            action_reason = reason

            if action == "FASTPATH":
                if action_reason == "BRIEF_EXTRACTIVE":
                    summary = _extractive_summary(cleaned, title=title)
                else:
                    summary = _fast_summary(cleaned) if cleaned else (title or "")
                status = "DONE_FASTPATH" if summary else "ERROR"
                if not summary:
                    error_msg = "empty_summary"
            elif action == "JUNK":
                status = "SKIPPED_JUNK"
                error_msg = reason
            else:
                if not SUMMARY_USE_LLM:
                    summary = _extractive_summary(cleaned, title=title)
                    status = "DONE_FASTPATH" if summary else "ERROR"
                    if not summary:
                        error_msg = "empty_summary"
                    action = "FASTPATH"
                    reason = "EXTRACTIVE"
                elif len(cleaned) > DEFERRED_LONG_THRESHOLD:
                    status = "DEFERRED_LONG"
                    error_msg = "deferred_long_content"
                elif len(cleaned) < MODEL_MIN_CHARS:
                    summary = _fast_summary(cleaned)
                    status = "DONE_FASTPATH" if summary else "ERROR"
                    if not summary:
                        error_msg = "empty_summary"
                    action = "FASTPATH"
                    reason = "SHORT_MODEL_MIN"
                else:
                    if mode == "slow":
                        summary_text = _truncate_chars(cleaned, SUMMARY_MAX_CHARS)
                        summary_text = _truncate_words(summary_text, SUMMARY_MAX_WORDS)
                        summary = summarize(summary_text)
                    else:
                        continue
                if summary and status == "ERROR":
                    status = "OK"
                if not summary and status == "ERROR":
                    error_msg = "empty_summary"
        except LLMUnavailable as e:
            status = "LLM_UNAVAILABLE"
            error_msg = str(e)[:200] if str(e) else "llm_unavailable"
        except LLMError as e:
            status = "LLM_ERROR"
            error_msg = str(e)[:200] if str(e) else "llm_error"
        except (WallClockTimeout, ReadTimeout, ConnectionError) as e:
            status = "TIMEOUT"
            error_msg = str(e)[:200] if str(e) else "ollama_timeout"
        except Exception as e:
            status = "ERROR"
            error_msg = str(e)[:200]

        is_success = status in ("OK", "DONE_FASTPATH", "DEFERRED_LONG", "SKIPPED_JUNK")
        if is_success:
            attempts = 0
        elif has_attempts:
            attempts = int(item.get("summary_attempts") or 0) + 1
        else:
            attempts = _parse_attempts(item.get("summary_error")) + 1
        payload = {
            "summary": summary,
            "summary_status": "DONE" if status == "OK" else status,
            "summary_updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if has_content_hash and content_hash:
            payload["content_hash"] = content_hash
        if has_summary_hash and summary:
            payload["summary_hash"] = _summary_hash(summary)
        if status == "OK" or status == "DONE_FASTPATH":
            if has_attempts:
                payload["summary_attempts"] = 0
            if has_next_attempt:
                payload["summary_next_attempt_at"] = None
        else:
            if has_attempts:
                payload["summary_attempts"] = attempts
            if error_msg:
                payload["summary_error"] = _next_attempt_error(error_msg, attempts)
            if status == "TIMEOUT" and has_next_attempt:
                next_dt = datetime.now(timezone.utc) + timedelta(minutes=TIMEOUT_COOLDOWN_MIN)
                payload["summary_next_attempt_at"] = next_dt.isoformat()
        ok = _update_with_retry(sb, payload, "id", item["id"])
        elapsed_ms = int((time.monotonic() - start_ts) * 1000)
        if not ok:
            failed += 1
            src_stats["failed"] += 1
            if src_stats["failed"] <= 3:
                print(
                    f"SUMMARY_ITEM source={source} action=FAILED reason=db_update_failed "
                    f"text_chars={len(cleaned)} text_words={words} elapsed_ms={elapsed_ms} url={url}"
                )
            continue

        processed += 1
        src_stats["elapsed_ms"].append(elapsed_ms)
        action = "FAILED"
        if status == "OK":
            action = "DONE"
            src_stats["done"] += 1
            done += 1
        elif status == "DONE_FASTPATH":
            action = "FASTPATH"
            src_stats["fastpath"] += 1
            fastpath_done += 1
        elif status == "SKIPPED_JUNK":
            action = "JUNK"
            src_stats["junk"] += 1
            junk_skips += 1
        elif status in ("TIMEOUT", "ERROR", "LLM_ERROR", "LLM_UNAVAILABLE"):
            src_stats["failed"] += 1
            failed += 1
        print_reason = error_msg if action == "FAILED" else (action_reason or "")
        if not SUMMARY_USE_LLM and print_reason == "LLM":
            clamped_reason_count += 1
            print_reason = "EXTRACTIVE"
        if action in ("FASTPATH", "JUNK"):
            src_stats["fastpath"] = src_stats.get("fastpath", 0)
            if src_stats["fastpath"] <= 3:
                print(
                    f"SUMMARY_ITEM source={source} action={action} reason={print_reason} "
                    f"text_chars={len(cleaned)} text_words={words} elapsed_ms={elapsed_ms} url={url}"
                )
        elif action == "DONE" and src_stats["done"] <= 3:
            print(
                f"SUMMARY_ITEM source={source} action=DONE reason={print_reason} text_chars={len(cleaned)} "
                f"text_words={words} elapsed_ms={elapsed_ms} url={url}"
            )
        elif action == "FAILED" and src_stats["failed"] <= 3:
            print(
                f"SUMMARY_ITEM source={source} action=FAILED reason={print_reason} "
                f"text_chars={len(cleaned)} text_words={words} elapsed_ms={elapsed_ms} url={url}"
            )
        per_source_count[source] = per_source_count.get(source, 0) + 1

        if status in ("TIMEOUT", "ERROR"):
            consecutive_errors += 1
            error_ts.append(time.time())
        else:
            consecutive_errors = 0
        cutoff = time.time() - ERROR_WINDOW_SEC
        error_ts = [t for t in error_ts if t >= cutoff]
        if len(error_ts) >= ERROR_THRESHOLD and not degrade_until:
            degrade_until = time.time() + DEGRADE_SECONDS
            print(
                "SUMMARY_BREAKER tripped=1 "
                f"error_count={len(error_ts)} window_sec={ERROR_WINDOW_SEC} "
                f"degrade_until={datetime.fromtimestamp(degrade_until, tz=timezone.utc).isoformat()}"
            )
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            print(f"SUMMARY_BREAK consecutive_errors={consecutive_errors}")
            break
        if SUMMARY_COOLDOWN_MS:
            time.sleep(SUMMARY_COOLDOWN_MS / 1000.0)

    for src, s in per_source_stats.items():
        avg_ms = int(sum(s["elapsed_ms"]) / len(s["elapsed_ms"])) if s["elapsed_ms"] else 0
        p95_ms = _percentile(s["elapsed_ms"], 95)
        print(
            f"SUMMARY_SOURCE source={src} picked={s['selected']} processed={s['done'] + s['fastpath'] + s['junk'] + s['hash_skip'] + s['failed']} "
            f"done={s['done']} "
            f"fastpath={s['fastpath']} junk={s['junk']} hash_skip={s['hash_skip']} "
            f"failed={s['failed']} avg_ms={avg_ms} p95_ms={p95_ms}"
        )
    print(
        f"SUMMARY_DONE processed={processed} done={done} fastpath={fastpath_done} "
        f"junk={junk_skips} hash_skip={hash_skips} failed={failed} "
        f"elapsed_ms={int((time.monotonic() - started_ts) * 1000)}"
    )
    if clamped_reason_count:
        print(f"SUMMARY_WARN clamped_reason_llm_to_extractive count={clamped_reason_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

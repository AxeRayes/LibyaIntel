import os
from datetime import datetime, timezone, timedelta

from backend.db import get_client, get_key_column


def _start_of_day(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)

def _normalize_name(val: str | None) -> str:
    if not val:
        return ""
    cleaned = []
    depth = 0
    for ch in val:
        if ch == "(":
            depth += 1
            continue
        if ch == ")":
            if depth:
                depth -= 1
            continue
        if depth == 0:
            cleaned.append(ch)
    base = "".join(cleaned).strip()
    return "".join(ch for ch in base.lower() if ch.isalnum())

def _has_column(sb, table: str, name: str) -> bool:
    cols = sb.rpc("get_columns", {"p_table": table}).execute().data or []
    return name in {c["column_name"] for c in cols}

def _map_source_key(raw_source: str | None, source_name_to_key: dict[str, str]) -> str | None:
    norm = _normalize_name(raw_source)
    if not norm:
        return None
    direct = source_name_to_key.get(norm)
    if direct:
        return direct
    for name_norm, key in source_name_to_key.items():
        if name_norm and (name_norm in norm or norm in name_norm):
            return key
    return None


def _detect_article_source_mode(sb) -> str | None:
    cols = sb.rpc("get_columns", {"p_table": "articles"}).execute().data or []
    names = {c.get("column_name") for c in cols if c.get("column_name")}
    if "source_key" in names:
        return "source_key"
    if "source_id" in names:
        return "source_id"
    if "source" in names:
        return "source"
    return None


def _detect_article_ts_col(sb) -> str | None:
    cols = sb.rpc("get_columns", {"p_table": "articles"}).execute().data or []
    names = {c.get("column_name") for c in cols if c.get("column_name")}
    if "published_at" in names:
        return "published_at"
    if "created_at" in names:
        return "created_at"
    return None


def _article_counts(
    sb,
    *,
    days: int,
    source_id_to_key: dict[str, str],
    source_name_to_key: dict[str, str],
    source_key_set: set[str],
) -> dict[str, int]:
    source_col = _detect_article_source_mode(sb)
    ts_col = _detect_article_ts_col(sb)
    if not source_col or not ts_col:
        return {}
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = (
        sb.table("articles")
        .select(f"{source_col},{ts_col}")
        .gte(ts_col, since)
        .execute()
        .data
        or []
    )
    counts: dict[str, int] = {}
    for row in rows:
        raw_key = row.get(source_col)
        if not raw_key:
            continue
        if source_col == "source_id":
            key = source_id_to_key.get(str(raw_key)) or source_id_to_key.get(raw_key)
        elif source_col == "source":
            key = _map_source_key(str(raw_key), source_name_to_key) or (
                str(raw_key) if str(raw_key) in source_key_set else None
            )
        else:
            key = str(raw_key)
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return counts


def main() -> int:
    sb = get_client()
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_iso = day_start.isoformat()
    day_end = day_start + timedelta(days=1)
    day_end_iso = day_end.isoformat()
    print(f"DAILY_WINDOW start={day_start_iso} end={day_end_iso}")

    # Aggregate ingest_runs stats for today only (UTC window).
    runs = (
        sb.table("ingest_runs")
        .select("stats,started_at,job_name")
        .gte("started_at", day_start_iso)
        .lt("started_at", day_end_iso)
        .eq("job_name", "page_ingest")
        .execute()
        .data
        or []
    )
    daily_by_source: dict[str, dict] = {}
    for r in runs:
        stats = r.get("stats") or {}
        by_source = stats.get("by_source") or {}
        for key, s in by_source.items():
            saved = int(s.get("saved") or 0)
            failed = int(s.get("failed") or 0)
            blocked = int(s.get("blocked") or 0)
            attempted = s.get("attempted", None)
            if attempted is None:
                attempted = s.get("total", None)
            if attempted is None:
                attempted = saved + failed + blocked
            attempted = int(attempted or 0)
            if attempted > 0 and saved > attempted:
                print(
                    f"DAILY_WARN bad_run_detected job=page_ingest source={key} "
                    f"saved={saved} attempted={attempted} started_at={r.get('started_at')}"
                )
                continue
            row = daily_by_source.setdefault(
                key,
                {
                    "saved_new": 0,
                    "failed": 0,
                    "blocked": 0,
                    "junk_saved": 0,
                    "attempted": 0,
                    "total": 0,
                    "err_dns": 0,
                    "err_timeout": 0,
                    "err_connect": 0,
                    "err_tls": 0,
                    "err_http_403": 0,
                    "err_http_429": 0,
                    "err_http": 0,
                    "err_other": 0,
                    "dedup_existing": 0,
                    "dedup_new": 0,
                    "updated_existing": 0,
                    "fetch_degraded": False,
                    "discovery_degraded": False,
                },
            )
            total = int(s.get("total") or 0)

            dedup_existing = s.get("deduped_existing", None)
            if dedup_existing is None:
                dedup_existing = s.get("dedup_existing", 0)
            dedup_existing = int(dedup_existing or 0)

            row["saved_new"] += saved
            row["failed"] += failed
            row["blocked"] += blocked
            row["total"] += total
            row["attempted"] += attempted
            row["dedup_existing"] += dedup_existing
            row["dedup_new"] += int(s.get("dedup_new") or 0)
            row["updated_existing"] += int(s.get("updated_existing") or 0)
            row["junk_saved"] += int(s.get("junk_saved") or 0)

            row["err_dns"] += int(s.get("err_dns", 0))
            row["err_timeout"] += int(s.get("err_timeout", 0))
            row["err_connect"] += int(s.get("err_connect", 0))
            row["err_tls"] += int(s.get("err_tls", 0))
            row["err_http_403"] += int(s.get("err_http_403", 0))
            row["err_http_429"] += int(s.get("err_http_429", 0))
            row["err_http"] += int(s.get("err_http", 0))
            row["err_other"] += int(s.get("err_other", 0))
            row["fetch_degraded"] = row["fetch_degraded"] or bool(s.get("fetch_degraded"))
            row["discovery_degraded"] = row["discovery_degraded"] or bool(
                s.get("discovery_degraded")
            )

    # Map source_id -> source key for consistent reporting.
    key_col = get_key_column(sb)
    sources = sb.table("sources").select(f"id,{key_col},name").execute().data or []
    source_id_to_key = {row["id"]: row.get(key_col) for row in sources}
    source_name_to_key = {}
    for row in sources:
        norm = _normalize_name(row.get("name"))
        if norm:
            source_name_to_key[norm] = row.get(key_col)
    source_key_set = {row.get(key_col) for row in sources}
    has_source_id = _has_column(sb, "articles", "source_id")
    source_key_set = {row.get(key_col) for row in sources if row.get(key_col)}

    # Summary state (no date filter). These do NOT affect DAILY_METRICS.
    if has_source_id:
        summary_counts = (
            sb.table("articles").select("source_id,summary_status").execute().data or []
        )
    else:
        summary_counts = (
            sb.table("articles").select("source,summary_status").execute().data or []
        )
    summary_by_source = {}
    for row in summary_counts:
        if has_source_id:
            sid = row.get("source_id")
            key = source_id_to_key.get(sid) if sid else None
            if not key:
                key = "unknown"
        else:
            raw_source = row.get("source")
            key = _map_source_key(raw_source, source_name_to_key) or (
                raw_source if raw_source in source_key_set else None
            )
            if not key:
                key = row.get("source") or "unknown"
        status = row.get("summary_status") or "UNKNOWN"
        if status == "DONE_FASTPATH":
            status = "DONE"
        bucket = summary_by_source.setdefault(key, {})
        bucket[status] = bucket.get(status, 0) + 1

    # Summary completed today (optional, date filter).
    if has_source_id:
        summary_today = (
            sb.table("articles")
            .select("source_id,summary_status,summary_updated_at")
            .gte("summary_updated_at", day_start_iso)
            .lt("summary_updated_at", day_end_iso)
            .execute()
            .data
            or []
        )
    else:
        summary_today = (
            sb.table("articles")
            .select("source,summary_status,summary_updated_at")
            .gte("summary_updated_at", day_start_iso)
            .lt("summary_updated_at", day_end_iso)
            .execute()
            .data
            or []
        )
    summary_today_by_source = {}
    for row in summary_today:
        if has_source_id:
            sid = row.get("source_id")
            key = source_id_to_key.get(sid) if sid else None
            if not key:
                key = "unknown"
        else:
            raw_source = row.get("source")
            key = _map_source_key(raw_source, source_name_to_key) or (
                raw_source if raw_source in source_key_set else None
            )
            if not key:
                key = row.get("source") or "unknown"
        status = row.get("summary_status") or "UNKNOWN"
        if status == "DONE_FASTPATH":
            status = "DONE"
        bucket = summary_today_by_source.setdefault(key, {})
        bucket[status] = bucket.get(status, 0) + 1

    counts_7d = _article_counts(
        sb,
        days=7,
        source_id_to_key=source_id_to_key,
        source_name_to_key=source_name_to_key,
        source_key_set=source_key_set,
    )
    counts_48h = _article_counts(
        sb,
        days=2,
        source_id_to_key=source_id_to_key,
        source_name_to_key=source_name_to_key,
        source_key_set=source_key_set,
    )
    top5 = sorted(counts_7d.items(), key=lambda x: x[1], reverse=True)[:5]
    top5_str = ",".join(f"{k}:{v}" for k, v in top5)
    sources_active_48h = len([k for k, v in counts_48h.items() if v > 0])
    print(f"DAILY_SOURCES_TOP5 sources={top5_str}")
    print(f"DAILY_SOURCES_ACTIVE_48H count={sources_active_48h}")

    for key, s in daily_by_source.items():
        attempted = s["attempted"]
        fetch_degraded = s["fetch_degraded"]
        saved_new = s.get("saved_new", s.get("saved", 0))
        updated_existing = s.get("updated_existing", 0)

        if attempted == 0:
            fail_rate_str = "NA"
            block_rate_str = "NA"
            kept_ratio_str = "NA"
            if s.get("failed", 0) > 0 or s.get("blocked", 0) > 0:
                fetch_degraded = True
        else:
            fail_rate_str = f"{(s['failed'] / attempted):.3f}"
            block_rate_str = f"{(s['blocked'] / attempted):.3f}"
            kept_ratio_str = f"{(saved_new / attempted):.3f}"

        print(
            f"DAILY_METRICS source={key} "
            f"saved_new={saved_new} updated_existing={updated_existing} "
            f"failed={s['failed']} blocked={s['blocked']} "
            f"attempted={attempted} fail_rate={fail_rate_str} block_rate={block_rate_str} "
            f"junk_saved={s['junk_saved']} kept_ratio={kept_ratio_str} "
            f"dedup_existing={s['dedup_existing']} dedup_new={s['dedup_new']} "
            f"discovery_degraded={int(s['discovery_degraded'])} fetch_degraded={int(fetch_degraded)}"
        )
        if any(
            s.get(k, 0)
            for k in (
                "err_dns",
                "err_timeout",
                "err_connect",
                "err_tls",
                "err_http_403",
                "err_http_429",
                "err_http",
                "err_other",
            )
        ):
            print(
                f"DAILY_ERRORS source={key} dns={s['err_dns']} timeout={s['err_timeout']} "
                f"connect={s['err_connect']} tls={s['err_tls']} http_403={s['err_http_403']} "
                f"http_429={s['err_http_429']} http_other={s['err_http']} other={s['err_other']}"
            )
        if attempted > 0:
            http_err = s["err_http_403"] + s["err_http_429"] + s["err_http"]
            print(
                f"DAILY_ERROR_RATES source={key} dns_rate={(s['err_dns']/attempted):.3f} "
                f"timeout_rate={(s['err_timeout']/attempted):.3f} "
                f"http_rate={(http_err/attempted):.3f}"
            )
        if key in summary_by_source:
            done = summary_by_source[key].get("DONE", 0)
            pending = summary_by_source[key].get("PENDING", 0)
            timeout = summary_by_source[key].get("TIMEOUT", 0)
            error = summary_by_source[key].get("ERROR", 0)
            print(
                f"DAILY_SUMMARY_STATE source={key} done={done} pending={pending} "
                f"timeout={timeout} error={error}"
            )
        if key in summary_today_by_source:
            done_today = summary_today_by_source[key].get("DONE", 0)
            print(
                f"DAILY_SUMMARY_TODAY source={key} done_today={done_today}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

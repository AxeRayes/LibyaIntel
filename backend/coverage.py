import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

from backend.db import get_client, get_key_column


@dataclass
class SourceConfig:
    key: str
    enabled: bool
    source_type: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json_list(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def load_source_configs(repo_root: Path) -> dict[str, SourceConfig]:
    configs: dict[str, SourceConfig] = {}

    sources_path = repo_root / "runner" / "ingest" / "sources.json"
    for row in _load_json_list(sources_path):
        key = row.get("id")
        if not key:
            continue
        ingest_method = (row.get("ingest_method") or "page").strip().lower()
        source_type = "rss" if ingest_method == "rss" else "page"
        configs[key] = SourceConfig(
            key=key,
            enabled=bool(row.get("enabled", False)),
            source_type=source_type,
        )

    social_path = repo_root / "ingest" / "sources_social.json"
    for row in _load_json_list(social_path):
        key = row.get("id")
        if not key:
            continue
        configs[key] = SourceConfig(
            key=key,
            enabled=bool(row.get("enabled", False)),
            source_type="social",
        )

    return configs


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


def _map_source_key(
    raw_source: str | None, source_name_to_key: dict[str, str], source_key_set: set[str]
) -> str | None:
    if not raw_source:
        return None
    if raw_source in source_key_set:
        return raw_source
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


def _get_table_columns(sb, table: str) -> set[str]:
    try:
        cols = sb.rpc("get_columns", {"p_table": table}).execute().data or []
    except Exception:
        return set()
    return {c.get("column_name") for c in cols if c.get("column_name")}


def _detect_article_source_mode(sb) -> tuple[str | None, str | None]:
    cols = _get_table_columns(sb, "articles")
    if not cols:
        return None, None
    if "source_key" in cols:
        return "source_key", "source_key"
    if "source_id" in cols:
        return "source_id", "source_id"
    if "source" in cols:
        return "source", "source"
    return None, None


def _detect_article_ts_col(sb) -> str | None:
    cols = _get_table_columns(sb, "articles")
    if "published_at" in cols:
        return "published_at"
    if "created_at" in cols:
        return "created_at"
    return None


def _db_source_maps(sb) -> tuple[dict[str, str], dict[str, str], set[str]]:
    key_col = get_key_column(sb)
    rows = sb.table("sources").select(f"id,name,{key_col}").execute().data or []
    id_to_key = {
        row["id"]: row.get(key_col) or row["id"] for row in rows if row.get("id")
    }
    source_key_set = {row.get(key_col) for row in rows if row.get(key_col)}
    source_name_to_key = {}
    for row in rows:
        norm = _normalize_name(row.get("name"))
        if norm and row.get(key_col):
            source_name_to_key[norm] = row.get(key_col)
    return id_to_key, source_name_to_key, source_key_set


def _query_article_stats_psycopg2(
    dsn: str,
    source_col: str,
    ts_col: str,
    days: int,
    *,
    key_col: str | None,
    source_id_is_key: bool,
) -> dict[str, dict[str, Any]]:
    ts_expr = "COALESCE(published_at, created_at)" if ts_col == "published_at" else ts_col
    if source_col == "source_id" and source_id_is_key:
        src_expr = "a.source_id::text"
        join = ""
    elif source_col == "source_id" and key_col:
        src_expr = f"COALESCE(s.{key_col}::text, a.source_id::text)"
        join = "LEFT JOIN sources s ON s.id::text = a.source_id::text"
    else:
        src_expr = f"a.{source_col}::text"
        join = ""
    sql = (
        f"SELECT {src_expr} AS src_key, "
        f"COUNT(*) AS articles_7d, "
        f"MAX({ts_expr}) AS last_article_at "
        f"FROM articles a "
        f"{join} "
        f"WHERE {ts_expr} >= (NOW() AT TIME ZONE 'UTC') - INTERVAL %s "
        f"GROUP BY 1"
    )
    with psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (f"{days} days",))
            rows = cur.fetchall() or []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get("src_key")
        if not key:
            continue
        out[key] = {
            "articles_7d": int(row.get("articles_7d") or 0),
            "last_article_at": row.get("last_article_at").isoformat().replace("+00:00", "Z")
            if row.get("last_article_at")
            else None,
        }
    return out


def _query_article_stats_supabase(
    sb,
    source_col: str,
    ts_col: str,
    days: int,
    *,
    id_to_key: dict[str, str],
    source_name_to_key: dict[str, str],
    source_key_set: set[str],
) -> dict[str, dict[str, Any]]:
    since = (_utc_now() - timedelta(days=days)).isoformat()
    items = (
        sb.table("articles")
        .select(f"{source_col},{ts_col}")
        .gte(ts_col, since)
        .execute()
        .data
        or []
    )
    out: dict[str, dict[str, Any]] = {}
    for row in items:
        raw_key = row.get(source_col)
        if not raw_key:
            continue
        if source_col == "source_id":
            key = id_to_key.get(str(raw_key)) or id_to_key.get(raw_key)
        elif source_col == "source":
            key = _map_source_key(str(raw_key), source_name_to_key, source_key_set) or str(raw_key)
        else:
            key = str(raw_key)

        if not key:
            continue
        ts = row.get(ts_col)
        entry = out.setdefault(key, {"articles_7d": 0, "last_article_at": None})
        entry["articles_7d"] += 1
        if ts and (entry["last_article_at"] is None or ts > entry["last_article_at"]):
            entry["last_article_at"] = ts
    return out


def _query_article_quality_counts_psycopg2(
    dsn: str,
    source_col: str,
    ts_col: str,
    days: int,
    *,
    key_col: str | None,
    source_id_is_key: bool,
) -> dict[str, dict[str, Any]]:
    ts_expr = "COALESCE(published_at, created_at)" if ts_col == "published_at" else ts_col
    if source_col == "source_id" and source_id_is_key:
        src_expr = "a.source_id::text"
        join = ""
    elif source_col == "source_id" and key_col:
        src_expr = f"COALESCE(s.{key_col}::text, a.source_id::text)"
        join = "LEFT JOIN sources s ON s.id::text = a.source_id::text"
    else:
        src_expr = f"a.{source_col}::text"
        join = ""
    sql = (
        f"SELECT {src_expr} AS src_key, "
        f"SUM(CASE WHEN a.content_kind = 'full' THEN 1 ELSE 0 END) AS full_count_7d, "
        f"SUM(CASE WHEN a.content_kind = 'teaser' THEN 1 ELSE 0 END) AS teaser_count_7d, "
        f"SUM(CASE WHEN a.content_kind = 'title_only' THEN 1 ELSE 0 END) AS blocked_count_7d "
        f"FROM articles a "
        f"{join} "
        f"WHERE {ts_expr} >= (NOW() AT TIME ZONE 'UTC') - INTERVAL %s "
        f"GROUP BY 1"
    )
    with psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (f"{days} days",))
            rows = cur.fetchall() or []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get("src_key")
        if not key:
            continue
        out[key] = {
            "full_count_7d": int(row.get("full_count_7d") or 0),
            "teaser_count_7d": int(row.get("teaser_count_7d") or 0),
            "blocked_count_7d": int(row.get("blocked_count_7d") or 0),
        }
    return out


def _query_article_quality_counts_supabase(
    sb,
    source_col: str,
    ts_col: str,
    days: int,
    *,
    id_to_key: dict[str, str],
    source_name_to_key: dict[str, str],
    source_key_set: set[str],
) -> dict[str, dict[str, Any]]:
    since = (_utc_now() - timedelta(days=days)).isoformat()
    items = (
        sb.table("articles")
        .select(f"{source_col},{ts_col},content_kind")
        .gte(ts_col, since)
        .execute()
        .data
        or []
    )
    out: dict[str, dict[str, Any]] = {}
    for row in items:
        raw_key = row.get(source_col)
        if not raw_key:
            continue
        if source_col == "source_id":
            key = id_to_key.get(str(raw_key)) or id_to_key.get(raw_key)
        elif source_col == "source":
            key = _map_source_key(str(raw_key), source_name_to_key, source_key_set) or str(raw_key)
        else:
            key = str(raw_key)
        if not key:
            continue
        entry = out.setdefault(
            key, {"full_count_7d": 0, "teaser_count_7d": 0, "blocked_count_7d": 0}
        )
        kind = row.get("content_kind") or "full"
        if kind == "teaser":
            entry["teaser_count_7d"] += 1
        elif kind == "title_only":
            entry["blocked_count_7d"] += 1
        else:
            entry["full_count_7d"] += 1
    return out


def _query_feed_quality_counts_psycopg2(
    dsn: str,
    days: int,
    *,
    key_col: str | None,
) -> dict[str, dict[str, Any]]:
    ts_expr = "COALESCE(published_at, created_at)"
    if key_col:
        src_expr = f"COALESCE(s.{key_col}::text, fi.source_id::text)"
        join = "LEFT JOIN sources s ON s.id::text = fi.source_id::text"
    else:
        src_expr = "fi.source_id::text"
        join = ""
    sql = (
        f"SELECT {src_expr} AS src_key, "
        f"SUM(CASE WHEN fi.content_kind = 'full' THEN 1 ELSE 0 END) AS full_count_7d, "
        f"SUM(CASE WHEN fi.content_kind = 'teaser' THEN 1 ELSE 0 END) AS teaser_count_7d, "
        f"SUM(CASE WHEN fi.content_kind = 'title_only' THEN 1 ELSE 0 END) AS blocked_count_7d "
        f"FROM feed_items fi "
        f"{join} "
        f"WHERE {ts_expr} >= (NOW() AT TIME ZONE 'UTC') - INTERVAL %s "
        f"GROUP BY 1"
    )
    with psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (f"{days} days",))
            rows = cur.fetchall() or []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get("src_key")
        if not key:
            continue
        out[key] = {
            "full_count_7d": int(row.get("full_count_7d") or 0),
            "teaser_count_7d": int(row.get("teaser_count_7d") or 0),
            "blocked_count_7d": int(row.get("blocked_count_7d") or 0),
        }
    return out


def _query_feed_quality_counts_supabase(
    sb,
    days: int,
    *,
    id_to_key: dict[str, str],
    source_name_to_key: dict[str, str],
    source_key_set: set[str],
) -> dict[str, dict[str, Any]]:
    since = (_utc_now() - timedelta(days=days)).isoformat()
    items = (
        sb.table("feed_items")
        .select("source_id,published_at,created_at,content_kind")
        .gte("published_at", since)
        .execute()
        .data
        or []
    )
    out: dict[str, dict[str, Any]] = {}
    for row in items:
        raw_key = row.get("source_id")
        if not raw_key:
            continue
        key = id_to_key.get(str(raw_key)) or id_to_key.get(raw_key)
        if not key:
            norm = _normalize_name(raw_key)
            key = source_name_to_key.get(norm) or (raw_key if raw_key in source_key_set else None)
        if not key:
            continue
        entry = out.setdefault(
            key, {"full_count_7d": 0, "teaser_count_7d": 0, "blocked_count_7d": 0}
        )
        kind = row.get("content_kind") or "full"
        if kind == "teaser":
            entry["teaser_count_7d"] += 1
        elif kind == "title_only":
            entry["blocked_count_7d"] += 1
        else:
            entry["full_count_7d"] += 1
    return out

def _fetch_ingest_stats(sb) -> tuple[dict[str, dict[str, Any]], bool]:
    if not _get_table_columns(sb, "ingest_runs"):
        return {}, False

    since_24h = (_utc_now() - timedelta(hours=24)).isoformat()
    runs = (
        sb.table("ingest_runs")
        .select("id,ok,started_at,finished_at,stats")
        .gte("started_at", since_24h)
        .order("started_at", desc=True)
        .limit(500)
        .execute()
        .data
        or []
    )

    per_source: dict[str, dict[str, Any]] = {}
    for run in runs:
        stats = run.get("stats") or {}
        by_source = stats.get("by_source") or {}
        for key, meta in by_source.items():
            entry = per_source.setdefault(
                key,
                {
                    "failed": 0,
                    "attempted": 0,
                    "last_ingest_ok_at": None,
                    "last_ingest_status": "unknown",
                    "last_ingest_run_id": None,
                    "last_run_ts": None,
                },
            )
            started_at = run.get("started_at")
            finished_at = run.get("finished_at") or started_at
            if started_at and (entry["last_run_ts"] is None or started_at > entry["last_run_ts"]):
                entry["last_run_ts"] = started_at
                ok = run.get("ok")
                if ok is True:
                    entry["last_ingest_status"] = "ok"
                elif ok is False:
                    entry["last_ingest_status"] = "fail"
                else:
                    entry["last_ingest_status"] = "unknown"
                entry["last_ingest_run_id"] = run.get("id")

            if run.get("ok") is True and finished_at:
                if entry["last_ingest_ok_at"] is None or finished_at > entry["last_ingest_ok_at"]:
                    entry["last_ingest_ok_at"] = finished_at

            attempted = meta.get("attempted")
            if attempted is None:
                attempted = meta.get("total")
            if attempted is None:
                attempted = (meta.get("saved") or 0) + (meta.get("failed") or 0) + (meta.get("blocked") or 0)
            entry["attempted"] += int(attempted or 0)
            entry["failed"] += int(meta.get("failed") or 0)

    return per_source, True


def _fetch_ingest_health_stats(sb, window_hours: int) -> tuple[dict[str, dict[str, Any]], bool]:
    if not _get_table_columns(sb, "ingest_runs"):
        return {}, False

    since = (_utc_now() - timedelta(hours=window_hours)).isoformat()
    runs = (
        sb.table("ingest_runs")
        .select("id,ok,started_at,finished_at,stats")
        .gte("started_at", since)
        .order("started_at", desc=True)
        .limit(500)
        .execute()
        .data
        or []
    )

    per_source: dict[str, dict[str, Any]] = {}
    for run in runs:
        stats = run.get("stats") or {}
        by_source = stats.get("by_source") or {}
        started_at = run.get("started_at")
        finished_at = run.get("finished_at") or started_at
        for key, meta in by_source.items():
            entry = per_source.setdefault(
                key,
                {
                    "discovered": 0,
                    "fetched": 0,
                    "saved": 0,
                    "blocked": 0,
                    "failed": 0,
                    "last_error": None,
                    "last_error_ts": None,
                    "last_ok_at": None,
                },
            )

            discovered = meta.get("discovered_total")
            if discovered is None:
                discovered = meta.get("new_candidates")
            if discovered is None:
                discovered = meta.get("total")
            if discovered is None:
                discovered = 0

            fetched = meta.get("attempted")
            if fetched is None:
                fetched = meta.get("total")
            if fetched is None:
                fetched = 0

            entry["discovered"] += int(discovered or 0)
            entry["fetched"] += int(fetched or 0)
            entry["saved"] += int(meta.get("saved") or 0)
            entry["blocked"] += int(meta.get("blocked") or 0)
            entry["failed"] += int(meta.get("failed") or 0)

            last_error = meta.get("last_error")
            if last_error and started_at:
                last_ts = entry.get("last_error_ts")
                if last_ts is None or started_at > last_ts:
                    entry["last_error"] = last_error
                    entry["last_error_ts"] = started_at

            if run.get("ok") is True and finished_at:
                last_ok = entry.get("last_ok_at")
                if last_ok is None or finished_at > last_ok:
                    entry["last_ok_at"] = finished_at

    return per_source, True


def compute_ingest_health(window_hours: int = 24) -> dict[str, Any]:
    sb = get_client()
    repo_root = Path(__file__).resolve().parents[1]
    configs = load_source_configs(repo_root)

    stats, has_stats = _fetch_ingest_health_stats(sb, window_hours)
    quality_counts: dict[str, dict[str, Any]] = {}
    feed_counts: dict[str, dict[str, Any]] = {}
    cols = _get_table_columns(sb, "articles") or []
    if "content_kind" in cols:
        source_col, key_col = _detect_article_source_mode(sb)
        ts_col = _detect_article_ts_col(sb)
        if source_col and ts_col:
            id_to_key, source_name_to_key, source_key_set = _db_source_maps(sb)
            source_id_is_key = False
            if source_col == "source_id" and source_key_set:
                try:
                    sample = (
                        sb.table("articles")
                        .select("source_id")
                        .limit(20)
                        .execute()
                        .data
                        or []
                    )
                except Exception:
                    sample = []
                sample_vals = {row.get("source_id") for row in sample if row.get("source_id")}
                if sample_vals and sample_vals.issubset(source_key_set):
                    source_id_is_key = True
            dsn = os.getenv("DATABASE_URL")
            if dsn:
                quality_counts = _query_article_quality_counts_psycopg2(
                    dsn,
                    source_col,
                    ts_col,
                    7,
                    key_col=key_col,
                    source_id_is_key=source_id_is_key,
                )
                if _get_table_columns(sb, "feed_items"):
                    feed_counts = _query_feed_quality_counts_psycopg2(
                        dsn,
                        7,
                        key_col=key_col,
                    )
            else:
                quality_counts = _query_article_quality_counts_supabase(
                    sb,
                    source_col,
                    ts_col,
                    7,
                    id_to_key=id_to_key,
                    source_name_to_key=source_name_to_key,
                    source_key_set=source_key_set,
                )
                if _get_table_columns(sb, "feed_items"):
                    feed_counts = _query_feed_quality_counts_supabase(
                        sb,
                        7,
                        id_to_key=id_to_key,
                        source_name_to_key=source_name_to_key,
                        source_key_set=source_key_set,
                    )
    sources_payload = []
    unhealthy = []

    for key, cfg in configs.items():
        meta = stats.get(key) if has_stats else None
        discovered = meta.get("discovered") if meta else None
        fetched = meta.get("fetched") if meta else None
        saved = meta.get("saved") if meta else None
        blocked = meta.get("blocked") if meta else None
        failed = meta.get("failed") if meta else None
        last_error = meta.get("last_error") if meta else None
        last_ok_at = meta.get("last_ok_at") if meta else None

        q_counts = quality_counts.get(key) or {}
        f_counts = feed_counts.get(key) or {}
        full_count_7d = int(q_counts.get("full_count_7d") or 0)
        teaser_count_7d = int(q_counts.get("teaser_count_7d") or 0)
        blocked_count_7d = int(q_counts.get("blocked_count_7d") or 0)
        feed_full_count_7d = int(f_counts.get("full_count_7d") or 0)
        feed_teaser_count_7d = int(f_counts.get("teaser_count_7d") or 0)
        feed_blocked_count_7d = int(f_counts.get("blocked_count_7d") or 0)

        fail_rate = None
        if meta and meta.get("fetched", 0) > 0:
            fail_rate = meta.get("failed", 0) / meta.get("fetched", 0)

        classification = "unknown"
        if meta:
            if full_count_7d > 0:
                classification = "ok"
            elif teaser_count_7d > 0:
                classification = "degraded"
            elif (fetched or 0) > 0 and (saved or 0) == 0:
                classification = "extract"
            elif (fetched or 0) == 0 and (blocked or 0) > 0:
                classification = "blocked"
            elif (discovered or 0) == 0:
                classification = "config"
        if classification != "ok":
            unhealthy.append(
                {
                    "source_key": key,
                    "classification": classification,
                    "last_error": last_error,
                    "last_ok_at": last_ok_at,
                    "full_count_7d": full_count_7d,
                    "teaser_count_7d": teaser_count_7d,
                    "blocked_count_7d": blocked_count_7d,
                    "feed_full_count_7d": feed_full_count_7d,
                    "feed_teaser_count_7d": feed_teaser_count_7d,
                    "feed_blocked_count_7d": feed_blocked_count_7d,
                }
            )

        sources_payload.append(
            {
                "source_key": key,
                "enabled": cfg.enabled,
                "type": cfg.source_type,
                "discovered_24h": discovered,
                "fetched_24h": fetched,
                "saved_24h": saved,
                "blocked_24h": blocked,
                "failed_24h": failed,
                "fail_rate_24h": fail_rate,
                "last_error": last_error,
                "last_ok_at": last_ok_at,
                "classification": classification,
                "full_count_7d": full_count_7d,
                "teaser_count_7d": teaser_count_7d,
                "blocked_count_7d": blocked_count_7d,
                "feed_full_count_7d": feed_full_count_7d,
                "feed_teaser_count_7d": feed_teaser_count_7d,
                "feed_blocked_count_7d": feed_blocked_count_7d,
            }
        )

    sources_payload.sort(
        key=lambda x: (x.get("saved_24h") or 0, x.get("source_key")), reverse=True
    )

    return {
        "generated_at": _iso_z(_utc_now()),
        "window_hours": window_hours,
        "sources": sources_payload,
        "unhealthy": unhealthy,
    }

def compute_coverage(days: int = 7) -> dict[str, Any]:
    sb = get_client()
    repo_root = Path(__file__).resolve().parents[1]
    configs = load_source_configs(repo_root)

    source_col, _ = _detect_article_source_mode(sb)
    ts_col = _detect_article_ts_col(sb)

    id_to_key, source_name_to_key, source_key_set = _db_source_maps(sb)
    source_id_is_key = False
    if source_col == "source_id" and source_key_set:
        try:
            sample = (
                sb.table("articles")
                .select("source_id")
                .limit(20)
                .execute()
                .data
                or []
            )
            for row in sample:
                sid = row.get("source_id")
                if sid and sid in source_key_set:
                    source_id_is_key = True
                    break
        except Exception:
            source_id_is_key = False

    article_stats: dict[str, dict[str, Any]] = {}
    if source_col and ts_col:
        dsn = os.getenv("DATABASE_URL")
        if dsn:
            try:
                article_stats = _query_article_stats_psycopg2(
                    dsn,
                    source_col,
                    ts_col,
                    days,
                    key_col=get_key_column(sb),
                    source_id_is_key=source_id_is_key,
                )
            except Exception:
                article_stats = _query_article_stats_supabase(
                    sb,
                    source_col,
                    ts_col,
                    days,
                    id_to_key=id_to_key,
                    source_name_to_key=source_name_to_key,
                    source_key_set=source_key_set,
                )
        else:
            article_stats = _query_article_stats_supabase(
                sb,
                source_col,
                ts_col,
                days,
                id_to_key=id_to_key,
                source_name_to_key=source_name_to_key,
                source_key_set=source_key_set,
            )

    ingest_stats, has_ingest_stats = _fetch_ingest_stats(sb)

    sources_payload = []
    unhealthy = []

    now = _utc_now()
    stale_cutoff = now - timedelta(hours=48)
    articles_7d_total = 0
    sources_with_articles_7d = 0

    for key, cfg in configs.items():
        stats = article_stats.get(key, {})
        articles_7d = int(stats.get("articles_7d") or 0)
        last_article_at = stats.get("last_article_at")

        ingest = ingest_stats.get(key) if has_ingest_stats else None
        last_ingest_ok_at = ingest.get("last_ingest_ok_at") if ingest else None
        last_ingest_status = ingest.get("last_ingest_status") if ingest else None
        last_ingest_run_id = ingest.get("last_ingest_run_id") if ingest else None
        if has_ingest_stats and not last_ingest_status:
            last_ingest_status = "unknown"

        fail_rate_24h = None
        if ingest and ingest.get("attempted", 0) > 0:
            fail_rate_24h = ingest.get("failed", 0) / ingest.get("attempted", 0)

        notes = None
        reason = None
        if articles_7d == 0:
            reason = "no_articles_7d"
        else:
            last_dt = None
            if last_article_at:
                try:
                    last_dt = datetime.fromisoformat(last_article_at.replace("Z", "+00:00"))
                except Exception:
                    last_dt = None
            if not last_dt or last_dt < stale_cutoff:
                reason = "stale_48h"
        if reason is None and fail_rate_24h is not None and fail_rate_24h > 0.5:
            reason = "high_fail_rate_24h"

        if reason:
            notes = reason
            unhealthy.append(
                {
                    "source_key": key,
                    "reason": reason,
                    "last_article_at": last_article_at,
                    "last_ingest_ok_at": last_ingest_ok_at,
                }
            )

        sources_payload.append(
            {
                "source_key": key,
                "enabled": cfg.enabled,
                "type": cfg.source_type,
                "last_article_at": last_article_at,
                "articles_7d": articles_7d,
                "last_ingest_ok_at": last_ingest_ok_at,
                "last_ingest_run_id": last_ingest_run_id,
                "last_ingest_status": last_ingest_status,
                "fail_rate_24h": fail_rate_24h,
                "notes": notes,
            }
        )

        articles_7d_total += articles_7d
        if articles_7d > 0:
            sources_with_articles_7d += 1

    sources_payload.sort(
        key=lambda x: (x.get("articles_7d", 0), x.get("source_key")), reverse=True
    )

    orphaned_sources = []
    if source_key_set:
        for key in sorted(source_key_set):
            if key not in configs:
                orphaned_sources.append({"source_key": key})

    return {
        "generated_at": _iso_z(_utc_now()),
        "window_days": days,
        "totals": {
            "sources_configured": len(configs),
            "sources_with_articles_7d": sources_with_articles_7d,
            "articles_7d": articles_7d_total,
        },
        "sources": sources_payload,
        "unhealthy": unhealthy,
        "orphaned_sources": orphaned_sources,
    }

import hashlib
import json
import os
import random
import time
from datetime import datetime, timezone, timedelta

import httpx
from dotenv import load_dotenv
from supabase import create_client as _create_client

_ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(_ENV_PATH)

_source_cache: dict[str, str] = {}
_key_column: str | None = None
_sb = None


def create_client(url: str | None = None, key: str | None = None):
    url = url or os.getenv("SUPABASE_URL")
    key = key or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    if not url or not key:
        missing = []
        if not url:
            missing.append("SUPABASE_URL")
        if not (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")):
            missing.append("SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY")
        raise RuntimeError(f"Missing {', '.join(missing)}")
    return _create_client(url, key)


def get_client():
    global _sb
    if _sb:
        return _sb

    _sb = create_client()
    return _sb


def _table_exists(sb, table: str) -> bool:
    try:
        cols = sb.rpc("get_columns", {"p_table": table}).execute().data or []
    except Exception:
        return False
    return bool(cols)


def _normalize_entity(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def get_article_id_by_url(sb, url: str | None) -> int | None:
    if not url:
        return None
    try:
        res = sb.table("articles").select("id").eq("url", url).limit(1).execute()
    except Exception:
        return None
    if res.data:
        return res.data[0].get("id")
    return None


def upsert_entities_for_article(sb, article_id: int | None, ents: dict | None) -> None:
    if not article_id or not ents:
        return
    if not _table_exists(sb, "entities") or not _table_exists(sb, "article_entities"):
        return

    type_map = {
        "orgs": "org",
        "people": "person",
        "locations": "location",
        "topics": "topic",
    }
    rows: list[dict] = []
    normals: list[str] = []
    for key, entity_type in type_map.items():
        for name in (ents.get(key) or []):
            norm = _normalize_entity(str(name))
            if not norm:
                continue
            rows.append({"name": str(name), "type": entity_type, "normalized_name": norm})
            normals.append(norm)

    if not rows:
        return

    try:
        sb.table("entities").upsert(rows, on_conflict="normalized_name").execute()
    except Exception:
        return

    try:
        res = (
            sb.table("entities")
            .select("id,normalized_name")
            .in_("normalized_name", list(set(normals)))
            .execute()
        )
    except Exception:
        return

    ids = [row.get("id") for row in (res.data or []) if row.get("id")]
    if not ids:
        return

    link_rows = [{"article_id": article_id, "entity_id": eid} for eid in ids]
    try:
        sb.table("article_entities").upsert(
            link_rows, on_conflict="article_id,entity_id"
        ).execute()
    except Exception:
        return


def content_hash(payload: dict) -> str:
    stable = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(stable).hexdigest()


def upsert_feed_item(sb, item: dict) -> str | None:
    # item: must include source_type, external_id OR hash, published_at
    if not item.get("hash"):
        item["hash"] = content_hash(
            {
                "title": item.get("title"),
                "summary": item.get("summary"),
                "content": item.get("content"),
                "url": item.get("url"),
                "external_id": item.get("external_id"),
            }
        )

    if not item.get("published_at"):
        item["published_at"] = datetime.utcnow().isoformat()

    if item.get("external_id"):
        sb.table("feed_items").upsert(item, on_conflict="external_id").execute()
        lookup_col = "external_id"
        lookup_val = item.get("external_id")
    else:
        sb.table("feed_items").upsert(item, on_conflict="hash").execute()
        lookup_col = "hash"
        lookup_val = item.get("hash")

    if lookup_val is None:
        return None

    res = sb.table("feed_items").select("id").eq(lookup_col, lookup_val).limit(1).execute()
    if res.data:
        return res.data[0]["id"]
    return None


def enqueue_fetch(sb, source_id: str | None, url: str | None, reason: str) -> None:
    if not source_id or not url:
        return
    if not _table_exists(sb, "fetch_queue"):
        return
    row = {"source_id": source_id, "url": url, "reason": reason}
    try:
        sb.table("fetch_queue").insert(row).execute()
    except Exception:
        return


def _detect_key_column(sb) -> str:
    global _key_column
    if _key_column:
        return _key_column

    cols = sb.rpc("get_columns", {"p_table": "sources"}).execute().data or []
    names = {c["column_name"] for c in cols}
    if "key" in names:
        _key_column = "key"
        return _key_column
    if "source_key" in names:
        _key_column = "source_key"
        return _key_column

    raise RuntimeError("sources table missing 'key' or 'source_key'")


def get_key_column(sb) -> str:
    return _detect_key_column(sb)


def get_source_id(sb, key: str) -> str:
    if key in _source_cache:
        return _source_cache[key]

    key_col = _detect_key_column(sb)
    res = sb.table("sources").select("id").eq(key_col, key).limit(1).execute()
    if not res.data:
        raise ValueError(f"Unknown source key: {key}")

    _source_cache[key] = res.data[0]["id"]
    return _source_cache[key]


def _is_transient_run_row_error(err: Exception) -> bool:
    if isinstance(err, (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError)):
        return True
    msg = str(err)
    transient_markers = [
        "UNEXPECTED_EOF_WHILE_READING",
        "SSL",
        "Connection reset",
        "Broken pipe",
        "timeout",
    ]
    return any(m in msg for m in transient_markers)


def _run_row_retry(fn, *args, **kwargs):
    delays = [1, 2, 4, 8, 16]
    for i, delay in enumerate(delays, start=1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if not _is_transient_run_row_error(e) or i == len(delays):
                raise
            jitter = random.uniform(0, 0.2)
            print(f"RUN_ROW_RETRY attempt={i} error={str(e)[:200]}")
            time.sleep(delay + jitter)


def start_ingest_run(sb, job_name: str) -> str | None:
    try:
        res = _run_row_retry(
            sb.table("ingest_runs").insert, {"job_name": job_name}
        ).execute()
        return res.data[0]["id"]
    except Exception:
        print("RUN_ROW_UNAVAILABLE proceeding_without_run_row=1")
        return None


def finish_ingest_run(
    sb, run_id: str | None, ok: bool, stats: dict, error: str | None = None
):
    if not run_id:
        return
    payload = {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok,
        "stats": stats or {},
        "error": error,
    }
    try:
        _run_row_retry(
            sb.table("ingest_runs").update, payload
        ).eq("id", run_id).execute()
    except Exception as e:
        if _is_transient_run_row_error(e):
            print("RUN_ROW_UNAVAILABLE finish_failed=1")
        else:
            print(f"RUN_ROW_UNAVAILABLE finish_failed=1 error={str(e)[:200]}")


def should_extract_entities(item: dict) -> bool:
    if os.getenv("EXTRACT_ENTITIES") != "1":
        return False
    ent = item.get("entities")
    if isinstance(ent, dict) and (
        ent.get("orgs") or ent.get("people") or ent.get("locations") or ent.get("topics")
    ):
        return False
    return True


def is_source_in_cooldown(sb, source_key: str) -> bool:
    try:
        res = (
            sb.table("source_health")
            .select("cooldown_until")
            .eq("source_key", source_key)
            .limit(1)
            .execute()
        )
        if not res.data:
            return False
        cooldown_until = res.data[0].get("cooldown_until")
        if not cooldown_until:
            return False
        return cooldown_until > datetime.now(timezone.utc).isoformat()
    except Exception:
        return False


def mark_source_blocked(sb, source_key: str, cooldown_hours: int = 24) -> None:
    try:
        now = datetime.now(timezone.utc)
        cooldown_until = (now + timedelta(hours=cooldown_hours)).isoformat()
        res = (
            sb.table("source_health")
            .select("blocked_count_24h")
            .eq("source_key", source_key)
            .limit(1)
            .execute()
        )
        count = 0
        if res.data:
            count = int(res.data[0].get("blocked_count_24h") or 0)
        payload = {
            "source_key": source_key,
            "last_blocked_at": now.isoformat(),
            "blocked_count_24h": count + 1,
            "cooldown_until": cooldown_until,
        }
        sb.table("source_health").upsert(payload, on_conflict="source_key").execute()
    except Exception:
        return None

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from backend.db import supabase_client

load_dotenv()

SOCIAL_PATH = BASE_DIR / "ingest" / "sources_social.json"
RUNNER_SOURCES_PATH = BASE_DIR / "runner" / "ingest" / "sources.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def detect_key_column(sb) -> str:
    cols = sb.rpc("get_columns", {"p_table": "sources"}).execute().data or []
    names = {c["column_name"] for c in cols}
    if "key" in names:
        return "key"
    if "source_key" in names:
        return "source_key"
    raise RuntimeError("sources table missing 'key' or 'source_key'")


def normalize_source_type(raw: str | None, src: dict) -> str:
    if not raw:
        return "web"

    x = raw.strip().lower()

    if x in {"rss", "feed"}:
        return "rss"

    if x in {"web", "site", "website", "page"}:
        return "web"

    if x in {"facebook", "fb", "social", "social_inbox", "inbox"}:
        return "social_inbox"

    if x in {"news", "article", "articles"}:
        rss_url = src.get("rss_url")
        url = src.get("url") or ""
        return "rss" if rss_url or url.endswith(".xml") else "web"

    return "web"


def normalize_social(item: dict) -> dict:
    return {
        "key": item.get("id"),
        "name": item.get("name"),
        "source_type": item.get("type"),
        "language": item.get("language"),
        "url": item.get("page_url"),
        "rss_url": item.get("rss"),
        "meta": item,
        "is_active": bool(item.get("enabled", True)),
    }


def normalize_runner(item: dict) -> dict | None:
    ingest_method = (item.get("ingest_method") or "").lower()
    if "facebook" in ingest_method or item.get("type") == "facebook":
        return None

    if item.get("rss") or ingest_method == "rss":
        source_type = "rss"
        url = item.get("rss") or item.get("url")
    else:
        source_type = "web"
        url = item.get("url")

    return {
        "key": item.get("id"),
        "name": item.get("name"),
        "source_type": item.get("type") or source_type,
        "url": url,
        "language": item.get("language"),
        "rss_url": item.get("rss"),
        "meta": item,
        "is_active": bool(item.get("enabled", True)),
    }


def chunked(rows: list[dict], size: int = 100):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", help="Path to runner sources.json")
    parser.add_argument("--social-sources", help="Path to sources_social.json")
    args = parser.parse_args()

    sb = supabase_client()
    key_col = detect_key_column(sb)

    social_path = Path(args.social_sources) if args.social_sources else SOCIAL_PATH
    runner_path = Path(args.sources) if args.sources else RUNNER_SOURCES_PATH

    social_sources = load_json(social_path) if social_path.exists() else []
    runner_sources = load_json(runner_path) if runner_path.exists() else []

    rows: list[dict] = []

    for item in social_sources:
        row = normalize_social(item)
        if row.get("key"):
            rows.append(row)

    for item in runner_sources:
        row = normalize_runner(item)
        if row and row.get("key"):
            rows.append(row)

    if not rows:
        print("No sources to seed.")
        return 0

    inserted = 0
    for batch in chunked(rows, size=100):
        payload = []
        for row in batch:
            raw_type = row.get("source_type") or row.get("type")
            st = normalize_source_type(raw_type, row)
            payload.append(
                {
                    key_col: row.get("key"),
                    "name": row.get("name"),
                    "source_type": st,
                    "language": row.get("language"),
                    "url": row.get("url"),
                    "meta": row.get("meta", {}),
                    "is_active": row.get("is_active", True),
                }
            )
        sb.table("sources").upsert(payload, on_conflict=key_col).execute()
        inserted += len(batch)

    print(f"Seeded/updated {inserted} sources.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

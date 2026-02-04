#!/usr/bin/env python3
import argparse
import os
import re
from typing import List
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras


def parse_list(value: str) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def validate_uuid(value: str) -> bool:
    return bool(
        re.match(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
            value,
        )
    )


def db_host_allowed(dsn: str, allowlist: List[str]) -> bool:
    host = urlparse(dsn).hostname
    if not host:
        return False
    return host in allowlist


def main() -> int:
    parser = argparse.ArgumentParser(description="Upsert user alert preferences.")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--dedupe-window-sec", type=int)
    parser.add_argument("--immediate-priorities")
    parser.add_argument("--digest-priorities")
    parser.add_argument("--priority-categories")
    parser.add_argument("--clear-priority-categories", action="store_true")
    parser.add_argument("--digest-schedule")
    parser.add_argument("--channels-enabled")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if not validate_uuid(args.user_id):
        print("Invalid --user-id (expected UUID)")
        return 1

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL is required")
        return 1

    allowlist_raw = os.getenv("PREFS_ALLOWED_DB_HOSTS", "").strip()
    if allowlist_raw:
        allowlist = [h.strip() for h in allowlist_raw.split(",") if h.strip()]
        if not db_host_allowed(dsn, allowlist):
            if args.force:
                print("WARNING: DATABASE_URL host not in allowlist, proceeding due to --force")
            else:
                print("ERROR: DATABASE_URL host not in allowlist (set PREFS_ALLOWED_DB_HOSTS or use --force)")
                return 1
    else:
        print("WARNING: PREFS_ALLOWED_DB_HOSTS not set; allowlist not enforced")

    values = {
        "dedupe_window_sec": args.dedupe_window_sec,
        "immediate_priorities": parse_list(args.immediate_priorities or ""),
        "digest_priorities": parse_list(args.digest_priorities or ""),
        "priority_categories": parse_list(args.priority_categories or ""),
        "digest_schedule": args.digest_schedule,
        "channels_enabled": parse_list(args.channels_enabled or ""),
    }

    if args.clear_priority_categories:
        values["priority_categories"] = None

    sql = """
                INSERT INTO public.user_alert_prefs (
                  user_id, dedupe_window_sec, immediate_priorities, digest_priorities,
                  priority_categories, digest_schedule, channels_enabled, updated_at
                )
                VALUES (
                  %s,
                  COALESCE(%s, 21600),
                  COALESCE(%s, ARRAY['P0']),
                  COALESCE(%s, ARRAY['P1','P2']),
                  NULLIF(%s, ARRAY[]::text[]),
                  COALESCE(%s, 'daily'),
                  COALESCE(%s, ARRAY['email']),
                  now()
                )
                ON CONFLICT (user_id)
                DO UPDATE SET
                  dedupe_window_sec = COALESCE(EXCLUDED.dedupe_window_sec, user_alert_prefs.dedupe_window_sec),
                  immediate_priorities = COALESCE(EXCLUDED.immediate_priorities, user_alert_prefs.immediate_priorities),
                  digest_priorities = COALESCE(EXCLUDED.digest_priorities, user_alert_prefs.digest_priorities),
                  priority_categories = COALESCE(EXCLUDED.priority_categories, user_alert_prefs.priority_categories),
                  digest_schedule = COALESCE(EXCLUDED.digest_schedule, user_alert_prefs.digest_schedule),
                  channels_enabled = COALESCE(EXCLUDED.channels_enabled, user_alert_prefs.channels_enabled),
                  updated_at = now()
                RETURNING *
                """
    params = (
        args.user_id,
        values["dedupe_window_sec"],
        values["immediate_priorities"] or None,
        values["digest_priorities"] or None,
        values["priority_categories"] or None,
        values["digest_schedule"],
        values["channels_enabled"] or None,
    )

    if args.dry_run:
        print("DRY RUN")
        print(sql.strip())
        print(params)
        return 0

    with psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()

    print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

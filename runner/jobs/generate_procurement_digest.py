#!/usr/bin/env python3
import argparse
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2


STATE_PATH = Path("/var/lib/libyaintel/last_procurement_digest_at.txt")
OUT_PATH = Path("/var/lib/libyaintel/procurement_digest.md")


def utcnow():
    return datetime.now(timezone.utc)


def load_last_run():
    try:
        s = STATE_PATH.read_text(encoding="utf-8").strip()
        return datetime.fromisoformat(s)
    except Exception:
        return utcnow() - timedelta(days=7)


def save_last_run(ts: datetime):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(ts.isoformat(), encoding="utf-8")


def md_escape(s: str) -> str:
    return (s or "").replace("\n", " ").strip()


def run(mode: str):
    db_url = os.environ["DATABASE_URL"]
    now = utcnow()
    window_start = now - timedelta(days=7)

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          source,
          buyer,
          COALESCE(sector, 'unknown') AS sector,
          COALESCE(title_en, title, '') AS title,
          publish_date,
          deadline_date,
          COALESCE(attachments_count, 0) AS attachments_count,
          url,
          created_at
        FROM tenders
        WHERE created_at >= (now() at time zone 'utc') - interval '7 days'
        ORDER BY buyer, created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    by_buyer = {}
    for r in rows:
        source, buyer, sector, title, pub, deadline, attc, url, created_at = r
        key = buyer or source
        by_buyer.setdefault(key, {"sector": sector, "items": []})
        by_buyer[key]["items"].append(
            {
                "title": title,
                "publish_date": pub,
                "deadline_date": deadline,
                "attachments_count": attc,
                "url": url,
                "created_at": created_at,
            }
        )

    lines = []
    lines.append("# LibyaIntel Procurement Digest")
    lines.append("")
    lines.append(f"**Window:** {window_start.date().isoformat()} -> {now.date().isoformat()}")
    lines.append(f"**Total new items:** {len(rows)}")
    lines.append("")

    if by_buyer:
        lines.append("## Summary by buyer")
        for buyer in sorted(by_buyer.keys()):
            sector = by_buyer[buyer]["sector"]
            count = len(by_buyer[buyer]["items"])
            lines.append(f"- **{md_escape(buyer)}** ({md_escape(sector)}): {count}")
        lines.append("")
    else:
        lines.append("_No new tenders in this window._")
        lines.append("")

    for buyer in sorted(by_buyer.keys()):
        sector = by_buyer[buyer]["sector"]
        items = by_buyer[buyer]["items"]
        lines.append(f"## {md_escape(buyer)}")
        lines.append(f"**Sector:** {md_escape(sector)}  |  **Count:** {len(items)}")
        lines.append("")
        lines.append("| Title | Published | Deadline | Attachments | Link |")
        lines.append("|---|---:|---:|---:|---|")

        for it in items[:200]:
            title = md_escape(it["title"])[:160] or "(no title)"
            pub = it["publish_date"].isoformat() if it["publish_date"] else ""
            dl = it["deadline_date"].isoformat() if it["deadline_date"] else ""
            deadline_cell = dl
            attc = str(it["attachments_count"] or 0)
            url = it["url"] or ""
            lines.append(f"| {title} | {pub} | {deadline_cell} | {attc} | {url} |")
        lines.append("")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(lines)
    try:
        OUT_PATH.write_text(body, encoding="utf-8")
    except PermissionError as exc:
        raise SystemExit(
            f"permission denied writing {OUT_PATH}. "
            "Run: sudo mkdir -p /var/lib/libyaintel && "
            "sudo chown -R libyaintel:libyaintel /var/lib/libyaintel"
        ) from exc

    subject_prefix = os.getenv("DIGEST_SUBJECT_PREFIX", "[LibyaIntel] Procurement Digest").strip()
    subject = f"{subject_prefix} ({now.date().isoformat()})"
    if mode != "demo":
        send_resend_email(body, subject)
        save_last_run(now)
    print(
        f"DIGEST_OK path={OUT_PATH} items={len(rows)} buyers={len(by_buyer)} "
        f"window_start={window_start.isoformat()}"
    )


def send_resend_email(markdown_body: str, subject: str):
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        print("DIGEST_EMAIL_SKIP missing RESEND_API_KEY")
        return

    to_raw = os.getenv("DIGEST_TO", "").strip()
    if not to_raw:
        print("DIGEST_EMAIL_SKIP missing DIGEST_TO")
        return

    try:
        import resend
    except Exception as exc:
        print(f"DIGEST_EMAIL_FAIL missing resend dependency err={exc}")
        return

    resend.api_key = api_key

    from_addr = os.getenv("DIGEST_FROM", "LibyaIntel <digest@libyaintel.io>")
    to_list = [x.strip() for x in to_raw.split(",") if x.strip()]
    max_retries = int(os.getenv("DIGEST_SEND_MAX_RETRIES", "5"))
    base_sleep = int(os.getenv("DIGEST_SEND_RETRY_BASE_SECONDS", "3"))

    payload = {
        "from": from_addr,
        "to": to_list,
        "subject": subject,
        "text": markdown_body,
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = resend.Emails.send(payload)
            msg_id = resp.get("id") if isinstance(resp, dict) else ""
            print(f"DIGEST_EMAIL_OK to={len(to_list)} attempt={attempt} id={msg_id}")
            return
        except Exception as exc:
            if attempt == max_retries:
                print(f"DIGEST_EMAIL_FAIL to={len(to_list)} attempt={attempt} err={exc}")
                return
            sleep_s = base_sleep * attempt
            print(f"DIGEST_EMAIL_RETRY to={len(to_list)} attempt={attempt} sleep={sleep_s}s err={exc}")
            time.sleep(sleep_s)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["live", "demo"],
        default="live",
        help="Use demo mode to skip sending email and not update last-run state",
    )
    args = parser.parse_args()
    run(args.mode)

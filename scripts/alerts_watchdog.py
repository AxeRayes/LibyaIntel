#!/usr/bin/env python3
import argparse
import json
import os
import time
from pathlib import Path
from typing import Tuple

import requests


HEARTBEAT_FILE = Path(os.getenv("ALERTS_HEARTBEAT_FILE", "/var/lib/libyaintel/alerts_last_ok.txt"))
STATE_FILE = Path("/var/lib/libyaintel/alerts_stale_notify.json")


def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def stale_warn_sec() -> int:
    return int(env("ALERTS_STALE_WARN_SEC", "900") or 900)


def cooldown_sec() -> int:
    return int(env("ALERTS_STALE_NOTIFY_COOLDOWN_SEC", "3600") or 3600)


def should_notify(key: str) -> bool:
    now = int(time.time())
    try:
        data = json.loads(STATE_FILE.read_text())
    except Exception:
        data = {}
    last = int(data.get(key, 0))
    if now - last < cooldown_sec():
        return False
    data[key] = now
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data))
    return True


def set_state(key: str, value) -> None:
    try:
        data = json.loads(STATE_FILE.read_text())
    except Exception:
        data = {}
    if value is None:
        data.pop(key, None)
    else:
        data[key] = value
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data))


def get_state(key: str, default=None):
    try:
        data = json.loads(STATE_FILE.read_text())
    except Exception:
        data = {}
    return data.get(key, default)


def send_admin_telegram(text: str) -> Tuple[bool, str]:
    token = env("ALERTS_ADMIN_TELEGRAM_BOT_TOKEN")
    chat_id = env("ALERTS_ADMIN_TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False, "not_configured"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=10)
    if 200 <= resp.status_code < 300:
        return True, ""
    return False, f"status_{resp.status_code}"


def send_admin_email(subject: str, text: str) -> Tuple[bool, str]:
    emails_raw = env("ALERTS_ADMIN_EMAILS")
    if not emails_raw:
        return False, "not_configured"
    emails = [e.strip() for e in emails_raw.split(",") if e.strip()]
    if not emails:
        return False, "not_configured"
    api_key = env("RESEND_API_KEY")
    from_email = env("ALERTS_FROM_EMAIL", "alerts@libyaintel.com")
    if not api_key or not from_email:
        return False, "not_configured"
    prefix = env("ALERTS_ADMIN_EMAIL_SUBJECT_PREFIX")
    full_subject = f"{prefix} {subject}".strip()
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    errors = []
    for email in emails:
        payload = {"from": from_email, "to": [email], "subject": full_subject, "text": text}
        resp = requests.post("https://api.resend.com/emails", headers=headers, data=json.dumps(payload), timeout=20)
        if not (200 <= resp.status_code < 300):
            errors.append(email)
    if errors:
        return False, "send_failed"
    return True, ""


def run_watchdog() -> int:
    if not HEARTBEAT_FILE.exists():
        now = int(time.time())
        first_missing = int(get_state("first_missing_at", now) or now)
        if get_state("first_missing_at") is None:
            set_state("first_missing_at", now)
        missing_age = now - first_missing
        print(f"ALERTS_WATCHDOG_NO_HEARTBEAT missing_age_sec={missing_age} warn_sec={stale_warn_sec()}")
        if missing_age >= stale_warn_sec():
            if should_notify("alerts_stale_missing"):
                msg = (
                    "ALERTS_STALE_WARN\n"
                    f"reason=missing_heartbeat\n"
                    f"missing_age_sec={missing_age}\n"
                    f"warn_sec={stale_warn_sec()}\n"
                    "check: systemctl status libyaintel-alerts.service --no-pager"
                )
                ok, err = send_admin_telegram(msg)
                print(f"ALERTS_ADMIN_NOTIFY channel=telegram ok={1 if ok else 0} err={err}")
                ok, err = send_admin_email("ALERTS_STALE_WARN", msg)
                print(f"ALERTS_ADMIN_NOTIFY channel=email ok={1 if ok else 0} err={err}")
            return 1
        return 0

    if get_state("first_missing_at") is not None:
        set_state("first_missing_at", None)

    age_sec = int(time.time() - HEARTBEAT_FILE.stat().st_mtime)
    warn_sec = stale_warn_sec()
    if age_sec < warn_sec:
        print(f"ALERTS_STALE_OK age_sec={age_sec} warn_sec={warn_sec}")
        return 0

    print(f"ALERTS_STALE_WARN age_sec={age_sec} warn_sec={warn_sec}")
    if should_notify("alerts_stale"):
        msg = (
            "ALERTS_STALE_WARN\n"
            f"age_sec={age_sec}\n"
            f"warn_sec={warn_sec}\n"
            "check: systemctl status libyaintel-alerts.service --no-pager"
        )
        ok, err = send_admin_telegram(msg)
        print(f"ALERTS_ADMIN_NOTIFY channel=telegram ok={1 if ok else 0} err={err}")
        ok, err = send_admin_email("ALERTS_STALE_WARN", msg)
        print(f"ALERTS_ADMIN_NOTIFY channel=email ok={1 if ok else 0} err={err}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Alerts watchdog (staleness check).")
    parser.add_argument("--check", action="store_true", help="Run once and exit.")
    args = parser.parse_args()

    if args.check:
        return run_watchdog()
    return run_watchdog()


if __name__ == "__main__":
    raise SystemExit(main())

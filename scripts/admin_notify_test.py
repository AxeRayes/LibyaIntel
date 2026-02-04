#!/usr/bin/env python3
import argparse
import os

from backend.runner.alerts import deliver


def main() -> int:
    parser = argparse.ArgumentParser(description="Trigger admin give-up notification safely.")
    parser.add_argument("--alert-id", default="test-alert-1")
    parser.add_argument("--user-id", default="test-user-1")
    parser.add_argument("--channel", default="email")
    parser.add_argument("--target", default="test-target")
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--error", default="test_giveup_error")
    parser.add_argument("--cooldown", type=int, default=None, help="Override cooldown seconds for this run.")
    parser.add_argument("--dry-run", action="store_true", help="Print message only; do not send.")
    args = parser.parse_args()

    if not deliver.admin_notify_enabled():
        print("ADMIN_NOTIFY_SKIPPED reason=not_configured")
        return 0

    delivery = {
        "alert_id": args.alert_id,
        "user_id": args.user_id,
        "channel": args.channel,
        "target": args.target,
    }

    if args.cooldown is not None:
        os.environ["ALERTS_ADMIN_NOTIFY_COOLDOWN_SEC"] = str(args.cooldown)

    if args.dry_run:
        target = (delivery.get("target") or "").strip()
        message = (
            "ALERTS_DELIVERY_GIVEUP\n"
            f"alert_id={delivery.get('alert_id')}\n"
            f"user_id={delivery.get('user_id')}\n"
            f"channel={delivery.get('channel')}\n"
            f"target={target}\n"
            f"attempts={args.attempts}\n"
            f"error={args.error[:200]}\n"
            "check: journalctl -u libyaintel-alerts.service -n 200"
        )
        print(message)
        return 0

    deliver.notify_admin_giveup(delivery, args.error, args.attempts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

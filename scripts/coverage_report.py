import argparse
import json

from backend.coverage import compute_coverage


def _format_row(row: dict) -> str:
    fail_rate = row.get("fail_rate_24h")
    fail_str = "NA" if fail_rate is None else f"{fail_rate:.2f}"
    return (
        f"{row.get('articles_7d', 0):>6}  "
        f"{row.get('source_key',''):<28}  "
        f"{'Y' if row.get('enabled') else 'N':<1}  "
        f"{row.get('type',''):<6}  "
        f"{(row.get('last_article_at') or '-'):>20}  "
        f"{(row.get('last_ingest_ok_at') or '-'):>20}  "
        f"{fail_str:>6}  "
        f"{(row.get('notes') or '')}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Coverage report.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    payload = compute_coverage(days=args.days)

    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    print(
        "articles  source_key                    en  type    last_article_at       "
        "last_ingest_ok_at     fail24  notes"
    )
    for row in payload.get("sources", []):
        print(_format_row(row))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import os

from runner.ingest import page_ingest


def main() -> int:
    os.environ["SOURCE_IDS"] = "libya_review"
    os.environ["MAX_SOURCES"] = "1"
    os.environ["EXTRACT_ENTITIES"] = "0"
    os.environ["LR_RECENT_LIMIT"] = "2000"
    os.environ["LR_MAX_NEW"] = "200"
    os.environ["MAX_NEW_GLOBAL"] = os.getenv("MAX_NEW_GLOBAL", "200")
    os.environ["MIN_DOMAIN_DELAY_MS"] = os.getenv("MIN_DOMAIN_DELAY_MS", "200")
    os.environ["MAX_DOMAIN_DELAY_MS"] = os.getenv("MAX_DOMAIN_DELAY_MS", "500")
    return page_ingest.main()


if __name__ == "__main__":
    raise SystemExit(main())

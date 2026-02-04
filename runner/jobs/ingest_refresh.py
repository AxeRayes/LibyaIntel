import os

from runner.ingest import page_ingest


def main() -> int:
    os.environ["SOURCE_IDS"] = "libya_observer,libya_review"
    os.environ["MAX_SOURCES"] = "2"
    os.environ["EXTRACT_ENTITIES"] = "0"
    os.environ["LR_RECENT_LIMIT"] = "120"
    os.environ["LR_MAX_NEW"] = "15"
    os.environ["MAX_NEW_GLOBAL"] = os.getenv("MAX_NEW_GLOBAL", "200")
    return page_ingest.main()


if __name__ == "__main__":
    raise SystemExit(main())

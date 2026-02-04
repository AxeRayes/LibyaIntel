# Runner

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install feedparser requests beautifulsoup4
cp .env.example .env
```

## Run RSS ingest

```bash
python ingest/rss_ingest.py
```

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import requests
from bs4 import BeautifulSoup


CBL_FX_URL = "https://cbl.gov.ly/en/currency-exchange-rates/"
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
STOOQ_DAILY_CSV_URL = "https://stooq.com/q/d/l/"

PARALLEL_FX_PATH = Path("/etc/libyaintel/parallel_fx.json")


_NUM_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)")


@dataclass(frozen=True)
class Quote:
    instrument: str
    rate_type: str
    quote_currency: str
    value: float
    unit: str | None
    as_of: datetime
    source_name: str
    source_url: str
    status: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_date_utc(value: str) -> datetime:
    # Expect YYYY-MM-DD from upstreams we use.
    dt = datetime.fromisoformat(value.strip())
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_float(text: str) -> float | None:
    if not text:
        return None
    m = _NUM_RE.search(text.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _fetch_cbl_official_fx(timeout_s: int) -> list[Quote]:
    r = requests.get(
        CBL_FX_URL,
        timeout=timeout_s,
        headers={"User-Agent": "Mozilla/5.0 (LibyaIntel market quotes)"},
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    table = soup.find("table", {"id": "currency-table"})
    if table is None:
        raise RuntimeError("CBL_PARSE_FAIL missing currency-table")

    # CBL uses names in the "Currency" column, typically prefixed with "Currency:".
    name_to_instrument = {
        "american dollar": "USD",
        "euro": "EUR",
        "pound": "GBP",
        "egyptian pound": "EGP",
        "tunisian dinar": "TND",
    }

    out: list[Quote] = []
    rows = table.find_all("tr")
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        date_s = tds[0].get_text(" ", strip=True)
        currency_name = tds[1].get_text(" ", strip=True)
        unit_s = tds[2].get_text(" ", strip=True)
        avg_s = tds[3].get_text(" ", strip=True)

        date_s = re.sub(r"^date:\s*", "", date_s.strip(), flags=re.I)

        currency_key = currency_name.strip().lower()
        currency_key = re.sub(r"^currency:\s*", "", currency_key)
        instrument = name_to_instrument.get(currency_key)
        if not instrument:
            continue

        value = _parse_float(avg_s)
        if value is None:
            continue

        # Normalize as_of to midnight UTC.
        as_of = _parse_date_utc(date_s).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        out.append(
            Quote(
                instrument=instrument,
                rate_type="official",
                quote_currency="LYD",
                value=value,
                unit=f"LYD per 1 {instrument}",
                as_of=as_of,
                source_name="CBL",
                source_url=CBL_FX_URL,
                status="ok",
            )
        )

    return out


def _read_parallel_manual_fx() -> list[Quote]:
    mode = (os.getenv("PARALLEL_FX_MODE") or "manual").strip().lower()
    if mode != "manual":
        return []
    if not PARALLEL_FX_PATH.exists():
        return []

    payload = json.loads(PARALLEL_FX_PATH.read_text(encoding="utf-8"))
    as_of_raw = str(payload.get("as_of") or "").strip()
    as_of = _utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
    if as_of_raw:
        try:
            as_of = _parse_date_utc(as_of_raw)
        except Exception:
            pass

    source_url = str(payload.get("source_url") or "").strip() or "manual"

    out: list[Quote] = []
    for instrument in ("USD", "EUR", "GBP", "EGP", "TND"):
        val = payload.get(instrument)
        try:
            value = float(val)
        except Exception:
            continue
        out.append(
            Quote(
                instrument=instrument,
                rate_type="parallel",
                quote_currency="LYD",
                value=value,
                unit=f"LYD per 1 {instrument}",
                as_of=as_of,
                source_name="Parallel FX (Manual)",
                source_url=source_url,
                status="ok",
            )
        )
    return out


def _fetch_fred_series(series_id: str, timeout_s: int) -> tuple[datetime, float]:
    r = requests.get(
        FRED_CSV_URL,
        params={"id": series_id},
        timeout=timeout_s,
        headers={"User-Agent": "Mozilla/5.0 (LibyaIntel market quotes)"},
    )
    r.raise_for_status()
    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"FRED_EMPTY series={series_id}")

    # Walk backwards to find latest non-missing.
    for ln in reversed(lines[1:]):
        parts = ln.split(",", 1)
        if len(parts) != 2:
            continue
        date_s, val_s = parts[0].strip(), parts[1].strip()
        if not val_s or val_s == ".":
            continue
        try:
            value = float(val_s)
        except ValueError:
            continue
        as_of = _parse_date_utc(date_s).replace(hour=0, minute=0, second=0, microsecond=0)
        return as_of, value

    raise RuntimeError(f"FRED_NO_VALUE series={series_id}")


def _fetch_stooq_daily(symbol: str, timeout_s: int) -> tuple[datetime, float]:
    r = requests.get(
        STOOQ_DAILY_CSV_URL,
        params={"s": symbol, "i": "d"},
        timeout=timeout_s,
        headers={"User-Agent": "Mozilla/5.0 (LibyaIntel market quotes)"},
    )
    r.raise_for_status()
    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]
    if len(lines) < 2:
        raise RuntimeError(f"STOOQ_EMPTY symbol={symbol}")
    header = lines[0].split(",")
    # Expect Date,Open,High,Low,Close
    try:
        close_idx = header.index("Close")
    except ValueError:
        close_idx = 4
    last = lines[-1].split(",")
    if len(last) <= close_idx:
        raise RuntimeError(f"STOOQ_PARSE_FAIL symbol={symbol}")
    as_of = _parse_date_utc(last[0]).replace(hour=0, minute=0, second=0, microsecond=0)
    value = float(last[close_idx])
    return as_of, value


def _upsert_quotes(conn, quotes: list[Quote]) -> int:
    if not quotes:
        return 0
    cur = conn.cursor()
    updated = 0
    for q in quotes:
        cur.execute(
            """
            INSERT INTO market_quotes
              (instrument, rate_type, quote_currency, value, unit, as_of, source_name, source_url, status, fetched_at)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (instrument, rate_type, quote_currency)
            DO UPDATE SET
              value = EXCLUDED.value,
              unit = EXCLUDED.unit,
              as_of = EXCLUDED.as_of,
              source_name = EXCLUDED.source_name,
              source_url = EXCLUDED.source_url,
              status = EXCLUDED.status,
              fetched_at = now()
            """,
            (
                q.instrument,
                q.rate_type,
                q.quote_currency,
                q.value,
                q.unit,
                q.as_of,
                q.source_name,
                q.source_url,
                q.status,
            ),
        )
        updated += 1
    return updated


def main() -> int:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("MARKET_QUOTES_FAIL err=missing_DATABASE_URL")
        return 2

    stale_after_min = int(os.getenv("MARKET_QUOTES_STALE_AFTER_MIN") or "180")
    timeout_s = int(os.getenv("MARKET_QUOTES_FETCH_TIMEOUT") or "15")

    quotes: list[Quote] = []
    try:
        quotes += _fetch_cbl_official_fx(timeout_s=timeout_s)
    except Exception as e:
        # Keep last known values; staleness will be handled below.
        print(f"MARKET_QUOTES_PARTIAL source=CBL err={type(e).__name__}")

    try:
        quotes += _read_parallel_manual_fx()
    except Exception as e:
        print(f"MARKET_QUOTES_PARTIAL source=PARALLEL_MANUAL err={type(e).__name__}")

    # Metals (Stooq daily)
    try:
        as_of, value = _fetch_stooq_daily("xauusd", timeout_s=timeout_s)
        quotes.append(
            Quote(
                instrument="XAU",
                rate_type="spot",
                quote_currency="USD",
                value=value,
                unit="USD per oz",
                as_of=as_of,
                source_name="Stooq",
                source_url=f"{STOOQ_DAILY_CSV_URL}?s=xauusd&i=d",
                status="ok",
            )
        )
    except Exception as e:
        print(f"MARKET_QUOTES_PARTIAL source=STOOQ_XAU err={type(e).__name__}")

    try:
        as_of, value = _fetch_stooq_daily("xagusd", timeout_s=timeout_s)
        quotes.append(
            Quote(
                instrument="XAG",
                rate_type="spot",
                quote_currency="USD",
                value=value,
                unit="USD per oz",
                as_of=as_of,
                source_name="Stooq",
                source_url=f"{STOOQ_DAILY_CSV_URL}?s=xagusd&i=d",
                status="ok",
            )
        )
    except Exception as e:
        print(f"MARKET_QUOTES_PARTIAL source=STOOQ_XAG err={type(e).__name__}")

    # Commodities (FRED daily; no API key required for CSV export).
    try:
        as_of, value = _fetch_fred_series("DCOILBRENTEU", timeout_s=timeout_s)
        quotes.append(
            Quote(
                instrument="BRENT",
                rate_type="spot",
                quote_currency="USD",
                value=value,
                unit="USD per bbl",
                as_of=as_of,
                source_name="FRED (EIA)",
                source_url=f"{FRED_CSV_URL}?id=DCOILBRENTEU",
                status="ok",
            )
        )
    except Exception as e:
        print(f"MARKET_QUOTES_PARTIAL source=FRED_BRENT err={type(e).__name__}")

    try:
        as_of, value = _fetch_fred_series("DCOILWTICO", timeout_s=timeout_s)
        quotes.append(
            Quote(
                instrument="WTI",
                rate_type="spot",
                quote_currency="USD",
                value=value,
                unit="USD per bbl",
                as_of=as_of,
                source_name="FRED (EIA)",
                source_url=f"{FRED_CSV_URL}?id=DCOILWTICO",
                status="ok",
            )
        )
    except Exception as e:
        print(f"MARKET_QUOTES_PARTIAL source=FRED_WTI err={type(e).__name__}")

    try:
        as_of, value = _fetch_fred_series("DHHNGSP", timeout_s=timeout_s)
        quotes.append(
            Quote(
                instrument="NG_HH",
                rate_type="spot",
                quote_currency="USD",
                value=value,
                unit="USD per MMBtu",
                as_of=as_of,
                source_name="FRED (EIA)",
                source_url=f"{FRED_CSV_URL}?id=DHHNGSP",
                status="ok",
            )
        )
    except Exception as e:
        print(f"MARKET_QUOTES_PARTIAL source=FRED_NG_HH err={type(e).__name__}")

    now = _utc_now()
    stale_threshold = now - timedelta(minutes=stale_after_min)

    try:
        conn = psycopg2.connect(db_url)
        updated = _upsert_quotes(conn, quotes)

        cur = conn.cursor()
        cur.execute(
            "UPDATE market_quotes SET status='stale' WHERE fetched_at < %s AND status='ok'",
            (stale_threshold,),
        )
        stale = int(cur.rowcount or 0)
        conn.commit()
        conn.close()

        instruments = len({(q.instrument, q.rate_type, q.quote_currency) for q in quotes})
        print(f"MARKET_QUOTES_OK instruments={instruments} updated={updated} stale={stale}")
        return 0
    except Exception as e:
        print(f"MARKET_QUOTES_FAIL err={type(e).__name__}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

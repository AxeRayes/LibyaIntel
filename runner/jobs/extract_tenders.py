import json
import argparse
import os
import re
import subprocess
import tempfile
from datetime import datetime
from html import unescape
from pathlib import Path
from urllib.parse import urljoin

import psycopg2
import requests


CONFIG_PATH = Path(__file__).resolve().parents[1] / "ingest" / "procurement_sources.json"

DEADLINE_PATTERNS = [
    r"(آخر موعد|موعد تقديم العروض|تقديم العروض|آخر موعد للتقديم|آخر موعد لاستلام)[^\d]{0,20}(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
    r"(Submission deadline|Closing date)[^\d]{0,20}(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
]

SECTOR_KEYWORDS = {
    "oil": ["rig", "pipeline", "well", "drilling", "compressor", "refinery"],
    "utilities": ["generator", "transformer", "substation", "grid", "switchgear"],
    "ports": ["port", "terminal", "berth", "dredging"],
    "telecom": ["fiber", "tower", "core network", "radio", "telecom"],
}

AR_KEYWORDS = [
    "مناقصة",
    "عطاء",
    "توريد",
    "تأهيل",
    "إبداء الاهتمام",
    "إعلان",
    "لجنة العطاءات",
    "كراسة الشروط",
    "تقديم العروض",
]

AR_CORE_KEYWORDS = [
    "مناقصة",
    "عطاء",
    "توريد",
    "تأهيل",
    "إبداء الاهتمام",
    "لجنة العطاءات",
    "كراسة الشروط",
    "تقديم العروض",
]

ATTACH_RE = re.compile(r'href="([^"]+\.(?:pdf|docx?|jpg|jpeg|png))"', re.I)


def _load_source_meta() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        sources = json.load(f)
    meta = {}
    for s in sources:
        key = s.get("key")
        if not key:
            continue
        meta[key] = {
            "buyer": s.get("buyer") or s.get("name") or key,
            "sector": s.get("sector"),
        }
    return meta


def _parse_date(val: str | None):
    if not val:
        return None
    for fmt in (
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%m/%d %Y",
        "%m/%d/%Y",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(val, fmt).date()
        except Exception:
            continue
    return None


_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _normalize_digits(text: str) -> str:
    return text.translate(_ARABIC_DIGITS)


def extract_deadline(text: str | None):
    if not text:
        return None
    text = _normalize_digits(text)
    for p in DEADLINE_PATTERNS:
        m = re.search(p, text, re.I)
        if m:
            return _parse_date(m.group(2))
    return None


def contains_keywords(text: str) -> bool:
    return any(k in text for k in AR_KEYWORDS)


def contains_core_keywords(text: str) -> bool:
    return any(k in text for k in AR_CORE_KEYWORDS)


def html_to_text(html: str) -> str:
    if not html:
        return ""
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    html = unescape(html)
    html = re.sub(r"\s+", " ", html)
    return html.strip()


def extract_item_html(html: str) -> str:
    if not html:
        return ""
    m = re.search(r'<div class="item-page"[^>]*>(.*)', html, re.S | re.I)
    if not m:
        return ""
    tail = m.group(1)
    for marker in (
        '<div class="bt-social-share"',
        '<ul class="pagenav"',
        '<div class="ja-moduletable',
    ):
        idx = tail.find(marker)
        if idx != -1:
            tail = tail[:idx]
            break
    return tail


def extract_pdf_text(url: str) -> str:
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return ""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(r.content)
            tmp = f.name
        txt_path = tmp + ".txt"
        subprocess.run(
            ["pdftotext", "-layout", tmp, txt_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        text = Path(txt_path).read_text(errors="ignore")
        Path(tmp).unlink(missing_ok=True)
        Path(txt_path).unlink(missing_ok=True)
        return text
    except Exception:
        return ""


def fetch_bytes(url: str, timeout: int = 25) -> bytes:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    if r.status_code != 200:
        return b""
    return r.content


def extract_pdf_text_bytes(pdf_bytes: bytes) -> str:
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(pdf_bytes)
            pdf_path = f.name
        txt_path = pdf_path + ".txt"
        subprocess.run(
            ["pdftotext", "-layout", pdf_path, txt_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        text = Path(txt_path).read_text(errors="ignore")
        Path(pdf_path).unlink(missing_ok=True)
        Path(txt_path).unlink(missing_ok=True)
        return text.strip()
    except Exception:
        return ""


def extract_doc_text_bytes(doc_bytes: bytes) -> str:
    try:
        with tempfile.NamedTemporaryFile(suffix=".doc", delete=False) as f:
            f.write(doc_bytes)
            doc_path = f.name
        out = subprocess.run(
            ["antiword", doc_path],
            capture_output=True,
            text=True,
            check=False,
        )
        Path(doc_path).unlink(missing_ok=True)
        return (out.stdout or "").strip()
    except Exception:
        return ""


def extract_docx_text_bytes(docx_bytes: bytes) -> str:
    try:
        import docx2txt
        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as f:
            f.write(docx_bytes)
            docx_path = f.name
        text = docx2txt.process(docx_path) or ""
        Path(docx_path).unlink(missing_ok=True)
        return text.strip()
    except Exception:
        return ""


def extract_attachment_text(url: str) -> str:
    lower = url.lower()
    b = fetch_bytes(url)
    if not b:
        return ""
    if lower.endswith(".pdf"):
        return extract_pdf_text_bytes(b)
    if lower.endswith(".doc"):
        return extract_doc_text_bytes(b)
    if lower.endswith(".docx"):
        return extract_docx_text_bytes(b)
    return ""


def find_attachment_links(base_url: str, html: str) -> list[str]:
    links = []
    for m in ATTACH_RE.finditer(html or ""):
        href = m.group(1)
        links.append(urljoin(base_url, href))
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def classify_sector(text: str | None):
    if not text:
        return "unknown"
    t = text.lower()
    for sector, kws in SECTOR_KEYWORDS.items():
        if any(k in t for k in kws):
            return sector
    return "unknown"


def summarize(text: str | None):
    if not text:
        return ""
    return " ".join(text.split()[:40])


def run(db_url: str, source_filter: str | None = None):
    meta = _load_source_meta()
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    c_candidates = 0
    c_fetch_ok = 0
    c_gate_html = 0
    c_gate_attach = 0
    c_gate_fail = 0
    c_inserted = 0

    cur.execute(
        """
        SELECT id,
               raw->'procurement'->>'source_key' as source_key,
               url,
               content,
               summary,
               published_at,
               language
        FROM feed_items
        WHERE raw ? 'procurement'
          AND (%s IS NULL OR raw->'procurement'->>'source_key' = %s)
          AND id NOT IN (SELECT raw_article_id FROM tenders WHERE raw_article_id IS NOT NULL)
        ORDER BY ingested_at DESC
        LIMIT 500
        """,
        (source_filter, source_filter),
    )
    rows = cur.fetchall()

    for rid, source_key, url, content, summary, published_at, language in rows:
        c_candidates += 1
        text = content or summary or ""
        text = text or ""
        title = None
        publish_date = None
        if published_at:
            try:
                publish_date = published_at.date()
            except Exception:
                publish_date = None

        if source_key == "lpma_tenders":
            detail_html = ""
            try:
                detail_html = requests.get(url, timeout=20).text
            except Exception:
                detail_html = ""
            if detail_html:
                c_fetch_ok += 1
            item_html = extract_item_html(detail_html) or detail_html
            detail_text = html_to_text(item_html) if item_html else ""
            if not detail_text:
                detail_text = text
            title_match = re.search(
                r'<h2 class="contentheading">\s*<a[^>]*>(.*?)</a>',
                detail_html,
                re.S | re.I,
            )
            if title_match:
                title = html_to_text(title_match.group(1))[:200] or title
            date_match = re.search(
                r'<div class="article-date">\s*([0-9]{1,2}/[0-9]{1,2}\s+[0-9]{4})',
                detail_html,
                re.S | re.I,
            )
            if date_match:
                publish_date = _parse_date(date_match.group(1))
            if publish_date is None:
                url_date = re.search(r'/(\d{4}-\d{2}-\d{2})', url)
                if url_date:
                    publish_date = _parse_date(url_date.group(1))
            passed = contains_core_keywords(detail_text)
            attachment_text = ""
            attachments = find_attachment_links(url, item_html)

            if passed:
                c_gate_html += 1
            elif attachments:
                for aurl in attachments:
                    t = extract_attachment_text(aurl)
                    if t and contains_core_keywords(t):
                        attachment_text = t
                        passed = True
                        c_gate_attach += 1
                        break

            if not passed:
                c_gate_fail += 1
                continue

            best_text = attachment_text if attachment_text else detail_text
            text = best_text
            attachments_count = len(attachments)
        else:
            if not text:
                continue
            attachments_count = 0

        if not title:
            title = text.split("\n")[0][:200]
        buyer = meta.get(source_key or "", {}).get("buyer") or (source_key or "unknown")
        sector = meta.get(source_key or "", {}).get("sector") or classify_sector(text)
        deadline = extract_deadline(text)
        summary_text = summarize(text)
        confidence = 0.2
        if deadline:
            confidence += 0.4
        if sector and sector != "unknown":
            confidence += 0.2

        pdf_text = text if len(text) < 200000 else text[:200000]
        cur.execute(
            """
            INSERT INTO tenders (
                source, buyer, title, summary,
                publish_date, deadline_date, sector, url, raw_article_id,
                language, confidence_score, pdf_text, attachments_count
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (url) DO NOTHING
            """,
            (
                source_key or "unknown",
                buyer,
                title,
                summary_text,
                publish_date,
                deadline,
                sector,
                url,
                rid,
                language,
                confidence,
                pdf_text,
                attachments_count,
            ),
        )
        c_inserted += cur.rowcount

    conn.commit()
    conn.close()
    print(
        f"EXTRACT_OK source={source_filter or 'ALL'} candidates={c_candidates} "
        f"fetch_ok={c_fetch_ok} gate_html={c_gate_html} gate_attach={c_gate_attach} "
        f"gate_fail={c_gate_fail} inserted={c_inserted}"
    )
    if c_candidates:
        gate_fail_rate = c_gate_fail / c_candidates
        if c_inserted == 0 and c_gate_fail:
            print(
                "EXTRACT_WARN inserted=0 "
                f"gate_fail_rate={gate_fail_rate:.2f} "
                f"gate_fail={c_gate_fail} candidates={c_candidates}"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", dest="source", help="Filter by procurement source_key")
    args = parser.parse_args()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is required")
    run(db_url, source_filter=args.source)


if __name__ == "__main__":
    main()

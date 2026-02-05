import os
import base64
import re
import html as _html
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.db import get_key_column, get_client
from backend.coverage import compute_coverage, compute_ingest_health
from backend.llm.ollama import summarize as summarize_items

app = FastAPI(title="LibyaIntel API")

ALLOWED_ORIGINS = ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

ADMIN_KEY = os.environ.get("ADMIN_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPPORT_INBOX_EMAIL = os.environ.get("SUPPORT_INBOX_EMAIL")

_SERVICE_CATEGORIES = {
    "legal",
    "tax",
    "accounting",
    "payroll",
    "eor/manpower",
    "recruitment",
    "training",
    "consultancy",
}

_SERVICE_URGENCY = {"low", "normal", "high"}


def require_admin(x_admin_key: str | None = Header(default=None)):
    if not ADMIN_KEY or x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_user(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(status_code=500, detail="Auth not configured")
    token = authorization.split(" ", 1)[1].strip()
    try:
        r = httpx.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": SUPABASE_SERVICE_ROLE_KEY,
            },
            timeout=5,
        )
    except Exception:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if r.status_code != 200:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return r.json()


def _encode_cursor(published_at: str, item_id: str | int) -> str:
    raw = f"{published_at}|{item_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _decode_cursor(token: str | None) -> tuple[str, str] | None:
    if not token:
        return None
    try:
        pad = "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode((token + pad).encode("utf-8")).decode("utf-8")
        published_at, item_id = raw.split("|", 1)
        return published_at, item_id
    except Exception:
        return None


@app.get("/health")
def health():
    return {"ok": True}


class MarketQuoteItem(BaseModel):
    instrument: str
    rate_type: str
    quote_currency: str
    value: float
    unit: str | None = None
    as_of: str
    source_name: str
    source_url: str
    status: str


@app.get("/api/market/quotes")
def market_quotes():
    sb = get_client()
    if not _table_exists(sb, "market_quotes"):
        return {"as_of": None, "items": []}

    res = (
        sb.table("market_quotes")
        .select(
            "instrument,rate_type,quote_currency,value,unit,as_of,source_name,source_url,status"
        )
        .execute()
    )
    rows = res.data or []

    items: list[MarketQuoteItem] = []
    latest_as_of: str | None = None
    for row in rows:
        value = row.get("value")
        if isinstance(value, Decimal):
            value = float(value)
        elif isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                value = 0.0

        as_of = row.get("as_of") or ""
        if as_of and (latest_as_of is None or as_of > latest_as_of):
            latest_as_of = as_of

        try:
            items.append(
                MarketQuoteItem(
                    instrument=row.get("instrument") or "",
                    rate_type=row.get("rate_type") or "",
                    quote_currency=row.get("quote_currency") or "",
                    value=value if isinstance(value, (int, float)) else 0.0,
                    unit=row.get("unit"),
                    as_of=as_of,
                    source_name=row.get("source_name") or "",
                    source_url=row.get("source_url") or "",
                    status=row.get("status") or "error",
                )
            )
        except Exception:
            continue

    items.sort(key=lambda i: (i.instrument, i.rate_type, i.quote_currency))
    return {"as_of": latest_as_of, "items": [i.model_dump() for i in items]}


class ServiceRequestIn(BaseModel):
    category: str
    company_name: str | None = None
    contact_name: str
    email: str
    whatsapp: str | None = None
    country: str | None = None
    city: str | None = None
    urgency: str = "normal"
    message: str


def _send_support_request_email(request_id: str, payload: ServiceRequestIn) -> None:
    api_key = os.getenv("RESEND_API_KEY")
    if not api_key:
        print(f"SUPPORT_EMAIL_SKIP request_id={request_id} missing RESEND_API_KEY")
        return
    if not SUPPORT_INBOX_EMAIL:
        print(f"SUPPORT_EMAIL_SKIP request_id={request_id} missing SUPPORT_INBOX_EMAIL")
        return

    try:
        import resend
    except Exception as exc:
        print(f"SUPPORT_EMAIL_FAIL request_id={request_id} missing resend err={exc}")
        return

    resend.api_key = api_key
    from_addr = (
        os.getenv("SUPPORT_FROM")
        or os.getenv("DIGEST_FROM")
        or "LibyaIntel <alerts@libyaintel.com>"
    ).strip()
    subject = f"[LibyaIntel] New support request ({payload.category})"
    body = "\n".join(
        [
            "New service request received:",
            "",
            f"Request ID: {request_id}",
            f"Category: {payload.category}",
            f"Urgency: {payload.urgency}",
            "",
            f"Contact: {payload.contact_name}",
            f"Company: {payload.company_name or ''}".strip(),
            f"Email: {payload.email}",
            f"WhatsApp: {payload.whatsapp or ''}".strip(),
            f"Location: {', '.join([x for x in [payload.city, payload.country] if x])}",
            "",
            "Message:",
            payload.message,
        ]
    ).strip()

    try:
        resp = resend.Emails.send(
            {
                "from": from_addr,
                "to": [SUPPORT_INBOX_EMAIL],
                "subject": subject,
                "text": body,
            }
        )
        msg_id = resp.get("id") if isinstance(resp, dict) else ""
        print(f"SUPPORT_EMAIL_OK request_id={request_id} id={msg_id}")
    except Exception as exc:
        # Avoid printing request payload or recipient details (PII) into logs.
        print(f"SUPPORT_EMAIL_FAIL request_id={request_id} err={type(exc).__name__}")


@app.post("/api/service-requests")
def create_service_request(payload: ServiceRequestIn):
    if not SUPPORT_INBOX_EMAIL or not os.getenv("RESEND_API_KEY"):
        raise HTTPException(status_code=500, detail="Support email not configured")

    category = (payload.category or "").strip().lower()
    urgency = (payload.urgency or "").strip().lower()
    if category not in _SERVICE_CATEGORIES:
        raise HTTPException(status_code=400, detail="Invalid category")
    if urgency not in _SERVICE_URGENCY:
        raise HTTPException(status_code=400, detail="Invalid urgency")

    sb = get_client()
    if not _table_exists(sb, "service_requests"):
        raise HTTPException(status_code=500, detail="service_requests table missing")

    row = payload.model_dump()
    row["category"] = category
    row["urgency"] = urgency

    res = sb.table("service_requests").insert(row).execute()
    created = (res.data or [])
    if not created or not created[0].get("id"):
        raise HTTPException(status_code=500, detail="Failed to create request")

    request_id = str(created[0].get("id"))
    _send_support_request_email(request_id, payload)
    return {"request_id": request_id}


class PartnerIn(BaseModel):
    company_name: str
    categories: list[str] = []
    city: str | None = None
    contact_name: str | None = None
    email: str | None = None
    phone: str | None = None
    website: str | None = None
    status: str = "pending"
    tier: str = "standard"
    is_public: bool = False
    annual_fee_usd: float | None = None
    renewal_date: str | None = None
    notes_internal: str | None = None


@app.get("/api/admin/partners", dependencies=[Depends(require_admin)])
def admin_list_partners(limit: int = Query(200, ge=1, le=500)):
    sb = get_client()
    if not _table_exists(sb, "partners"):
        return {"items": []}
    res = sb.table("partners").select("*").order("created_at", desc=True).limit(limit).execute()
    return {"items": res.data or []}


@app.post("/api/admin/partners", dependencies=[Depends(require_admin)])
def admin_create_partner(payload: PartnerIn):
    sb = get_client()
    if not _table_exists(sb, "partners"):
        raise HTTPException(status_code=500, detail="partners table missing")

    row = payload.model_dump()
    res = sb.table("partners").insert(row).execute()
    created = (res.data or [])
    if not created or not created[0].get("id"):
        raise HTTPException(status_code=500, detail="Failed to create partner")
    return {"partner_id": str(created[0].get("id"))}


@app.get("/api/admin/requests", dependencies=[Depends(require_admin)])
def admin_list_requests(
    status: str | None = Query(default=None),
    limit: int = Query(200, ge=1, le=500),
):
    sb = get_client()
    if not _table_exists(sb, "service_requests"):
        return {"items": []}
    q = sb.table("service_requests").select("*").order("created_at", desc=True).limit(limit)
    if status:
        q = q.eq("status", status)
    res = q.execute()
    return {"items": res.data or []}


class AssignRequestIn(BaseModel):
    partner_id: str


@app.post("/api/admin/requests/{request_id}/assign", dependencies=[Depends(require_admin)])
def admin_assign_request(request_id: str, payload: AssignRequestIn):
    sb = get_client()
    if not _table_exists(sb, "service_requests") or not _table_exists(sb, "partner_leads"):
        raise HTTPException(status_code=500, detail="tables missing")

    partner_id = (payload.partner_id or "").strip()
    if not partner_id:
        raise HTTPException(status_code=400, detail="partner_id required")

    sb.table("service_requests").update(
        {"assigned_partner_id": partner_id, "status": "assigned"}
    ).eq("id", request_id).execute()
    sb.table("partner_leads").insert(
        {"partner_id": partner_id, "request_id": request_id, "status": "sent"}
    ).execute()
    return {"ok": True}


@app.get("/feed")
def feed(
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = None,
    source_type: str | None = None,
    language: str | None = None,
):
    sb = get_client()
    q = (
        sb.table("feed_items")
        .select("*")
        .eq("is_deleted", False)
        .order("published_at", desc=True)
        .order("id", desc=True)
        .limit(limit)
    )

    if source_type:
        q = q.eq("source_type", source_type)
    if language:
        q = q.eq("language", language)
    parsed = _decode_cursor(cursor)
    if parsed:
        published_at, item_id = parsed
        q = q.or_(f"published_at.lt.{published_at},and(published_at.eq.{published_at},id.lt.{item_id})")

    res = q.execute()
    items = res.data or []
    next_cursor = (
        _encode_cursor(items[-1]["published_at"], items[-1]["id"]) if items else None
    )
    return {"items": items, "next_cursor": next_cursor}


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _table_has_column(sb, table: str, column: str) -> bool:
    try:
        cols = sb.rpc("get_columns", {"p_table": table}).execute().data or []
    except Exception:
        return False
    return any(c.get("column_name") == column for c in cols)

def _table_exists(sb, table: str) -> bool:
    try:
        cols = sb.rpc("get_columns", {"p_table": table}).execute().data or []
    except Exception:
        return False
    return bool(cols)


def _coalesce_time(item: dict, published_key: str = "published_at", created_key: str = "created_at"):
    return item.get(published_key) or item.get(created_key)


def _category_from_text(title: str | None, summary: str | None) -> str:
    text = f"{title or ''} {summary or ''}".lower()
    rules: list[tuple[str, list[str]]] = [
        ("Energy", ["oil", "gas", "energy", "power", "pipeline", "fuel"]),
        ("Finance", ["bank", "loan", "budget", "bond", "fund", "finance"]),
        ("Security", ["attack", "clash", "militia", "security", "armed"]),
        ("Governance", ["parliament", "cabinet", "minister", "election", "law"]),
        ("Trade", ["import", "export", "trade", "customs", "tariff"]),
        ("Migration", ["migrant", "migration", "border", "detention"]),
    ]
    for label, keys in rules:
        if any(k in text for k in keys):
            return label
    return "General"


def _clean_text(value: str | None, allow_html: bool = False) -> str | None:
    if not value:
        return None
    text = value
    if allow_html:
        text = re.sub(r"(?i)<\s*br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</\s*p\s*>", "\n", text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = _html.unescape(text)
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text).strip()
    return text or None


def _filter_boilerplate(text: str | None, max_chars: int = 4000) -> str | None:
    if not text:
        return None
    phrases = [
        "skip to main content",
        "main navigation",
        "menu",
        "search",
        "follow us",
        "print edition",
        "drupal",
        "just another",
        "category:",
        "news archive",
        "sitemap",
        "contact us",
        "careers",
        "login",
        "subscribe",
        "all rights reserved",
        "privacy policy",
        "terms of service",
        "cookie",
        "newsletter",
    ]

    def is_boilerplate(line: str) -> bool:
        low = line.lower()
        return any(p in low for p in phrases)

    lines = [l.strip() for l in re.split(r"\n+", text) if l.strip()]
    if len(lines) <= 2:
        # Likely a single long blob; split into sentences.
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        filtered = [s for s in sentences if not is_boilerplate(s) and len(s) > 40]
        cleaned = "\n\n".join(filtered)
    else:
        filtered = [l for l in lines if not is_boilerplate(l) and len(l) > 25]
        cleaned = "\n\n".join(filtered)

    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    if not cleaned:
        return None
    return cleaned[:max_chars]


def _first_existing_column(sb, table: str, candidates: list[str]) -> str | None:
    try:
        cols = sb.rpc("get_columns", {"p_table": table}).execute().data or []
    except Exception:
        return None
    names = {c.get("column_name") for c in cols}
    for col in candidates:
        if col in names:
            return col
    return None


def _source_meta(sb, source_id: str | None, source_key: str | None) -> dict | None:
    try:
        cols = sb.rpc("get_columns", {"p_table": "sources"}).execute().data or []
    except Exception:
        return None
    names = {c.get("column_name") for c in cols}
    if "id" not in names:
        return None
    select_cols = ["id"]
    if "name" in names:
        select_cols.append("name")
    if "url" in names:
        select_cols.append("url")
    if "website" in names:
        select_cols.append("website")
    key_col = None
    if "key" in names:
        key_col = "key"
    elif "source_key" in names:
        key_col = "source_key"

    if source_id:
        if re.fullmatch(r"[0-9a-fA-F-]{36}", str(source_id)):
            res = (
                sb.table("sources")
                .select(",".join(select_cols))
                .eq("id", source_id)
                .limit(1)
                .execute()
            )
            if res.data:
                return res.data[0]
    if source_key and key_col:
        res = (
            sb.table("sources")
            .select(",".join(select_cols))
            .eq(key_col, source_key)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0]
    return None

def _fetch_entities(sb, article_id: int) -> list[str]:
    if not _table_exists(sb, "article_entities") or not _table_exists(sb, "entities"):
        return []
    try:
        res = (
            sb.table("article_entities")
            .select("entity_id")
            .eq("article_id", article_id)
            .limit(50)
            .execute()
        )
    except Exception:
        return []
    ids = [row.get("entity_id") for row in (res.data or []) if row.get("entity_id")]
    if not ids:
        return []
    try:
        ent_cols = sb.rpc("get_columns", {"p_table": "entities"}).execute().data or []
    except Exception:
        return []
    names = {c.get("column_name") for c in ent_cols}
    select_cols = ["id"]
    if "name" in names:
        select_cols.append("name")
    if "normalized_name" in names:
        select_cols.append("normalized_name")
    try:
        res2 = (
            sb.table("entities")
            .select(",".join(select_cols))
            .in_("id", ids)
            .execute()
        )
    except Exception:
        return []
    out: list[str] = []
    for row in res2.data or []:
        name = row.get("name") or row.get("normalized_name")
        if name and name not in out:
            out.append(name)
    return out


@app.get("/stats/overview")
def stats_overview():
    sb = get_client()
    now = datetime.now(timezone.utc)
    since_24h_dt = now - timedelta(hours=24)
    since_7d_dt = now - timedelta(days=7)
    since_24h = since_24h_dt.isoformat()
    since_7d = since_7d_dt.isoformat()

    base_query = sb.table("feed_items").select(
        "id,language,source_type,source_id,published_at"
    )
    base_query = base_query.eq("is_deleted", False)

    res_7d = base_query.gte("published_at", since_7d).execute()
    items_7d = res_7d.data or []

    total_7d = len(items_7d)

    res_24h = (
        sb.table("feed_items")
        .select("id")
        .eq("is_deleted", False)
        .gte("published_at", since_24h)
        .execute()
    )
    total_24h = len(res_24h.data or [])

    by_language: dict[str, int] = {}
    by_source_type: dict[str, int] = {}
    by_source_id: dict[str, int] = {}

    for item in items_7d:
        lang = item.get("language") or "unknown"
        stype = item.get("source_type") or "unknown"
        sid = item.get("source_id")

        by_language[lang] = by_language.get(lang, 0) + 1
        by_source_type[stype] = by_source_type.get(stype, 0) + 1
        if sid:
            by_source_id[sid] = by_source_id.get(sid, 0) + 1

    top_sources = sorted(by_source_id.items(), key=lambda x: x[1], reverse=True)[:5]
    source_names: dict[str, str] = {}
    if top_sources:
        ids = [sid for sid, _ in top_sources]
        src_res = sb.table("sources").select("id,name").in_("id", ids).execute()
        source_names = {row["id"]: row.get("name") for row in (src_res.data or [])}

    top_sources_7d = [
        {"source_id": sid, "name": source_names.get(sid), "count": count}
        for sid, count in top_sources
    ]

    return {
        "total_24h": total_24h,
        "total_7d": total_7d,
        "by_language": by_language,
        "by_source_type": by_source_type,
        "top_sources_7d": top_sources_7d,
    }


@app.get("/public/preview")
def public_preview(response: Response, limit: int = Query(10, ge=1, le=50)):
    sb = get_client()
    has_created = _table_has_column(sb, "articles", "created_at")
    has_published = _table_has_column(sb, "articles", "published_at")

    select_cols = ["id", "title", "url", "source", "summary"]
    if has_published:
        select_cols.append("published_at")
    if has_created:
        select_cols.append("created_at")

    q = sb.table("articles").select(",".join(select_cols))
    if has_published:
        q = q.order("published_at", desc=True)
    if has_created:
        q = q.order("created_at", desc=True)
    q = q.order("id", desc=True).limit(max(limit, 30))
    res = q.execute()
    raw_items = res.data or []

    items = sorted(
        raw_items,
        key=lambda x: (_coalesce_time(x) or "", x.get("id") or 0),
        reverse=True,
    )[:limit]

    last_updated = None
    if items:
        last_updated = _coalesce_time(items[0])

    for item in items:
        item["category_guess"] = _category_from_text(item.get("title"), item.get("summary"))

    response.headers["Cache-Control"] = "public, max-age=60"
    return {"last_updated": last_updated, "items": items}


def _activity_payload():
    sb = get_client()
    has_created = _table_has_column(sb, "articles", "created_at")
    has_published = _table_has_column(sb, "articles", "published_at")

    now = datetime.now(timezone.utc)
    since_24h_dt = now - timedelta(hours=24)
    since_24h = since_24h_dt.isoformat()

    select_cols = ["id", "title", "summary"]
    if has_published:
        select_cols.append("published_at")
    if has_created:
        select_cols.append("created_at")

    items: list[dict] = []
    if has_published:
        res_pub = (
            sb.table("articles")
            .select(",".join(select_cols))
            .gte("published_at", since_24h)
            .execute()
        )
        items.extend(res_pub.data or [])

    if has_created:
        res_created = (
            sb.table("articles")
            .select(",".join(select_cols))
            .gte("created_at", since_24h)
            .execute()
        )
        items.extend(res_created.data or [])

    if not has_published and not has_created:
        res = sb.table("articles").select(",".join(select_cols)).execute()
        items = res.data or []

    by_id: dict[str | int, dict] = {}
    for item in items:
        by_id[item.get("id")] = item
    items = list(by_id.values())

    filtered: list[dict] = []
    for item in items:
        ts_raw = _coalesce_time(item)
        ts = _parse_iso(ts_raw)
        if not ts:
            continue
        if ts >= since_24h_dt:
            filtered.append(item)
    items = filtered

    def _count_by_keywords(keywords: list[str]) -> int:
        count = 0
        for item in items:
            text = f"{item.get('title') or ''} {item.get('summary') or ''}".lower()
            if any(kw in text for kw in keywords):
                count += 1
        return count

    total_24h = len(items)
    tenders_24h = _count_by_keywords(["tender", "procurement", "rfp", "bid", "auction"])
    regulations_24h = _count_by_keywords(
        ["regulation", "regulatory", "law", "decree", "gazette", "policy"]
    )
    high_impact_24h = _count_by_keywords(
        ["sanction", "shutdown", "explosion", "attack", "strike", "ceasefire", "resignation"]
    )

    return {
        "total_24h": total_24h,
        "tenders_24h": tenders_24h,
        "regulations_24h": regulations_24h,
        "high_impact_24h": high_impact_24h,
    }


@app.get("/public/activity")
def public_activity(response: Response):
    response.headers["Cache-Control"] = "public, max-age=60"
    return _activity_payload()


@app.get("/private/activity")
def private_activity(user=Depends(require_user)):
    return _activity_payload()


def _search_payload(
    q: str | None,
    days: int,
    category: str | None,
    source: str | None,
    limit: int,
):
    sb = get_client()
    has_created = _table_has_column(sb, "articles", "created_at")
    has_published = _table_has_column(sb, "articles", "published_at")
    has_translated = _table_has_column(sb, "articles", "translated_content")
    has_category = _table_has_column(sb, "articles", "category")
    has_source_name = _table_has_column(sb, "articles", "source_name")

    now = datetime.now(timezone.utc)
    since_dt = now - timedelta(days=days)
    since = since_dt.isoformat()

    select_cols = ["id", "title", "summary", "url", "source"]
    if has_source_name:
        select_cols.append("source_name")
    if has_category:
        select_cols.append("category")
    if has_published:
        select_cols.append("published_at")
    if has_created:
        select_cols.append("created_at")
    if has_translated:
        select_cols.append("translated_content")
    if _table_has_column(sb, "articles", "content"):
        select_cols.append("content")

    # Try RPC full-text search if available
    if q:
        try:
            rpc_res = sb.rpc(
                "search_articles",
                {
                    "q": q,
                    "days": days,
                    "category_filter": category,
                    "source_filter": f"%{source}%" if source else None,
                    "limit_count": limit,
                },
            ).execute()
            if rpc_res.data is not None:
                items = rpc_res.data or []
                for item in items:
                    item["category_guess"] = _category_from_text(
                        item.get("title"), item.get("summary")
                    )
                    if item.get("category"):
                        item["category_guess"] = item.get("category")
                return {"items": items, "count": len(items)}
        except Exception:
            pass

    search_cols = [c for c in ["title", "summary", "content", "translated_content"] if c in select_cols]
    pattern = f"%{q}%" if q else None

    def _apply_search(query):
        if pattern and search_cols:
            or_parts = [f"{col}.ilike.{pattern}" for col in search_cols]
            return query.or_(",".join(or_parts))
        return query

    items: list[dict] = []
    if has_published:
        q_pub = sb.table("articles").select(",".join(select_cols)).gte("published_at", since)
        q_pub = _apply_search(q_pub)
        items.extend(q_pub.execute().data or [])

    if has_created:
        q_created = sb.table("articles").select(",".join(select_cols)).gte("created_at", since)
        q_created = _apply_search(q_created)
        items.extend(q_created.execute().data or [])

    if not has_published and not has_created:
        q_all = sb.table("articles").select(",".join(select_cols))
        q_all = _apply_search(q_all)
        items = q_all.execute().data or []

    by_id: dict[str | int, dict] = {item.get("id"): item for item in items}
    items = list(by_id.values())

    filtered: list[dict] = []
    for item in items:
        ts_raw = _coalesce_time(item)
        ts = _parse_iso(ts_raw)
        if ts and ts >= since_dt:
            filtered.append(item)
    items = filtered

    if source:
        source_l = source.lower()
        items = [
            item
            for item in items
            if source_l in (item.get("source") or "").lower()
            or source_l in (item.get("source_name") or "").lower()
        ]

    for item in items:
        item["category_guess"] = _category_from_text(item.get("title"), item.get("summary"))
        if has_category and item.get("category"):
            item["category_guess"] = item.get("category")

    if category:
        category_l = category.lower()
        items = [item for item in items if (item.get("category_guess") or "").lower() == category_l]

    items = sorted(
        items,
        key=lambda x: (_coalesce_time(x) or "", x.get("id") or 0),
        reverse=True,
    )[:limit]

    return {"items": items, "count": len(items)}


@app.get("/public/search")
def public_search(
    response: Response,
    q: str | None = None,
    days: int = Query(7, ge=1, le=30),
    category: str | None = None,
    source: str | None = None,
    limit: int = Query(50, ge=1, le=100),
):
    response.headers["Cache-Control"] = "public, max-age=60"
    return _search_payload(q, days, category, source, limit)


@app.get("/private/search")
def private_search(
    q: str | None = None,
    days: int = Query(7, ge=1, le=30),
    category: str | None = None,
    source: str | None = None,
    limit: int = Query(50, ge=1, le=100),
    user=Depends(require_user),
):
    return _search_payload(q, days, category, source, limit)


@app.get("/public/article/{article_id}")
def public_article(response: Response, article_id: int):
    sb = get_client()
    res = (
        sb.table("articles")
        .select("*")
        .eq("id", article_id)
        .limit(1)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Not found")

    row = res.data[0]
    summary_col = _first_existing_column(sb, "articles", ["summary", "ai_summary"])
    content_col = _first_existing_column(
        sb, "articles", ["content_clean", "clean_text", "content_text", "text", "content"]
    )

    source_meta = _source_meta(sb, row.get("source_id"), row.get("source"))
    source_name = row.get("source_name") or row.get("source") or (
        source_meta.get("name") if source_meta else None
    )
    source_url = None
    if source_meta:
        source_url = source_meta.get("url") or source_meta.get("website")

    category_value = row.get("category") or _category_from_text(
        row.get("title"), row.get(summary_col) if summary_col else None
    )

    payload = {
        "id": row.get("id"),
        "title": row.get("title"),
        "summary": row.get(summary_col) if summary_col else None,
        "category_guess": category_value,
        "published_at": row.get("published_at"),
        "created_at": row.get("created_at"),
        "source_name": source_name,
        "source_url": source_url,
        "url": row.get("url"),
        "content_clean": _filter_boilerplate(
            _clean_text(
                row.get(content_col) if content_col else None,
                allow_html=content_col == "content",
            )
        ),
        "entities": _fetch_entities(sb, row.get("id")),
    }

    response.headers["Cache-Control"] = "public, max-age=60"
    return payload


@app.get("/stats/sources")
def stats_sources():
    sb = get_client()
    now = datetime.now(timezone.utc)
    since_24h = (now - timedelta(hours=24)).isoformat()
    since_7d = (now - timedelta(days=7)).isoformat()

    items_24h = (
        sb.table("feed_items")
        .select("id,source_id,published_at")
        .eq("is_deleted", False)
        .gte("published_at", since_24h)
        .execute()
        .data
        or []
    )

    items_7d = (
        sb.table("feed_items")
        .select("id,source_id,published_at")
        .eq("is_deleted", False)
        .gte("published_at", since_7d)
        .execute()
        .data
        or []
    )

    def summarize(items: list[dict]) -> dict[str, dict]:
        by_source: dict[str, dict] = {}
        for item in items:
            sid = item.get("source_id")
            if not sid:
                continue
            published_at = item.get("published_at")
            entry = by_source.setdefault(sid, {"count": 0, "last_ingested_at": None})
            entry["count"] += 1
            if published_at and (entry["last_ingested_at"] is None or published_at > entry["last_ingested_at"]):
                entry["last_ingested_at"] = published_at
        return by_source

    by_source_24h = summarize(items_24h)
    by_source_7d = summarize(items_7d)

    source_ids = list({*by_source_24h.keys(), *by_source_7d.keys()})
    source_names: dict[str, str] = {}
    if source_ids:
        key_col = get_key_column(sb)
        src_res = (
            sb.table("sources")
            .select(f"id,name,{key_col}")
            .in_("id", source_ids)
            .execute()
        )
        source_names = {
            row["id"]: row.get("name") or row.get(key_col) or row["id"]
            for row in (src_res.data or [])
        }

    def with_names(payload: dict[str, dict]) -> list[dict]:
        output = []
        for sid, meta in payload.items():
            output.append(
                {
                    "source_id": sid,
                    "name": source_names.get(sid),
                    "count": meta.get("count", 0),
                    "last_ingested_at": meta.get("last_ingested_at"),
                }
            )
        return sorted(output, key=lambda x: x["count"], reverse=True)

    runs = (
        sb.table("ingest_runs")
        .select("job_name,ok,started_at,finished_at,stats")
        .order("started_at", desc=True)
        .limit(200)
        .execute()
        .data
        or []
    )

    error_counts: dict[str, int] = {}
    for run in runs:
        if run.get("ok") is False:
            job = run.get("job_name") or "unknown"
            error_counts[job] = error_counts.get(job, 0) + 1

    key_col = get_key_column(sb)

    source_columns = sb.rpc("get_columns", {"p_table": "sources"}).execute().data or []
    source_column_names = {c["column_name"] for c in source_columns}
    active_col = None
    for candidate in ("enabled", "active", "is_active"):
        if candidate in source_column_names:
            active_col = candidate
            break

    source_select_cols = f"id,name,{key_col}"
    if active_col:
        source_select_cols = f"{source_select_cols},{active_col}"
    source_rows = sb.table("sources").select(source_select_cols).execute().data or []
    source_names = {
        row["id"]: row.get("name") or row.get(key_col) or row["id"] for row in source_rows
    }
    key_to_source = {row.get(key_col): row["id"] for row in source_rows if row.get(key_col)}

    feed_columns = sb.rpc("get_columns", {"p_table": "feed_items"}).execute().data or []
    feed_column_names = {c["column_name"] for c in feed_columns}
    if "ingested_at" in feed_column_names:
        item_ts_col = "ingested_at"
    elif "created_at" in feed_column_names:
        item_ts_col = "created_at"
    else:
        item_ts_col = "published_at"

    items_recent = (
        sb.table("feed_items")
        .select(f"source_id,{item_ts_col}")
        .eq("is_deleted", False)
        .gte(item_ts_col, since_7d)
        .execute()
        .data
        or []
    )
    last_item_at_by_source: dict[str, str] = {}
    for item in items_recent:
        sid = item.get("source_id")
        ts = item.get(item_ts_col)
        if not sid or not ts:
            continue
        existing = last_item_at_by_source.get(sid)
        if existing is None or ts > existing:
            last_item_at_by_source[sid] = ts

    blocked_24h_by_source: dict[str, int] = {}
    last_error_by_source: dict[str, str | None] = {}
    last_error_ts_by_source: dict[str, datetime] = {}
    for run in runs:
        started_at_raw = run.get("started_at")
        started_at = _parse_iso(started_at_raw)
        stats = run.get("stats") or {}
        by_source = stats.get("by_source") or {}
        in_24h = started_at is not None and started_at >= since_24h_dt
        in_7d = started_at is not None and started_at >= since_7d_dt
        if not in_7d:
            continue
        for source_key, meta in by_source.items():
            sid = key_to_source.get(source_key)
            if not sid:
                continue
            if in_24h:
                blocked_24h_by_source[sid] = blocked_24h_by_source.get(sid, 0) + int(
                    meta.get("blocked", 0) or 0
                )
            last_error = meta.get("last_error")
            if last_error:
                last_ts = last_error_ts_by_source.get(sid)
                if last_ts is None or started_at > last_ts:
                    last_error_ts_by_source[sid] = started_at
                    last_error_by_source[sid] = last_error

    sources_by_id = {row["id"]: row for row in source_rows}
    source_health = []
    for row in source_rows:
        sid = row["id"]
        last_item_at = last_item_at_by_source.get(sid)
        items_24h_count = (by_source_24h.get(sid) or {}).get("count", 0)
        items_7d_count = (by_source_7d.get(sid) or {}).get("count", 0)
        blocked_24h = blocked_24h_by_source.get(sid, 0)
        last_error = last_error_by_source.get(sid)
        is_active = True
        if active_col:
            is_active = bool(sources_by_id[sid].get(active_col))

        status = "ok"
        if blocked_24h > 0 and items_24h_count == 0:
            status = "blocked"
        elif is_active:
            last_item_dt = _parse_iso(last_item_at) if last_item_at else None
            if last_item_dt is None or last_item_dt < since_24h_dt:
                status = "stale"
            status = "stale"

        source_health.append(
            {
                "source_id": sid,
                "name": source_names.get(sid),
                "last_item_at": last_item_at,
                "items_24h": items_24h_count,
                "items_7d": items_7d_count,
                "blocked_24h": blocked_24h,
                "last_error": last_error,
                "status": status,
            }
        )

    return {
        "sources_24h": with_names(by_source_24h),
        "sources_7d": with_names(by_source_7d),
        "ingest_errors": error_counts,
        "source_health": source_health,
    }


@app.get("/stats/sources/coverage")
def stats_sources_coverage():
    return compute_coverage(days=7)


@app.get("/stats/sources/health")
def stats_sources_health():
    return compute_ingest_health(window_hours=24)


class ReportRequest(BaseModel):
    start: str | None = None
    end: str | None = None
    language: str | None = None
    keywords: list[str] | None = None
    include_sources: bool = True
    limit: int = 50


@app.post("/reports/generate")
def generate_report(payload: ReportRequest = Body(...)):
    sb = get_client()
    now = datetime.now(timezone.utc)
    default_start = (now - timedelta(days=7)).isoformat()

    start = _parse_iso(payload.start) or _parse_iso(default_start)
    end = _parse_iso(payload.end) or now

    q = (
        sb.table("feed_items")
        .select("id,source_id,source_type,url,title,summary,content,language,published_at")
        .eq("is_deleted", False)
        .gte("published_at", start.isoformat() if start else default_start)
        .lte("published_at", end.isoformat())
        .order("published_at", desc=True)
        .limit(payload.limit)
    )

    if payload.language:
        q = q.eq("language", payload.language)

    res = q.execute()
    items = res.data or []

    keywords = [k.strip().lower() for k in (payload.keywords or []) if k.strip()]

    source_ids = list({item.get("source_id") for item in items if item.get("source_id")})
    source_keys: dict[str, str] = {}
    if source_ids:
        key_col = get_key_column(sb)
        src_res = sb.table("sources").select(f"id,{key_col}").in_("id", source_ids).execute()
        for row in src_res.data or []:
            source_keys[row["id"]] = row.get(key_col) or row["id"]

    source_weight = {
        "cbl": 3.0,
        "noc": 2.8,
        "unsmil": 2.4,
        "world_bank_libya": 2.2,
        "imf_libya": 2.2,
    }

    def score_item(item: dict) -> float:
        text = " ".join(
            [
                item.get("title") or "",
                item.get("summary") or "",
                item.get("content") or "",
            ]
        ).lower()

        keyword_hits = sum(1 for k in keywords if k in text)
        published_at = _parse_iso(item.get("published_at")) or now
        hours_ago = max((now - published_at).total_seconds() / 3600, 0)
        recency_score = max(0.0, 72 - hours_ago) / 72
        src_key = source_keys.get(item.get("source_id"), "")
        importance = source_weight.get(src_key, 1.0)
        return keyword_hits * 2.5 + recency_score + importance

    ranked = sorted(items, key=score_item, reverse=True)

    def section_lines(title: str, section_items: list[dict]) -> list[str]:
        lines = [f"## {title}", ""]
        for item in section_items[:10]:
            headline = item.get("title") or (item.get("summary") or "").split("\n")[0]
            url = item.get("url") or ""
            published_at = item.get("published_at") or ""
            url_suffix = f" — {url}" if url else ""
            lines.append(f"- {headline} ({published_at}){url_suffix}")
        lines.append("")
        return lines

    social_items = [i for i in ranked if i.get("source_type") == "social"]
    article_items = [i for i in ranked if i.get("source_type") != "social"]

    report_lines = [
        "# LibyaIntel Report",
        "",
        f"**Period:** {start.isoformat() if start else default_start} → {end.isoformat()}",
        "",
        "## Executive Summary",
        "",
        "- Deterministic summary (LLM optional).",
        "",
    ]

    if os.environ.get("REPORT_USE_OLLAMA") == "1":
        try:
            executive = summarize_items(ranked[:12])
            if executive:
                report_lines = report_lines[:-2] + [executive, ""]
        except Exception:
            pass

    report_lines += section_lines("Key Developments", article_items)
    report_lines += section_lines("Notable Social Signals", social_items)

    if payload.include_sources:
        source_ids = sorted({item.get("source_id") for item in ranked if item.get("source_id")})
        source_names: dict[str, str] = {}
        if source_ids:
            key_col = get_key_column(sb)
            src_res = (
                sb.table("sources")
                .select(f"id,name,{key_col}")
                .in_("id", source_ids)
                .execute()
            )
            source_names = {
                row["id"]: row.get("name") or row.get(key_col)
                for row in (src_res.data or [])
            }

        report_lines.append("## Sources")
        report_lines.append("")
        for sid in source_ids:
            report_lines.append(f"- {source_names.get(sid, sid)}")
        report_lines.append("")

    markdown = "\n".join(report_lines).strip() + "\n"
    return {"markdown": markdown, "items": ranked[:20]}


class ReportSaveRequest(BaseModel):
    title: str
    markdown: str
    metadata: dict | None = None


@app.post("/reports/save")
def save_report(payload: ReportSaveRequest = Body(...), _=Depends(require_admin)):
    sb = get_client()
    row = {
        "title": payload.title,
        "markdown": payload.markdown,
        "metadata": payload.metadata or {},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    res = sb.table("reports").insert(row).execute()
    data = res.data or []
    report_id = data[0].get("id") if data else None
    return {"ok": True, "id": report_id}


class SavedSearchRequest(BaseModel):
    name: str
    query: str | None = None
    days: int = 7
    category: str | None = None
    source: str | None = None


@app.get("/private/saved-searches")
def list_saved_searches(user=Depends(require_user)):
    sb = get_client()
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    res = (
        sb.table("saved_searches")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    return {"items": res.data or []}


@app.post("/private/saved-searches")
def create_saved_search(payload: SavedSearchRequest = Body(...), user=Depends(require_user)):
    sb = get_client()
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    row = {
        "user_id": user_id,
        "name": payload.name,
        "query": payload.query,
        "days": payload.days,
        "category": payload.category,
        "source": payload.source,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    res = sb.table("saved_searches").insert(row).execute()
    return {"item": (res.data or [None])[0]}


class AlertRequest(BaseModel):
    saved_search_id: int
    channel: str
    target: str
    active: bool = True


@app.get("/private/alerts")
def list_alerts(user=Depends(require_user)):
    sb = get_client()
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    res = (
        sb.table("alerts")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    return {"items": res.data or []}


@app.post("/private/alerts")
def create_alert(payload: AlertRequest = Body(...), user=Depends(require_user)):
    sb = get_client()
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    row = {
        "user_id": user_id,
        "saved_search_id": payload.saved_search_id,
        "channel": payload.channel,
        "target": payload.target,
        "active": payload.active,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    res = sb.table("alerts").insert(row).execute()
    return {"item": (res.data or [None])[0]}


@app.post("/private/reports/generate")
def private_generate_report(payload: ReportRequest = Body(...), user=Depends(require_user)):
    return generate_report(payload)


@app.post("/private/reports/save")
def private_save_report(payload: ReportSaveRequest = Body(...), user=Depends(require_user)):
    sb = get_client()
    user_id = user.get("id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    row = {
        "title": payload.title,
        "markdown": payload.markdown,
        "metadata": payload.metadata or {},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if _table_has_column(sb, "reports", "user_id"):
        row["user_id"] = user_id
    res = sb.table("reports").insert(row).execute()
    data = res.data or []
    report_id = data[0].get("id") if data else None
    return {"ok": True, "id": report_id}

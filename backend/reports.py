from datetime import datetime, timezone

from backend.db import get_key_column
from backend.llm.ollama import summarize as summarize_items


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def build_report_markdown(
    sb,
    items: list[dict],
    start: datetime,
    end: datetime,
    keywords: list[str] | None = None,
    include_sources: bool = True,
    use_ollama: bool = False,
) -> tuple[str, list[dict]]:
    now = datetime.now(timezone.utc)
    keywords = [k.strip().lower() for k in (keywords or []) if k.strip()]

    source_ids = list({item.get("source_id") for item in items if item.get("source_id")})
    source_meta: dict[str, dict] = {}
    source_names: dict[str, str] = {}

    if source_ids:
        key_col = get_key_column(sb)
        src_res = (
            sb.table("sources")
            .select(f"id,name,meta,{key_col}")
            .in_("id", source_ids)
            .execute()
        )
        for row in src_res.data or []:
            source_meta[row["id"]] = row.get("meta") or {}
            source_names[row["id"]] = row.get("name") or row.get(key_col) or row["id"]

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
        recency_weight = max(0.0, 72 - hours_ago) / 72

        meta = source_meta.get(item.get("source_id"), {})
        source_priority = float(meta.get("priority", 0) or 0)

        return recency_weight + source_priority + keyword_hits

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
        f"**Period:** {start.isoformat()} → {end.isoformat()}",
        "",
        "## Executive Summary",
        "",
        "- Deterministic summary (LLM optional).",
        "",
    ]

    if use_ollama:
        try:
            executive = summarize_items(ranked[:12])
            if executive:
                report_lines = report_lines[:-2] + [executive, ""]
        except Exception:
            pass

    report_lines += section_lines("Key Developments", article_items)
    report_lines += section_lines("Notable Social Signals", social_items)

    if include_sources:
        report_lines.append("## Sources")
        report_lines.append("")
        for sid in sorted(source_names.keys()):
            report_lines.append(f"- {source_names.get(sid, sid)}")
        report_lines.append("")

    markdown = "\n".join(report_lines).strip() + "\n"
    return markdown, ranked

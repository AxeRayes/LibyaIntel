import os

import requests


def ollama_generate(prompt: str, model: str | None = None, timeout: int = 120) -> str:
    base_url = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
    model_name = model or os.getenv("OLLAMA_MODEL", "llama3")

    resp = requests.post(
        base_url,
        json={"model": model_name, "prompt": prompt, "stream": False},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    return (data.get("response") or "").strip()


def summarize(items: list[dict]) -> str:
    if not items:
        return ""

    lines = ["Summarize the following items in 4-6 concise bullet points:", ""]
    for item in items[:12]:
        title = item.get("title") or ""
        summary = item.get("summary") or item.get("content") or ""
        summary = " ".join(summary.split())[:240]
        lines.append(f"- {title} — {summary}")

    prompt = "\n".join(lines)
    return ollama_generate(prompt)

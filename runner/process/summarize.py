import os
import re
import time
import signal
from contextlib import contextmanager

import requests
from dotenv import load_dotenv
from requests.exceptions import ConnectionError, Timeout, ReadTimeout, HTTPError, ConnectTimeout

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "12"))
OLLAMA_WALL_TIMEOUT = int(os.getenv("OLLAMA_WALL_TIMEOUT", str(OLLAMA_TIMEOUT)))
OLLAMA_RETRIES = int(os.getenv("OLLAMA_RETRIES", "1"))
MAX_INPUT_CHARS = int(os.getenv("SUMMARY_MAX_CHARS", "5000"))
MAX_OUTPUT_TOKENS = 120
SUMMARY_TEMPERATURE = 0.2
_LLM_CALL_LOGGED = False


class WallClockTimeout(Exception):
    pass


class LLMUnavailable(Exception):
    pass


class LLMError(Exception):
    pass


@contextmanager
def _wall_clock_timeout(seconds: int):
    if not seconds or seconds <= 0:
        yield
        return
    try:
        previous = signal.getsignal(signal.SIGALRM)
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, previous)
    except ValueError:
        # signal.alarm only works in main thread; skip hard timeout if unavailable
        yield


def _alarm_handler(signum, frame):
    raise WallClockTimeout()


def clean_text(text: str) -> str:
    text = (text or "").strip()
    if "<" in text and ">" in text:
        text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _compact_text(text: str, max_chars: int = MAX_INPUT_CHARS) -> str:
    text = clean_text(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def summarize(text: str) -> str:
    text = _compact_text(text)
    if len(text) < 200:
        return ""

    prompt = (
        "Summarize in 3 bullets and 1 sentence on why it matters. "
        "If Arabic, respond in Arabic. No intro text.\n\n"
        f"TEXT:\n{text}\n"
    )
    global _LLM_CALL_LOGGED
    if not _LLM_CALL_LOGGED:
        word_count = len(text.split())
        print(
            "SUMMARY_LLM_CALL "
            f"timeout={OLLAMA_TIMEOUT} wall_timeout={OLLAMA_WALL_TIMEOUT} "
            f"prompt_chars={len(prompt)} text_words={word_count} "
            f"max_chars={MAX_INPUT_CHARS} model={OLLAMA_MODEL}"
        )
        _LLM_CALL_LOGGED = True

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": MAX_OUTPUT_TOKENS,  # keep output short
            "temperature": SUMMARY_TEMPERATURE,
        },
    }

    connect_timeout = 5
    read_timeout = max(1, int(OLLAMA_TIMEOUT))
    wall_timeout = max(read_timeout + 2, int(OLLAMA_WALL_TIMEOUT))

    with _wall_clock_timeout(wall_timeout):
        last_exc: Exception | None = None
        for attempt in range(OLLAMA_RETRIES + 1):
            try:
                resp = requests.post(
                    f"{OLLAMA_BASE_URL}/api/generate",
                    json=payload,
                    timeout=(connect_timeout, read_timeout),
                )
                resp.raise_for_status()
                data = resp.json() or {}
                return (data.get("response") or "").strip()

            except ReadTimeout as e:
                last_exc = e
                raise WallClockTimeout(f"ollama_read_timeout: {e}") from e

            except ConnectTimeout as e:
                last_exc = e
                if attempt < OLLAMA_RETRIES:
                    time.sleep(0.5)
                    continue
                raise LLMUnavailable(f"ollama_connect_timeout: {e}") from e

            except ConnectionError as e:
                last_exc = e
                if attempt < OLLAMA_RETRIES:
                    time.sleep(0.5)
                    continue
                raise LLMUnavailable(f"ollama_connection_error: {e}") from e

            except HTTPError as e:
                last_exc = e
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status and status >= 500 and attempt < OLLAMA_RETRIES:
                    time.sleep(0.5)
                    continue
                raise LLMError(f"ollama_http_error status={status}: {e}") from e

            except Timeout as e:
                last_exc = e
                raise WallClockTimeout(f"ollama_timeout: {e}") from e

        raise LLMUnavailable(f"ollama_failed: {last_exc}") from last_exc

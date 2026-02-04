import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

JOBS = [
    ("rss_ingest", "runner.ingest.rss_ingest", "RSS_INGEST_TIMEOUT", 300),
    ("page_ingest", "runner.ingest.page_ingest", "PAGE_INGEST_TIMEOUT", 600),
    ("social_inbox_ingest", "ingest.social_inbox_ingest", "SOCIAL_INGEST_TIMEOUT", 300),
]


def run_job(name: str, module: str, timeout_env: str, default_timeout: int) -> int:
    print(f"Running {name}...")
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{existing}" if existing else str(REPO_ROOT)
    timeout_sec = int(env.get(timeout_env, str(default_timeout)))
    try:
        subprocess.run(
            [sys.executable, "-m", module],
            cwd=REPO_ROOT,
            env=env,
            check=True,
            timeout=timeout_sec,
        )
        print(f"Job ok: {name}")
        return 0
    except subprocess.TimeoutExpired:
        print(f"Job timed out: {name} after {timeout_sec}s")
        return 1
    except subprocess.CalledProcessError as e:
        print(f"Job failed: {name} ({e.returncode})")
        return e.returncode or 1


def main() -> int:
    failures = 0
    for name, module, timeout_env, default_timeout in JOBS:
        rc = run_job(name, module, timeout_env, default_timeout)
        if rc != 0:
            failures += 1

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())

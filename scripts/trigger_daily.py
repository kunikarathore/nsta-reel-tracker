from __future__ import annotations

import os
import sys

import httpx


def main() -> int:
    base_url = os.getenv("APP_BASE_URL", "").strip().rstrip("/")
    secret = os.getenv("CRON_SECRET", "").strip()

    if not base_url:
        print("APP_BASE_URL is required")
        return 2
    if not secret:
        print("CRON_SECRET is required")
        return 2

    url = f"{base_url}/api/cron/daily"
    try:
        response = httpx.post(url, headers={"X-Cron-Secret": secret}, timeout=120)
        print(response.status_code, response.text)
        return 0 if response.status_code < 300 else 1
    except Exception as exc:  # noqa: BLE001
        print(f"Cron call failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

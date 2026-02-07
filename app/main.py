from __future__ import annotations

import asyncio
import contextlib
import csv
import io
import json
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException
from fastapi import Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "tracker.db"
STATIC_DIR = BASE_DIR / "static"
DEFAULT_SQLITE_URL = f"sqlite:///{DB_PATH}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_SQLITE_URL).strip()
IS_POSTGRES = DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")

DEFAULT_APIFY_ACTOR_ID = "apify/instagram-reel-scraper"
DAILY_RUN_HOUR = int(os.getenv("DAILY_RUN_HOUR", "9"))
APIFY_MAX_CONCURRENT_RUNS = max(1, int(os.getenv("APIFY_MAX_CONCURRENT_RUNS", "1")))
POLL_BATCH_SIZE = max(1, int(os.getenv("POLL_BATCH_SIZE", "5")))
FETCH_RETRY_COUNT = max(1, int(os.getenv("FETCH_RETRY_COUNT", "2")))
ENABLE_INTERNAL_SCHEDULER = os.getenv("ENABLE_INTERNAL_SCHEDULER", "1").strip() in {"1", "true", "yes"}
CRON_SECRET = os.getenv("CRON_SECRET", "").strip()
MANUAL_POLL_ENABLED = os.getenv("MANUAL_POLL_ENABLED", "1").strip() in {"1", "true", "yes"}
INSTAGRAM_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


class AddPostRequest(BaseModel):
    campaign_name: str = Field(min_length=1, max_length=120)
    creator_handle: str = Field(min_length=1, max_length=120)
    post_url: str = Field(min_length=10, max_length=600)
    poll_interval_sec: int = Field(default=86400, ge=60, le=86400)

    @field_validator("post_url")
    @classmethod
    def validate_post_url(cls, value: str) -> str:
        if "instagram.com" not in value:
            raise ValueError("post_url must be an Instagram URL")
        normalized = normalize_instagram_url(value)
        if parse_shortcode(normalized) is None:
            raise ValueError("post_url must be a Reel/Post link like /reel/... or /p/...")
        return normalized


class PollRequest(BaseModel):
    post_id: int | None = None


class BulkAddRequest(BaseModel):
    campaign_name: str = Field(min_length=1, max_length=120)
    bulk_text: str = Field(min_length=1, max_length=200000)
    poll_interval_sec: int = Field(default=86400, ge=60, le=86400)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_instagram_url(url: str) -> str:
    normalized = url.strip()
    normalized = normalized.split("?")[0]
    if normalized.endswith("/"):
        normalized = normalized[:-1]
    return normalized


def parse_profile_handle(url: str) -> str | None:
    normalized = normalize_instagram_url(url)
    match = re.search(r"instagram\.com/([A-Za-z0-9._]+)/?$", normalized)
    if match:
        handle = match.group(1)
        if handle not in {"reel", "p", "tv"}:
            return handle
    return None


def get_db() -> sqlite3.Connection:
    if IS_POSTGRES:
        pg_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        return psycopg.connect(pg_url, row_factory=dict_row)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_execute(conn: Any, query: str, params: tuple[Any, ...] = ()) -> Any:
    if IS_POSTGRES:
        return conn.execute(query.replace("?", "%s"), params)
    return conn.execute(query, params)


def db_fetchone(conn: Any, query: str, params: tuple[Any, ...] = ()) -> Any:
    return db_execute(conn, query, params).fetchone()


def db_fetchall(conn: Any, query: str, params: tuple[Any, ...] = ()) -> Any:
    return db_execute(conn, query, params).fetchall()


def is_unique_violation(exc: Exception) -> bool:
    if isinstance(exc, sqlite3.IntegrityError):
        return True
    code = getattr(exc, "sqlstate", None)
    return code == "23505"


def insert_scheduled_snapshot(conn: Any, post_id: int) -> None:
    db_execute(
        conn,
        """
        INSERT INTO snapshots(post_id, fetched_at, likes, comments, views, source_status, source_error)
        VALUES (?, ?, NULL, NULL, NULL, 'scheduled', NULL)
        """,
        (post_id, utc_now_iso()),
    )


def init_db() -> None:
    with closing(get_db()) as conn:
        if IS_POSTGRES:
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS campaigns (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                )
                """,
            )
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS creators (
                    id BIGSERIAL PRIMARY KEY,
                    handle TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    display_name TEXT,
                    profile_url TEXT,
                    followers_text TEXT
                )
                """,
            )
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS posts (
                    id BIGSERIAL PRIMARY KEY,
                    campaign_id BIGINT NOT NULL REFERENCES campaigns(id),
                    creator_id BIGINT NOT NULL REFERENCES creators(id),
                    post_url TEXT NOT NULL UNIQUE,
                    shortcode TEXT,
                    poll_interval_sec INTEGER NOT NULL DEFAULT 300,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_polled_at TEXT
                )
                """,
            )
            db_execute(
                conn,
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    post_id BIGINT NOT NULL REFERENCES posts(id),
                    fetched_at TEXT NOT NULL,
                    likes INTEGER,
                    comments INTEGER,
                    views INTEGER,
                    source_status TEXT NOT NULL,
                    source_error TEXT
                )
                """,
            )
            db_execute(
                conn,
                "CREATE INDEX IF NOT EXISTS idx_snapshots_post_time ON snapshots(post_id, fetched_at DESC)",
            )
            db_execute(
                conn,
                "CREATE INDEX IF NOT EXISTS idx_posts_active ON posts(active, last_polled_at)",
            )
        else:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS creators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    handle TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER NOT NULL,
                    creator_id INTEGER NOT NULL,
                    post_url TEXT NOT NULL UNIQUE,
                    shortcode TEXT,
                    poll_interval_sec INTEGER NOT NULL DEFAULT 300,
                    active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_polled_at TEXT,
                    FOREIGN KEY (campaign_id) REFERENCES campaigns(id),
                    FOREIGN KEY (creator_id) REFERENCES creators(id)
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id INTEGER NOT NULL,
                    fetched_at TEXT NOT NULL,
                    likes INTEGER,
                    comments INTEGER,
                    views INTEGER,
                    source_status TEXT NOT NULL,
                    source_error TEXT,
                    FOREIGN KEY (post_id) REFERENCES posts(id)
                );

                CREATE INDEX IF NOT EXISTS idx_snapshots_post_time
                    ON snapshots(post_id, fetched_at DESC);

                CREATE INDEX IF NOT EXISTS idx_posts_active
                    ON posts(active, last_polled_at);
                """
            )

        if IS_POSTGRES:
            db_execute(conn, "ALTER TABLE creators ADD COLUMN IF NOT EXISTS display_name TEXT")
            db_execute(conn, "ALTER TABLE creators ADD COLUMN IF NOT EXISTS profile_url TEXT")
            db_execute(conn, "ALTER TABLE creators ADD COLUMN IF NOT EXISTS followers_text TEXT")
        else:
            existing_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(creators)").fetchall()
            }
            if "display_name" not in existing_columns:
                conn.execute("ALTER TABLE creators ADD COLUMN display_name TEXT")
            if "profile_url" not in existing_columns:
                conn.execute("ALTER TABLE creators ADD COLUMN profile_url TEXT")
            if "followers_text" not in existing_columns:
                conn.execute("ALTER TABLE creators ADD COLUMN followers_text TEXT")
        conn.commit()


def upsert_campaign(conn: sqlite3.Connection, name: str) -> int:
    if IS_POSTGRES:
        db_execute(
            conn,
            "INSERT INTO campaigns(name, created_at) VALUES (?, ?) ON CONFLICT(name) DO NOTHING",
            (name.strip(), utc_now_iso()),
        )
    else:
        db_execute(
            conn,
            "INSERT OR IGNORE INTO campaigns(name, created_at) VALUES (?, ?)",
            (name.strip(), utc_now_iso()),
        )
    row = db_fetchone(conn, "SELECT id FROM campaigns WHERE name = ?", (name.strip(),))
    return int(row["id"])


def upsert_creator(
    conn: sqlite3.Connection,
    handle: str,
    display_name: str | None = None,
    profile_url: str | None = None,
    followers_text: str | None = None,
) -> int:
    normalized = handle.strip().lstrip("@")
    insert_stmt = """
        INSERT INTO creators(handle, created_at, display_name, profile_url, followers_text)
        VALUES (?, ?, ?, ?, ?)
    """
    if IS_POSTGRES:
        insert_stmt += " ON CONFLICT(handle) DO NOTHING"
    else:
        insert_stmt = insert_stmt.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
    db_execute(
        conn,
        insert_stmt,
        (
            normalized,
            utc_now_iso(),
            (display_name or "").strip() or None,
            (profile_url or "").strip() or None,
            (followers_text or "").strip() or None,
        ),
    )
    db_execute(
        conn,
        """
        UPDATE creators
        SET
            display_name = COALESCE(NULLIF(?, ''), display_name),
            profile_url = COALESCE(NULLIF(?, ''), profile_url),
            followers_text = COALESCE(NULLIF(?, ''), followers_text)
        WHERE handle = ?
        """,
        (
            (display_name or "").strip(),
            (profile_url or "").strip(),
            (followers_text or "").strip(),
            normalized,
        ),
    )
    row = db_fetchone(conn, "SELECT id FROM creators WHERE handle = ?", (normalized,))
    return int(row["id"])


def parse_shortcode(url: str) -> str | None:
    match = re.search(r"instagram\.com/(?:reel|p)/([A-Za-z0-9_-]+)", url)
    if match:
        return match.group(1)
    return None


def coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").lower()
        compact_match = re.fullmatch(r"(\d+(?:\.\d+)?)([kmb])", cleaned)
        if compact_match:
            base = float(compact_match.group(1))
            unit = compact_match.group(2)
            mult = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}[unit]
            return int(base * mult)
        if cleaned.isdigit():
            return int(cleaned)
    return None


def resolve_comments(item: dict[str, Any]) -> int | None:
    comments_count = coerce_int(item.get("commentsCount"))
    if comments_count is not None:
        return comments_count
    comments_raw = item.get("comments")
    if isinstance(comments_raw, list):
        return len(comments_raw)
    return coerce_int(comments_raw)


def extract_first_int(html: str, patterns: list[str]) -> int | None:
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            coerced = coerce_int(match.group(1))
            if coerced is not None:
                return coerced
    return None


def extract_first_compact_number(html: str, pattern: str) -> int | None:
    match = re.search(pattern, html, flags=re.IGNORECASE)
    if not match:
        return None
    return coerce_int(match.group(1))


def extract_json_ld_metrics(html: str) -> dict[str, int | None]:
    likes: int | None = None
    comments: int | None = None
    views: int | None = None
    for block in re.findall(
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw = block.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        stats = data.get("interactionStatistic")
        if not isinstance(stats, list):
            continue
        for stat in stats:
            if not isinstance(stat, dict):
                continue
            value = coerce_int(stat.get("userInteractionCount"))
            interaction = stat.get("interactionType")
            interaction_name = ""
            if isinstance(interaction, dict):
                interaction_name = str(interaction.get("@type", "")).lower()
            elif isinstance(interaction, str):
                interaction_name = interaction.lower()
            if value is None:
                continue
            if "like" in interaction_name and likes is None:
                likes = value
            elif "comment" in interaction_name and comments is None:
                comments = value
            elif ("watch" in interaction_name or "view" in interaction_name or "play" in interaction_name) and views is None:
                views = value
    return {"likes": likes, "comments": comments, "views": views}


def extract_metrics_from_html(html: str) -> dict[str, int | None]:
    likes = extract_first_int(
        html,
        [
            r'"edge_media_preview_like"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'"like_count"\s*:\s*(\d+)',
            r'"likesCount"\s*:\s*(\d+)',
            r'"likes"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'content="([\d,]+)\s+likes',
        ],
    )
    comments = extract_first_int(
        html,
        [
            r'"edge_media_to_comment"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'"comment_count"\s*:\s*(\d+)',
            r'"commentsCount"\s*:\s*(\d+)',
            r'"comments"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
            r'likes,\s*([\d,]+)\s+comments',
            r'([\d,]+)\s+comments',
        ],
    )
    views = extract_first_int(
        html,
        [
            r'"video_view_count"\s*:\s*(\d+)',
            r'"video_play_count"\s*:\s*(\d+)',
            r'"videoPlayCount"\s*:\s*(\d+)',
            r'"play_count"\s*:\s*(\d+)',
            r'"videoViewCount"\s*:\s*(\d+)',
            r'"interactionStatistic"\s*:\s*\[.*?"WatchAction".*?"userInteractionCount"\s*:\s*(\d+)',
        ],
    )
    if likes is None:
        likes = extract_first_compact_number(
            html,
            r'([0-9][0-9,\.]*\s*[kmb]?)\s+(?:likes|like)\b',
        )
    if comments is None:
        comments = extract_first_compact_number(
            html,
            r'([0-9][0-9,\.]*\s*[kmb]?)\s+(?:comments|comment)\b',
        )
    if views is None:
        views = extract_first_compact_number(
            html,
            r'([0-9][0-9,\.]*\s*[kmb]?)\s+(?:views|view|plays|play)\b',
        )

    json_ld = extract_json_ld_metrics(html)
    if likes is None:
        likes = json_ld["likes"]
    if comments is None:
        comments = json_ld["comments"]
    if views is None:
        views = json_ld["views"]

    return {"likes": likes, "comments": comments, "views": views}


def find_first_key_int(data: Any, keys: tuple[str, ...]) -> int | None:
    if isinstance(data, dict):
        for key in keys:
            if key in data:
                parsed = coerce_int(data.get(key))
                if parsed is not None:
                    return parsed
        for value in data.values():
            found = find_first_key_int(value, keys)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = find_first_key_int(item, keys)
            if found is not None:
                return found
    return None


async def fetch_metrics_from_internal(url: str) -> dict[str, Any]:
    normalized = normalize_instagram_url(url)
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.get(normalized, headers=INSTAGRAM_HEADERS)
        if response.status_code >= 400:
            raise RuntimeError(f"Internal scraper request failed: HTTP {response.status_code}")
        if "Please wait a few minutes before you try again" in response.text:
            raise RuntimeError("Instagram rate-limited internal scraper request")
        metrics = extract_metrics_from_html(response.text)

        # Fallback to Instagram JSON payload for public posts when available.
        if all(metrics[key] is None for key in ("likes", "comments", "views")):
            json_url = f"{normalized}/?__a=1&__d=dis"
            json_resp = await client.get(json_url, headers=INSTAGRAM_HEADERS)
            if json_resp.status_code < 400:
                try:
                    data = json_resp.json()
                    metrics = {
                        "likes": find_first_key_int(data, ("like_count", "likes_count", "likesCount")),
                        "comments": find_first_key_int(data, ("comment_count", "comments_count", "commentsCount")),
                        "views": find_first_key_int(
                            data,
                            ("play_count", "video_view_count", "video_play_count", "videoPlayCount", "videoViewCount"),
                        ),
                    }
                except Exception:
                    pass

        if all(metrics[key] is None for key in ("likes", "comments", "views")):
            embed_url = f"{normalized}/embed/captioned"
            embed_response = await client.get(embed_url, headers=INSTAGRAM_HEADERS)
            if embed_response.status_code < 400:
                metrics = extract_metrics_from_html(embed_response.text)
    if all(metrics[key] is None for key in ("likes", "comments", "views")):
        raise RuntimeError("Internal scraper could not parse views/likes/comments")
    return metrics


async def fetch_metrics_from_apify(url: str) -> dict[str, Any]:
    token = os.getenv("APIFY_TOKEN", "").strip()
    if not token:
        raise RuntimeError("APIFY_TOKEN is not set")

    actor_id = os.getenv("APIFY_ACTOR_ID", DEFAULT_APIFY_ACTOR_ID).strip() or DEFAULT_APIFY_ACTOR_ID
    actor_path = actor_id.replace("/", "~")
    endpoint = f"https://api.apify.com/v2/acts/{actor_path}/run-sync-get-dataset-items"
    payload = {"username": [url]}

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.post(endpoint, params={"token": token}, json=payload)

    if response.status_code >= 400:
        raise RuntimeError(f"Apify actor call failed: HTTP {response.status_code} {response.text[:300]}")

    items = response.json()
    if not isinstance(items, list) or not items:
        raise RuntimeError("Apify actor returned no items")

    item = items[0] if isinstance(items[0], dict) else {}
    metrics = {
        "views": coerce_int(item.get("videoPlayCount"))
        or coerce_int(item.get("videoViewCount"))
        or coerce_int(item.get("playCount")),
        "likes": coerce_int(item.get("likesCount")) or coerce_int(item.get("likes")),
        "comments": resolve_comments(item),
    }
    if all(metrics[key] is None for key in ("likes", "comments", "views")):
        raise RuntimeError("Apify response missing views/likes/comments fields")

    return metrics


async def fetch_instagram_metrics(url: str) -> dict[str, Any]:
    errors: list[str] = []
    for attempt in range(FETCH_RETRY_COUNT):
        try:
            metrics = await fetch_metrics_from_apify(url)
            metrics["provider"] = "apify"
            return metrics
        except Exception as exc:  # noqa: BLE001
            errors.append(f"apify attempt {attempt + 1}: {exc}")
            if attempt < FETCH_RETRY_COUNT - 1:
                await asyncio.sleep(1.5 * (attempt + 1))
    raise RuntimeError(" | ".join(errors[:4]))


async def poll_post(post_id: int) -> None:
    with closing(get_db()) as conn:
        post = db_fetchone(
            conn,
            """
            SELECT p.id, p.post_url
            FROM posts p
            WHERE p.id = ? AND p.active = 1
            """,
            (post_id,),
        )

    if not post:
        return

    now = utc_now_iso()
    status = "ok"
    error: str | None = None
    likes: int | None = None
    comments: int | None = None
    views: int | None = None

    try:
        metrics = await fetch_instagram_metrics(post["post_url"])
        likes = metrics["likes"]
        comments = metrics["comments"]
        views = metrics["views"]
        status = f"ok:{metrics.get('provider', 'unknown')}"
    except Exception as exc:  # noqa: BLE001
        status = "error"
        error = str(exc)

    with closing(get_db()) as conn:
        db_execute(
            conn,
            """
            INSERT INTO snapshots(post_id, fetched_at, likes, comments, views, source_status, source_error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (post_id, now, likes, comments, views, status, error),
        )
        db_execute(
            conn,
            "UPDATE posts SET last_polled_at = ? WHERE id = ?",
            (now, post_id),
        )
        conn.commit()


async def poll_due_posts() -> int:
    with closing(get_db()) as conn:
        rows = db_fetchall(
            conn,
            """
            SELECT id, last_polled_at, poll_interval_sec
            FROM posts
            WHERE active = 1
            """
        )

    if not rows:
        return 0

    due_post_ids: list[int] = []
    now = datetime.now(timezone.utc)
    for row in rows:
        last_polled_raw = row["last_polled_at"]
        interval = int(row["poll_interval_sec"] or 300)
        if not last_polled_raw:
            due_post_ids.append(int(row["id"]))
            continue
        try:
            last_polled = datetime.fromisoformat(last_polled_raw)
            if last_polled.tzinfo is None:
                last_polled = last_polled.replace(tzinfo=timezone.utc)
            if now >= last_polled + timedelta(seconds=interval):
                due_post_ids.append(int(row["id"]))
        except Exception:
            due_post_ids.append(int(row["id"]))

    if not due_post_ids:
        return 0
    await poll_post_ids(due_post_ids)
    return len(due_post_ids)


async def poll_all_active_posts() -> int:
    with closing(get_db()) as conn:
        post_ids = [int(row["id"]) for row in db_fetchall(conn, "SELECT id FROM posts WHERE active = 1")]
    if not post_ids:
        return 0
    await poll_post_ids(post_ids)
    return len(post_ids)


async def poll_post_ids(post_ids: list[int]) -> None:
    if not post_ids:
        return

    concurrent_per_batch = min(APIFY_MAX_CONCURRENT_RUNS, POLL_BATCH_SIZE)
    for start in range(0, len(post_ids), POLL_BATCH_SIZE):
        chunk = post_ids[start : start + POLL_BATCH_SIZE]
        if concurrent_per_batch >= len(chunk):
            await asyncio.gather(*(poll_post(post_id) for post_id in chunk))
            continue

        semaphore = asyncio.Semaphore(concurrent_per_batch)

        async def worker(post_id: int) -> None:
            async with semaphore:
                await poll_post(post_id)

        await asyncio.gather(*(worker(post_id) for post_id in chunk))


def seconds_until_next_daily_run(hour: int) -> float:
    now = datetime.now().astimezone()
    next_run = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    return max((next_run - now).total_seconds(), 1.0)


async def polling_loop() -> None:
    while True:
        try:
            await asyncio.sleep(seconds_until_next_daily_run(DAILY_RUN_HOUR))
            await poll_all_active_posts()
        except Exception:
            # Keep loop alive even if one cycle fails.
            pass


app = FastAPI(title="Instagram Reel Campaign Tracker", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.on_event("startup")
async def startup_event() -> None:
    init_db()
    app.state.poll_task = None
    if ENABLE_INTERNAL_SCHEDULER:
        app.state.poll_task = asyncio.create_task(polling_loop())


@app.on_event("shutdown")
async def shutdown_event() -> None:
    poll_task = getattr(app.state, "poll_task", None)
    if poll_task:
        poll_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poll_task


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "time": utc_now_iso()}


@app.post("/api/cron/daily")
async def cron_daily(x_cron_secret: str | None = Header(default=None)) -> dict[str, Any]:
    if not CRON_SECRET:
        raise HTTPException(status_code=503, detail="CRON_SECRET not configured")
    if (x_cron_secret or "").strip() != CRON_SECRET:
        raise HTTPException(status_code=401, detail="Invalid cron secret")
    polled = await poll_all_active_posts()
    return {"ok": True, "polled": polled, "ran_at": utc_now_iso()}


@app.get("/")
async def home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/campaign/{campaign_id}")
async def campaign_view(campaign_id: int) -> FileResponse:
    return FileResponse(STATIC_DIR / "campaign.html")


@app.post("/api/posts")
async def add_post(payload: AddPostRequest) -> dict[str, Any]:
    with closing(get_db()) as conn:
        campaign_id = upsert_campaign(conn, payload.campaign_name)
        creator_id = upsert_creator(conn, payload.creator_handle)
        shortcode = parse_shortcode(payload.post_url)

        try:
            cur = db_execute(
                conn,
                """
                INSERT INTO posts(
                    campaign_id, creator_id, post_url, shortcode,
                    poll_interval_sec, active, created_at
                ) VALUES (?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    campaign_id,
                    creator_id,
                    payload.post_url,
                    shortcode,
                    86400,
                    utc_now_iso(),
                ),
            )
            post_row = db_fetchone(conn, "SELECT id FROM posts WHERE post_url = ?", (payload.post_url,))
            post_id = int(post_row["id"])
            insert_scheduled_snapshot(conn, post_id)
        except Exception as exc:
            if not is_unique_violation(exc):
                raise
            raise HTTPException(status_code=409, detail="Post URL already exists") from exc

        conn.commit()

    return {"ok": True, "post_id": post_id}


@app.post("/api/posts/bulk")
async def bulk_add_posts(payload: BulkAddRequest) -> dict[str, Any]:
    raw_rows = list(
        csv.DictReader(
            io.StringIO(payload.bulk_text.strip()),
            delimiter="\t",
        )
    )
    if not raw_rows:
        raise HTTPException(status_code=400, detail="No rows found in bulk text")

    def norm_header(key: str) -> str:
        return key.replace("\ufeff", "").strip().lower()

    def row_value(row: dict[str, Any], key: str) -> str:
        for raw_key, raw_value in row.items():
            if norm_header(raw_key) == key:
                return str(raw_value or "")
        return ""

    required_columns = {"name", "profile link", "followers", "live link"}
    incoming_columns = {norm_header(k) for k in raw_rows[0].keys()}
    if not required_columns.issubset(incoming_columns):
        raise HTTPException(
            status_code=400,
            detail="Bulk format must include: Name, Profile Link, Followers, Live Link",
        )

    inserted_post_ids: list[int] = []
    errors: list[dict[str, Any]] = []

    with closing(get_db()) as conn:
        campaign_id = upsert_campaign(conn, payload.campaign_name)
        for idx, row in enumerate(raw_rows, start=2):
            name = row_value(row, "name").strip()
            profile_link = row_value(row, "profile link").strip()
            followers = row_value(row, "followers").strip()
            live_link = row_value(row, "live link").strip()
            if not live_link:
                errors.append({"line": idx, "reason": "Missing Live Link"})
                continue
            post_url = normalize_instagram_url(live_link)
            shortcode = parse_shortcode(post_url)
            if shortcode is None:
                errors.append({"line": idx, "reason": "Live Link is not /reel/ or /p/ URL"})
                continue

            creator_handle = parse_profile_handle(profile_link) or re.sub(r"\s+", "_", name.lower()).strip("_")
            if not creator_handle:
                errors.append({"line": idx, "reason": "Could not resolve creator handle"})
                continue

            creator_id = upsert_creator(
                conn,
                creator_handle,
                display_name=name or None,
                profile_url=normalize_instagram_url(profile_link) if profile_link else None,
                followers_text=followers or None,
            )
            try:
                db_execute(
                    conn,
                    """
                    INSERT INTO posts(
                        campaign_id, creator_id, post_url, shortcode,
                        poll_interval_sec, active, created_at
                    ) VALUES (?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        campaign_id,
                        creator_id,
                        post_url,
                        shortcode,
                        86400,
                        utc_now_iso(),
                    ),
                )
                inserted_row = db_fetchone(conn, "SELECT id FROM posts WHERE post_url = ?", (post_url,))
                inserted_post_ids.append(int(inserted_row["id"]))
                insert_scheduled_snapshot(conn, int(inserted_row["id"]))
            except Exception as exc:
                if not is_unique_violation(exc):
                    raise
                errors.append({"line": idx, "reason": "Duplicate post URL"})

        conn.commit()

    # Bulk flow: fetch once immediately so first metrics are available,
    # then continue with daily 9 AM updates afterward.
    if inserted_post_ids:
        await poll_post_ids(inserted_post_ids)

    return {
        "ok": True,
        "inserted": len(inserted_post_ids),
        "initial_polled": len(inserted_post_ids),
        "errors": errors,
    }


@app.post("/api/poll-now")
async def poll_now(payload: PollRequest) -> dict[str, Any]:
    if not MANUAL_POLL_ENABLED:
        raise HTTPException(status_code=403, detail="Manual polling is disabled. Updates run daily at 9:00 AM.")

    if payload.post_id is not None:
        await poll_post(payload.post_id)
        return {"ok": True, "polled": 1}

    with closing(get_db()) as conn:
        post_ids = [int(row["id"]) for row in db_fetchall(conn, "SELECT id FROM posts WHERE active = 1")]

    await poll_post_ids(post_ids)
    return {"ok": True, "polled": len(post_ids)}


@app.delete("/api/creators/{creator_id}")
async def delete_creator(creator_id: int) -> dict[str, Any]:
    with closing(get_db()) as conn:
        creator_row = db_fetchone(
            conn,
            "SELECT id, handle FROM creators WHERE id = ?",
            (creator_id,),
        )
        if not creator_row:
            raise HTTPException(status_code=404, detail="Creator not found")

        post_ids = [int(row["id"]) for row in db_fetchall(conn, "SELECT id FROM posts WHERE creator_id = ?", (creator_id,))]

        deleted_snapshots = 0
        if post_ids:
            placeholders = ",".join("?" for _ in post_ids)
            deleted_snapshots = db_execute(
                conn,
                f"DELETE FROM snapshots WHERE post_id IN ({placeholders})",
                post_ids,
            ).rowcount

        deleted_posts = db_execute(
            conn,
            "DELETE FROM posts WHERE creator_id = ?",
            (creator_id,),
        ).rowcount
        db_execute(conn, "DELETE FROM creators WHERE id = ?", (creator_id,))
        conn.commit()

    return {
        "ok": True,
        "creator_id": creator_id,
        "creator_handle": creator_row["handle"],
        "deleted_posts": deleted_posts,
        "deleted_snapshots": deleted_snapshots,
    }


@app.delete("/api/all-data")
async def delete_all_data() -> dict[str, Any]:
    with closing(get_db()) as conn:
        deleted_snapshots = db_execute(conn, "DELETE FROM snapshots").rowcount
        deleted_posts = db_execute(conn, "DELETE FROM posts").rowcount
        deleted_creators = db_execute(conn, "DELETE FROM creators").rowcount
        deleted_campaigns = db_execute(conn, "DELETE FROM campaigns").rowcount
        conn.commit()

    return {
        "ok": True,
        "deleted_snapshots": deleted_snapshots,
        "deleted_posts": deleted_posts,
        "deleted_creators": deleted_creators,
        "deleted_campaigns": deleted_campaigns,
    }


@app.get("/api/dashboard")
async def dashboard() -> dict[str, Any]:
    with closing(get_db()) as conn:
        post_rows = db_fetchall(
            conn,
            """
            SELECT
                p.id AS post_id,
                p.post_url,
                p.poll_interval_sec,
                p.last_polled_at,
                c.id AS campaign_id,
                c.name AS campaign_name,
                cr.id AS creator_id,
                cr.handle AS creator_handle,
                cr.display_name AS creator_name,
                cr.followers_text AS followers_text,
                COALESCE(
                    s.likes,
                    (SELECT s3.likes FROM snapshots s3 WHERE s3.post_id = p.id AND s3.likes IS NOT NULL ORDER BY s3.fetched_at DESC LIMIT 1)
                ) AS likes,
                COALESCE(
                    s.comments,
                    (SELECT s3.comments FROM snapshots s3 WHERE s3.post_id = p.id AND s3.comments IS NOT NULL ORDER BY s3.fetched_at DESC LIMIT 1)
                ) AS comments,
                COALESCE(
                    s.views,
                    (SELECT s3.views FROM snapshots s3 WHERE s3.post_id = p.id AND s3.views IS NOT NULL ORDER BY s3.fetched_at DESC LIMIT 1)
                ) AS views,
                s.source_status,
                s.source_error,
                s.fetched_at
            FROM posts p
            JOIN campaigns c ON c.id = p.campaign_id
            JOIN creators cr ON cr.id = p.creator_id
            LEFT JOIN snapshots s ON s.id = (
                SELECT s2.id
                FROM snapshots s2
                WHERE s2.post_id = p.id
                ORDER BY s2.fetched_at DESC
                LIMIT 1
            )
            WHERE p.active = 1
            ORDER BY c.name ASC, cr.handle ASC, p.id ASC
            """
        )

    posts: list[dict[str, Any]] = []
    creator_agg: dict[str, dict[str, Any]] = {}
    campaign_agg: dict[str, dict[str, Any]] = {}

    totals = {"views": 0, "likes": 0, "comments": 0, "posts": 0}

    for row in post_rows:
        views_raw = coerce_int(row["views"])
        likes_raw = coerce_int(row["likes"])
        comments_raw = coerce_int(row["comments"])
        views = int(views_raw or 0)
        likes = int(likes_raw or 0)
        comments = int(comments_raw or 0)

        posts.append(
            {
                "post_id": row["post_id"],
                "campaign_id": row["campaign_id"],
                "campaign_name": row["campaign_name"],
                "creator_id": row["creator_id"],
                "creator_handle": row["creator_handle"],
                "creator_name": row["creator_name"] or row["creator_handle"],
                "followers_text": row["followers_text"] or "-",
                "post_url": row["post_url"],
                "views": views_raw,
                "likes": likes_raw,
                "comments": comments_raw,
                "source_status": row["source_status"] or "pending",
                "source_error": row["source_error"],
                "last_snapshot_at": row["fetched_at"],
                "last_polled_at": row["last_polled_at"],
            }
        )

        totals["views"] += views
        totals["likes"] += likes
        totals["comments"] += comments
        totals["posts"] += 1

        creator_key = row["creator_handle"]
        if creator_key not in creator_agg:
            creator_agg[creator_key] = {
                "creator_id": row["creator_id"],
                "creator_handle": creator_key,
                "creator_name": row["creator_name"] or creator_key,
                "posts": 0,
                "views": 0,
                "likes": 0,
                "comments": 0,
            }
        creator_agg[creator_key]["posts"] += 1
        creator_agg[creator_key]["views"] += views
        creator_agg[creator_key]["likes"] += likes
        creator_agg[creator_key]["comments"] += comments

        campaign_key = row["campaign_name"]
        if campaign_key not in campaign_agg:
            campaign_agg[campaign_key] = {
                "campaign_id": row["campaign_id"],
                "campaign_name": campaign_key,
                "posts": 0,
                "views": 0,
                "likes": 0,
                "comments": 0,
            }
        campaign_agg[campaign_key]["posts"] += 1
        campaign_agg[campaign_key]["views"] += views
        campaign_agg[campaign_key]["likes"] += likes
        campaign_agg[campaign_key]["comments"] += comments

    return {
        "generated_at": utc_now_iso(),
        "totals": totals,
        "campaigns": sorted(campaign_agg.values(), key=lambda x: x["campaign_name"].lower()),
        "creators": sorted(creator_agg.values(), key=lambda x: x["creator_handle"].lower()),
        "posts": posts,
    }


@app.get("/api/campaigns/{campaign_id}/dashboard")
async def campaign_dashboard(campaign_id: int) -> dict[str, Any]:
    with closing(get_db()) as conn:
        campaign = db_fetchone(
            conn,
            "SELECT id, name FROM campaigns WHERE id = ?",
            (campaign_id,),
        )
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        post_rows = db_fetchall(
            conn,
            """
            SELECT
                p.id AS post_id,
                p.post_url,
                p.last_polled_at,
                cr.id AS creator_id,
                cr.handle AS creator_handle,
                cr.display_name AS creator_name,
                cr.followers_text AS followers_text,
                COALESCE(
                    s.likes,
                    (SELECT s3.likes FROM snapshots s3 WHERE s3.post_id = p.id AND s3.likes IS NOT NULL ORDER BY s3.fetched_at DESC LIMIT 1)
                ) AS likes,
                COALESCE(
                    s.comments,
                    (SELECT s3.comments FROM snapshots s3 WHERE s3.post_id = p.id AND s3.comments IS NOT NULL ORDER BY s3.fetched_at DESC LIMIT 1)
                ) AS comments,
                COALESCE(
                    s.views,
                    (SELECT s3.views FROM snapshots s3 WHERE s3.post_id = p.id AND s3.views IS NOT NULL ORDER BY s3.fetched_at DESC LIMIT 1)
                ) AS views,
                s.source_status,
                s.fetched_at
            FROM posts p
            JOIN creators cr ON cr.id = p.creator_id
            LEFT JOIN snapshots s ON s.id = (
                SELECT s2.id
                FROM snapshots s2
                WHERE s2.post_id = p.id
                ORDER BY s2.fetched_at DESC
                LIMIT 1
            )
            WHERE p.active = 1
              AND p.campaign_id = ?
            ORDER BY cr.handle ASC, p.id ASC
            """,
            (campaign_id,),
        )

    totals = {"views": 0, "likes": 0, "comments": 0, "posts": 0}
    creators: dict[str, dict[str, Any]] = {}
    posts: list[dict[str, Any]] = []

    for row in post_rows:
        views_raw = coerce_int(row["views"])
        likes_raw = coerce_int(row["likes"])
        comments_raw = coerce_int(row["comments"])
        views = int(views_raw or 0)
        likes = int(likes_raw or 0)
        comments = int(comments_raw or 0)

        posts.append(
            {
                "post_id": row["post_id"],
                "creator_id": row["creator_id"],
                "creator_handle": row["creator_handle"],
                "creator_name": row["creator_name"] or row["creator_handle"],
                "followers_text": row["followers_text"] or "-",
                "post_url": row["post_url"],
                "views": views_raw,
                "likes": likes_raw,
                "comments": comments_raw,
                "source_status": row["source_status"] or "pending",
                "last_snapshot_at": row["fetched_at"],
            }
        )

        totals["views"] += views
        totals["likes"] += likes
        totals["comments"] += comments
        totals["posts"] += 1

        key = row["creator_handle"]
        if key not in creators:
            creators[key] = {
                "creator_id": row["creator_id"],
                "creator_handle": row["creator_handle"],
                "creator_name": row["creator_name"] or row["creator_handle"],
                "posts": 0,
                "views": 0,
                "likes": 0,
                "comments": 0,
            }
        creators[key]["posts"] += 1
        creators[key]["views"] += views
        creators[key]["likes"] += likes
        creators[key]["comments"] += comments

    return {
        "generated_at": utc_now_iso(),
        "campaign": {"campaign_id": campaign["id"], "campaign_name": campaign["name"]},
        "totals": totals,
        "creators": sorted(creators.values(), key=lambda x: x["creator_handle"].lower()),
        "posts": posts,
    }

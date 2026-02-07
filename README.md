# Instagram Reel Campaign Tracker (Apify-powered MVP)

This app lets you add public Instagram post/reel links and track visible metrics in one dashboard:
- Per-post table
- Per-creator totals
- Per-campaign totals
- Global totals across all campaigns

Important:
- This is **daily-refresh tracking** (not real-time).
- This app now uses the Apify actor [`apify/instagram-reel-scraper`](https://apify.com/apify/instagram-reel-scraper).
- You must provide `APIFY_TOKEN` for scraping to work.
- Metrics are saved in DB and only update on the scheduled 9:00 AM run.

## Features
- Add multiple profiles/creators and posts from a single form
- Bulk add by pasting Excel TSV rows (`Name, Profile Link, Followers, Live Link`)
- Automatic refresh once daily at 9:00 AM (local server time)
- Campaign share pages (read-only): `/campaign/{campaign_id}`
- `Delete All` to clear all campaigns, creators, posts, and snapshots
- Delete a creator from `Post-Level Tracker` (removes all their posts + snapshots)
- Snapshot history stored in SQLite (`tracker.db`)

## Tech Stack
- FastAPI backend + Apify Actor API
- SQLite (local) / PostgreSQL (production)
- Vanilla HTML/CSS/JS dashboard

## Run locally

```bash
cd /Users/kunikarathore/Downloads/insta-reel-tracker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export APIFY_TOKEN="apify_api_xxx"
export DATABASE_URL="sqlite:///tracker.db"
# Optional override, default is apify/instagram-reel-scraper
# export APIFY_ACTOR_ID="apify/instagram-reel-scraper"
# Optional override for daily run hour (0-23), default 9
# export DAILY_RUN_HOUR="9"
# Optional safety throttle for Apify parallel runs, default 1
# export APIFY_MAX_CONCURRENT_RUNS="5"
# Batch size for update cycles, default 5
# export POLL_BATCH_SIZE="5"
# Optional retries per provider, default 2
# export FETCH_RETRY_COUNT="2"
# Optional: local scheduler on/off, default enabled
# export ENABLE_INTERNAL_SCHEDULER="1"
# Optional: enable manual poll API (default enabled)
# export MANUAL_POLL_ENABLED="1"
uvicorn app.main:app --reload
```

Open:
- [http://127.0.0.1:8000](http://127.0.0.1:8000)

## API endpoints
- `POST /api/posts`
- `POST /api/posts/bulk`
- `POST /api/poll-now` (enabled by default)
- `POST /api/cron/daily` (protected by `X-Cron-Secret`)
- `GET /api/dashboard`
- `GET /api/campaigns/{campaign_id}/dashboard`
- `GET /api/health`

## Metrics mapping from Apify output
- `views`: `videoPlayCount` (fallback: `videoViewCount`, `playCount`)
- `likes`: `likesCount` (fallback: `likes`)
- `comments`: `commentsCount` (fallback: `len(comments)` if comments array exists)

### `POST /api/posts` sample body

```json
{
  "campaign_name": "Redmi Note 15",
  "creator_handle": "@creator_a",
  "post_url": "https://www.instagram.com/reel/ABC123XYZ/"
}
```

## Go Live (Render + PostgreSQL)

1. Push this folder to GitHub:
- `/Users/kunikarathore/Downloads/insta-reel-tracker`

2. In Render:
- New Blueprint -> select repo -> it will detect `render.yaml`
- This creates:
  - Web service (`insta-reel-tracker`)
  - PostgreSQL database (`insta-reel-tracker-db`)
  - Daily cron service (`insta-reel-tracker-daily`)

3. Set required env vars in Render web service:
- `APIFY_TOKEN`
- `CRON_SECRET` (long random string)
- `ENABLE_INTERNAL_SCHEDULER=0`

4. Set required env vars in Render cron service:
- `APP_BASE_URL` (your deployed web URL, e.g. `https://insta-reel-tracker.onrender.com`)
- `CRON_SECRET` (same value as web service)

5. Deploy and verify:
- Health: `GET /api/health`
- Dashboard: `/`
- Trigger test cron manually:
  - `POST /api/cron/daily` with header `X-Cron-Secret: <CRON_SECRET>`

## Railway alternative
- Use included `Dockerfile` and `Procfile`
- Provision PostgreSQL in Railway
- Set `DATABASE_URL` to Railway Postgres URL
- Set same env vars as above
- Use Railway Cron (or external cron) to call `/api/cron/daily`

## Security notes
- Rotate secrets if ever exposed:
  - `APIFY_TOKEN`
  - `CRON_SECRET`
- Keep `ENABLE_INTERNAL_SCHEDULER=0` in production when using external cron.

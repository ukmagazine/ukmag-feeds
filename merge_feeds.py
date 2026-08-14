#!/usr/bin/env python3
"""
UK Magazine - feed merger.

Reads sources.txt, fetches every feed, drops anything already sent,
and POSTs each new item to the Make ingestion webhook using the
existing three-key contract: News_Title, News_Body, Source_Link.

State lives in state/seen.json and is committed back by the workflow.
A dry run writes no state and consumes nothing.
"""

from __future__ import annotations

import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import feedparser
import requests

# ---------------------------------------------------------------- config

SOURCES_FILE = Path("sources.txt")
STATE_FILE = Path("state/seen.json")

WEBHOOK_URL = os.environ.get("INGESTION_WEBHOOK_URL", "").strip()
ALERTS_RAW = os.environ.get("GOOGLE_ALERTS_FEEDS", "").strip()
DRY_RUN = os.environ.get("DRY_RUN", "false").strip().lower() == "true"

MAX_ITEMS_PER_RUN = 25
MAX_AGE_HOURS = 48
STATE_RETENTION_DAYS = 14

FETCH_TIMEOUT = 30
POST_TIMEOUT = 30
POST_ATTEMPTS = 3
POST_BACKOFF = 5
PAUSE_BETWEEN_POSTS = 1.0

USER_AGENT = "UKMagazineFeedBot/1.0 (+https://theukmag.com)"

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "at_medium", "at_campaign", "at_custom1", "at_custom2", "at_custom3",
    "at_custom4", "at_bbc_team", "cmp", "ito", "ns_mchannel", "ns_source",
    "ns_campaign", "ns_linkname", "ns_fee", "fbclid", "gclid", "mc_cid",
    "mc_eid", "ref", "smid",
}

# ---------------------------------------------------------------- helpers


def log(message: str) -> None:
    print(message, flush=True)


def strip_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def unwrap_google(url: str) -> str:
    """Google Alerts wraps the real article in a redirect. Unwrap it."""
    parsed = urlparse(url)
    if "google." not in parsed.netloc.lower():
        return url
    target = parse_qs(parsed.query).get("url", [None])[0]
    return target if target else url


def clean_url(url: str) -> str:
    """The URL actually sent to Make. Tracking params and fragment removed."""
    url = unwrap_google((url or "").strip())
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    kept = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
    ]
    return urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params,
         urlencode(kept), "")
    )


def dedup_key(url: str) -> str:
    """The identity of an article. Only used for comparison, never sent."""
    parsed = urlparse(clean_url(url))
    if not parsed.netloc:
        return ""
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.rstrip("/").lower() or "/"
    return f"{host}{path}?{parsed.query}" if parsed.query else f"{host}{path}"


def load_sources() -> list[str]:
    urls: list[str] = []
    if SOURCES_FILE.exists():
        for line in SOURCES_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    for line in re.split(r"[\n,]", ALERTS_RAW):
        line = line.strip()
        if line:
            urls.append(line)
    return urls


def load_state() -> tuple[dict, bool]:
    if not STATE_FILE.exists():
        return {}, True
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return data.get("seen", {}), False
    except Exception as exc:
        log(f"WARNING: state file unreadable ({exc}). Treating as first run.")
        return {}, True


def save_state(seen: dict) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=STATE_RETENTION_DAYS)
    pruned = {
        key: stamp
        for key, stamp in seen.items()
        if stamp >= cutoff.isoformat()
    }
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(
        json.dumps(
            {"updated": datetime.now(timezone.utc).isoformat(), "seen": pruned},
            ensure_ascii=False,
            indent=1,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    log(f"State saved: {len(pruned)} keys retained.")


def entry_time(entry) -> datetime:
    for field in ("published_parsed", "updated_parsed"):
        value = getattr(entry, field, None)
        if value:
            try:
                return datetime(*value[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def collect(urls: list[str]) -> list[dict]:
    horizon = datetime.now(timezone.utc) - timedelta(hours=MAX_AGE_HOURS)
    items: dict[str, dict] = {}
    for feed_url in urls:
        try:
            response = requests.get(
                feed_url,
                timeout=FETCH_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            response.raise_for_status()
            parsed = feedparser.parse(response.content)
        except Exception as exc:
            log(f"FEED FAILED  {feed_url}  ->  {exc}")
            continue

        added = 0
        for entry in parsed.entries:
            link = clean_url(getattr(entry, "link", ""))
            key = dedup_key(link)
            title = strip_html(getattr(entry, "title", ""))
            if not key or not title:
                continue
            published = entry_time(entry)
            if published < horizon:
                continue
            if key in items:
                continue
            items[key] = {
                "key": key,
                "title": title,
                "body": strip_html(
                    getattr(entry, "summary", "")
                    or getattr(entry, "description", "")
                ),
                "link": link,
                "published": published,
            }
            added += 1
        log(f"FEED OK      {feed_url}  ->  {added} usable items")

    return sorted(items.values(), key=lambda item: item["published"])


def post(item: dict) -> bool:
    payload = {
        "News_Title": item["title"],
        "News_Body": item["body"],
        "Source_Link": item["link"],
    }
    for attempt in range(1, POST_ATTEMPTS + 1):
        try:
            response = requests.post(
                WEBHOOK_URL,
                json=payload,
                timeout=POST_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            if response.ok:
                return True
            log(f"  POST attempt {attempt} returned {response.status_code}")
        except Exception as exc:
            log(f"  POST attempt {attempt} failed: {exc}")
        if attempt < POST_ATTEMPTS:
            time.sleep(POST_BACKOFF * attempt)
    return False


# ---------------------------------------------------------------- main


def main() -> int:
    if not WEBHOOK_URL and not DRY_RUN:
        log("FATAL: INGESTION_WEBHOOK_URL is not set.")
        return 1

    sources = load_sources()
    if not sources:
        log("FATAL: no feed URLs found. Check sources.txt.")
        return 1
    log(f"Loaded {len(sources)} feed(s).")

    seen, first_run = load_state()
    items = collect(sources)
    log(f"Collected {len(items)} unique item(s) inside the {MAX_AGE_HOURS}h window.")

    fresh = [item for item in items if item["key"] not in seen]
    log(f"{len(fresh)} of those are new.")

    now = datetime.now(timezone.utc).isoformat()

    if first_run and not DRY_RUN:
        for item in items:
            seen[item["key"]] = now
        save_state(seen)
        log("FIRST RUN: everything recorded as seen, nothing sent.")
        log("The next run will send genuinely new articles only.")
        return 0

    batch = fresh[:MAX_ITEMS_PER_RUN]
    if len(fresh) > len(batch):
        log(f"Capped at {MAX_ITEMS_PER_RUN}. The rest follow next run.")

    sent = 0
    failed = 0
    for item in batch:
        if DRY_RUN:
            log(f"  DRY RUN  {item['title'][:80]}")
            sent += 1
            continue
        if post(item):
            seen[item["key"]] = now
            sent += 1
            log(f"  SENT     {item['title'][:80]}")
        else:
            failed += 1
            log(f"  GIVING UP (will retry next run)  {item['link']}")
        time.sleep(PAUSE_BETWEEN_POSTS)

    if DRY_RUN:
        log("DRY RUN: state not written. Nothing was consumed.")
    else:
        save_state(seen)

    log(f"Done. sent={sent} failed={failed} dry_run={DRY_RUN}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

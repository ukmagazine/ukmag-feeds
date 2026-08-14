#!/usr/bin/env python3
"""
UK Magazine - feed merger.

Reads sources.txt, fetches every source, drops anything already sent,
and POSTs each new item to the Make ingestion webhook using the
existing three-key contract: News_Title, News_Body, Source_Link.

Two source formats are supported:
  - RSS and Atom feeds, parsed by feedparser
  - Google News XML sitemaps, parsed natively. Used for publishers
    that have dropped RSS. A sitemap carries a title and a date but
    no summary, so News_Body is empty for those items.

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
import xml.etree.ElementTree as ET
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

MAX_ITEMS_PER_RUN = 1
MAX_AGE_HOURS = 48
STATE_RETENTION_DAYS = 14

FETCH_TIMEOUT = 30
POST_TIMEOUT = 30
POST_ATTEMPTS = 3
POST_BACKOFF = 5
PAUSE_BETWEEN_POSTS = 1.0

USER_AGENT = "UKMagazineFeedBot/1.0 (+https://theukmag.com)"

SITEMAP_NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
}

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


def title_from_url(url: str) -> str:
    """Last resort when a sitemap entry carries no title."""
    slug = urlparse(url).path.rstrip("/").split("/")[-1]
    slug = re.sub(r"\.(html?|php|aspx)$", "", slug, flags=re.I)
    slug = re.sub(r"[-_]+", " ", slug).strip()
    slug = re.sub(r"\s+\d{4,}$", "", slug)
    return slug[:1].upper() + slug[1:] if slug else ""


def parse_date(raw: str) -> datetime | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


# ---------------------------------------------------------------- parsers


def looks_like_sitemap(url: str) -> bool:
    return "sitemap" in url.lower()


def entry_time(entry) -> datetime:
    for field in ("published_parsed", "updated_parsed"):
        value = getattr(entry, field, None)
        if value:
            try:
                return datetime(*value[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def read_feed(content: bytes) -> list[dict]:
    parsed = feedparser.parse(content)
    out = []
    for entry in parsed.entries:
        out.append({
            "link": getattr(entry, "link", ""),
            "title": strip_html(getattr(entry, "title", "")),
            "body": strip_html(
                getattr(entry, "summary", "")
                or getattr(entry, "description", "")
            ),
            "published": entry_time(entry),
        })
    return out


def read_sitemap(content: bytes) -> list[dict]:
    root = ET.fromstring(content)
    out = []
    for url_el in root.findall("sm:url", SITEMAP_NS):
        loc = (url_el.findtext("sm:loc", "", SITEMAP_NS) or "").strip()
        if not loc:
            continue
        title = ""
        published = None
        news_el = url_el.find("news:news", SITEMAP_NS)
        if news_el is not None:
            title = strip_html(
                news_el.findtext("news:title", "", SITEMAP_NS) or ""
            )
            published = parse_date(
                news_el.findtext("news:publication_date", "", SITEMAP_NS) or ""
            )
        if published is None:
            published = parse_date(
                url_el.findtext("sm:lastmod", "", SITEMAP_NS) or ""
            )
        if not title:
            title = title_from_url(loc)
        out.append({
            "link": loc,
            "title": title,
            "body": "",
            "published": published or datetime.now(timezone.utc),
        })
    return out


def collect(urls: list[str]) -> list[dict]:
    horizon = datetime.now(timezone.utc) - timedelta(hours=MAX_AGE_HOURS)
    items: dict[str, dict] = {}

    for source_url in urls:
        kind = "SITEMAP" if looks_like_sitemap(source_url) else "FEED"
        try:
            response = requests.get(
                source_url,
                timeout=FETCH_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )
            response.raise_for_status()
            raw = (
                read_sitemap(response.content)
                if kind == "SITEMAP"
                else read_feed(response.content)
            )
        except Exception as exc:
            log(f"{kind} FAILED  {source_url}  ->  {exc}")
            continue

        added = 0
        for raw_item in raw:
            link = clean_url(raw_item["link"])
            key = dedup_key(link)
            if not key or not raw_item["title"]:
                continue
            if raw_item["published"] < horizon:
                continue
            if key in items:
                continue
            items[key] = {
                "key": key,
                "title": raw_item["title"],
                "body": raw_item["body"],
                "link": link,
                "published": raw_item["published"],
            }
            added += 1
        log(f"{kind} OK      {source_url}  ->  {added} usable items")

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
        log("FATAL: no sources found. Check sources.txt.")
        return 1
    log(f"Loaded {len(sources)} source(s).")

    seen, first_run = load_state()
    items = collect(sources)
    log(f"Collected {len(items)} unique item(s) inside the {MAX_AGE_HOURS}h window.")

    no_body = sum(1 for item in items if not item["body"])
    if no_body:
        log(f"NOTE: {no_body} item(s) carry no summary text (sitemap sources).")

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

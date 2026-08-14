# ukmag-feeds

Feed ingestion layer for UK Magazine.

## What this does

Fetches every RSS feed listed in `sources.txt` once an hour, drops
anything already sent, and POSTs each new article to the Make.com
ingestion webhook using the existing three-key contract:

- `News_Title`
- `News_Body`
- `Source_Link`

This replaces the Make scenarios `1.0 RSS Harvester` and
`1.1 Google Alerts Harvester`. Both are retired and switched off.
Nothing else in the pipeline changed.

## Files

| File | Purpose |
|---|---|
| `sources.txt` | The feed list. One URL per line. This is the only file to edit when adding a source. |
| `merge_feeds.py` | The merger. Fetch, clean, deduplicate, POST. |
| `.github/workflows/merge-feeds.yml` | Runs the merger hourly at minute 20. |
| `state/seen.json` | Written by the script. Never edit by hand. |

## Adding a news source

Add the feed URL as a new line in `sources.txt` and commit. Nothing else.

Google Alerts feeds are **not** listed here — the feed URL reveals what
the publication monitors and this repository is public. They go in the
`GOOGLE_ALERTS_FEEDS` secret instead, one per line.

## Deduplication — where it happens

This repository removes **certain** duplicates only: the same article
URL seen before, or the same URL appearing twice in one fetch.

**Semantic deduplication stays in Make.com** — modules 6, 13, 14 and 15
of `2.UK Mag - CORE ENGINE`. An RSS feed carries only a headline and a
short summary, not the article body, so this layer cannot judge whether
two differently-worded headlines describe the same event. It does not
try, and it must never be extended to try by matching headline text.

## What this layer must never do

**No editorial filtering. No keyword blocking. No content judgement.**

Editorial red lines are enforced by the triage prompt in Core Engine
module 3, and nowhere else. Volume is controlled by choosing narrower
section feeds, never by dropping items on keywords.

## Secrets

| Secret | Required | Purpose |
|---|---|---|
| `INGESTION_WEBHOOK_URL` | yes | The Make.com ingestion webhook. Bearer-equivalent — anyone holding it can inject articles into the pipeline. |
| `GOOGLE_ALERTS_FEEDS` | no | Google Alerts feed URLs, one per line. |

## Safety behaviour

- **The first run sends nothing.** It records everything currently in
  the feeds as already seen, then exits. Real sending starts on run two.
- **Maximum 25 articles per run.** The remainder follow on the next run.
- **A failed POST is not marked as seen** and is retried next run.
- **A broken feed does not stop the others.**

## Manual run

Actions tab -> Merge feeds -> Run workflow. Tick `dry_run` to log the
articles without sending anything to Make.

## Reversibility

Nothing here is destructive. To go back to the old arrangement, switch
`1.0 RSS Harvester` back on in Make and disable this workflow. The
webhook and the three-key contract are unchanged.

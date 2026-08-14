# ukmag-feeds

Feed ingestion layer for UK Magazine.

## What this does

Every hour, a GitHub Actions workflow runs `merge_feeds.py`. The script
reads `sources.txt`, fetches every source listed there, discards anything
it has sent before, and POSTs each new article to the Make.com ingestion
webhook using the existing three-key contract:

- `News_Title`
- `News_Body`
- `Source_Link`

This replaces the Make scenarios `1.0 RSS Harvester` and
`1.1 Google Alerts Harvester`. Both are retired and switched off.
**Nothing else in the pipeline changed** — the webhook, the data
structure, and every Core Engine module are exactly as they were.

## Files

| File | Purpose |
|---|---|
| `sources.txt` | The source list. The only file to edit when adding or removing a feed. |
| `merge_feeds.py` | Fetch, clean, deduplicate, POST. |
| `.github/workflows/merge-feeds.yml` | Runs the merger hourly at minute 20. |
| `state/seen.json` | Written by the script. Never edit by hand. |

Minute 20, not minute 0: GitHub's scheduler queue is busiest on the hour,
and an hourly job asked for at :00 is often delayed 10-45 minutes.

## Supported source formats

**RSS and Atom feeds.** Title, summary and link, parsed by feedparser.

**Google News XML sitemaps.** For publishers that have dropped RSS
entirely. The script switches to sitemap parsing when the URL contains
the word `sitemap`. A sitemap carries a title and a publication date but
**no summary**, so `News_Body` arrives empty for those items and the full
text comes from Jina Reader downstream. Time Out is ingested this way.

---

# HOW TO ADD A SOURCE

## Step 0 — Whitelist the domain FIRST

Before anything else, add the article domain to the `Whitelist E-Gate`
filter in Make: scenario `2.UK Mag - CORE ENGINE`, on the link from
module `20` to module `36`.

**This step is not optional and its failure mode is invisible.** An
article from a domain that is not whitelisted is fetched here, POSTed to
Make, accepted by the webhook, and then dropped at the very first link.
No Airtable record. No log entry. No error anywhere. The feed will look
perfectly healthy in the GitHub logs while delivering nothing.

Note the domain to whitelist is the domain of the **article**, not of the
feed. Sky News articles live on `news.sky.com` even though the feed is
served from `feeds.skynews.com`.

## Step 1 — Add the line

Edit `sources.txt`. Add the feed URL under the relevant category heading,
with no `#` in front of it. One URL per line.

## Step 2 — Dry run

1. Actions tab -> **Merge feeds** -> **Enable workflow**
2. **Run workflow** -> tick **Log only, send nothing** -> **Run workflow**
3. Open the log, expand **Run the merger**
4. **Disable workflow** again immediately

A dry run sends nothing to Make and writes no state. It is free and
cannot affect the pipeline. There is no reason ever to skip it.

## Step 3 — Read the log

Find the line for the source you added.

| Log line | Meaning | What to do |
|---|---|---|
| `FEED OK ... 12 usable items` | Healthy | Keep it |
| `SITEMAP OK ... 9 usable items` | Healthy sitemap | Keep it |
| `FEED FAILED ... 403 / 404 / 500` | Wrong URL, or the server refuses bots | Comment it out and write the reason beside it |
| `FEED OK ... 0 usable items` | **The dangerous one** | See below |

**`FEED OK` with zero items is not an error.** The feed opened and parsed
correctly; it simply had nothing inside the 48-hour window. That may mean
a quiet news day, or it may mean the URL is subtly wrong and will never
return anything. The two look identical. Re-test after two days. If it is
still zero, comment it out.

This is exactly how Bank of England, Eater London and The London Standard
were caught on 14 August 2026 — all three reported `FEED OK` and
delivered nothing.

## Step 4 — Watch the volume

The log prints `Collected N unique item(s) inside the 48h window`. Halve
that for the daily rate.

Measured cost, 14 August 2026: **one ingested article consumes 9 Make
operations** in the Core Engine before it ever reaches the Writer. So one
extra article per day is roughly 270 operations per month. A feed
producing 20 items a day costs about 5,400 operations a month on its own.

Budget arithmetic before adding anything: `items per day x 9 x 30`.

---

# HOW TO REMOVE A SOURCE

Put a `#` at the start of its line. **Do not delete the line.** Add a
short note saying why it was removed and on what date.

A deleted line gets rediscovered and re-tried six months later by
somebody who has no idea it was already tested and rejected. A commented
line with a reason cannot.

---

# TWO RULES THAT MUST NOT BE BROKEN

## 1. This layer makes no editorial judgement

**No keyword filters. No content blocking. No dropping items by subject.**

Editorial red lines are enforced by the triage prompt in Core Engine
module `3`, and nowhere else. That module reads the full article text and
judges it. A keyword list here would see only a headline, and would fail
in both directions.

A real example from this repository's own logs: *"Ann Widdecombe's loved
ones gather for her funeral"* is a death story, and no keyword list would
catch it.

**What this layer may do:** deduplicate by URL, strip HTML, normalise
encoding, unwrap redirect wrappers, drop items older than 48 hours.

**How volume is controlled instead:** by choosing narrow section feeds
over whole-site feeds, and one or two sources per category. Nothing else.

## 2. Always tick the dry-run box

Without it, a "test" sends 25 real articles into the live pipeline.

---

# DEDUPLICATION — WHERE IT HAPPENS

**Here:** exact duplicates only. The same article URL seen before, or the
same URL appearing twice in one fetch.

**In Make:** everything else. Modules `6`, `13`, `14` and `15` of
`2.UK Mag - CORE ENGINE` run an LLM that reads the text and decides
whether two differently-worded stories describe the same event.

An RSS feed carries a headline and a short summary, not the article body.
This layer therefore cannot tell that "PM ousted" and "Britain's leader
steps down" are one story. It does not try, and **it must never be
extended to try by matching headline text** — fuzzy title matching
silently discards genuinely distinct stories and leaves no trace.

Measured on 14 August 2026: across a 232-item sample, URL deduplication
removed **zero** cross-publisher duplicates. Every publisher uses its own
URLs. Semantic deduplication is not a nice-to-have here; it is the only
thing doing that job.

---

# SAFETY BEHAVIOUR

- **The first real run sends nothing.** With no state file, everything
  currently in the feeds is recorded as seen and the script exits. Real
  sending begins on run two.
- **Maximum 25 articles per run**, one second apart. The remainder follow
  next hour.
- **A failed POST is not marked as seen** and is retried on the next run.
  Three attempts with backoff before giving up.
- **A broken source does not stop the others.** It logs and continues.
- **A dry run writes no state and consumes nothing.**

---

# SECRETS

| Secret | Required | Purpose |
|---|---|---|
| `INGESTION_WEBHOOK_URL` | yes | The Make.com ingestion webhook. Bearer-equivalent: anyone holding it can inject articles into the pipeline. |
| `GOOGLE_ALERTS_FEEDS` | no | Google Alerts feed URLs, one per line. Not yet created. |

Google Alerts feed URLs are kept in a secret rather than in
`sources.txt`, because this repository is public and the feed URL reveals
what the publication monitors.

---

# MANUAL RUN

Actions tab -> **Merge feeds** -> **Run workflow**. Tick `dry_run` to log
the articles without sending anything.

---

# REVERSIBILITY

Nothing here is destructive. To return to the previous arrangement,
switch `1.0 RSS Harvester` back on in Make and disable this workflow. The
scenarios were retired, not deleted. The webhook and the three-key
contract are unchanged.

---

# KNOWN LIMITATIONS

- GitHub disables scheduled workflows in repositories with no commit
  activity for 60 days. The script commits `state/seen.json` on every
  real run, which keeps the repository active. **A long period of dry
  runs only does not count.**
- GitHub's cron is best-effort. An hourly job can be 10-45 minutes late.
- Sitemap-sourced items carry no summary. If the Jina fetch downstream
  fails, the article reaches triage with an empty body — and Core Engine
  module `36` runs with `stopOnHttpError: false`, so that failure is
  silent. Tracked as bug `H9`.
- If Make's webhook queue ever rejects a payload after returning `200`,
  the article is marked as sent here and is lost. Not observed; recorded
  because it would be silent.

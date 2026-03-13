# Career Link Finder Pipeline

This project crawls career pages, extracts candidate job links, and then processes those job links for HTML/LD-JSON in a separate stage.

The pipeline is built for high volume batch runs and multi-server workers.

## What It Does

1. Seed career page URLs from file or Mongo.
2. Crawl each career page and extract links.
3. Filter links and identify likely job links.
4. Persist discovered job links durably in Mongo.
5. Process job links later (recommended) or immediately (optional) to extract job page data.

## High-Level Architecture

- Redis Stream `career:pages`: career page crawl work.
- Redis Stream `career:pages:failed`: parked links-worker failures for replay.
- Redis Stream `career:links`: optional Redis-based HTML work queue.
- Redis Set `career:links:dedupe`: optional queue dedupe set.
- Mongo `career_links`: aggregated links per career URL.
- Mongo `career_link_jobs`: durable per-job-link records and HTML lifecycle state.
- Mongo `career_html`: job page extraction output.

## Recommended Flow (Best For Large Batches)

1. Seed career pages.
2. Run only `worker:links` (many servers).
3. Let links accumulate in `career_link_jobs`.
4. Run `worker:html-mongo` later (many servers).

This avoids data loss from temporary queue state and keeps Mongo as source of truth.

## Flowchart

```mermaid
flowchart TD
    A[Seed Career URLs\nseed] --> B[Redis Stream\ncareer:pages]
    B --> C[Links Workers\nworker:links]
    C --> D[Fetch career page\nGot -> Puppeteer fallback]
    D --> E[Extract + filter links]
    E --> F[Upsert career_links\nper careerUrl]
    E --> G[Upsert career_link_jobs\nper job url]
    G --> H[HTML Mongo Workers\nworker:html-mongo]
    H --> I[Atomic claim + lock\nhtmlStatus pending/processing]
    I --> J[Fetch job page html\nGot -> Puppeteer fallback]
    J --> K[Extract LD-JSON]
    K --> L[Upsert career_html]
    K --> M[Update career_link_jobs\nhtmlStatus done/error/skipped]
```

Additional diagrams are in `docs/PIPELINE_FLOW.md`.

## Commands

- Seed from Mongo:
```bash
npm run seed -- --collection <collection_name> --field careerUrl --batch-size 1000
```

- Resume career-page seeding after Redis loss (skip already completed pages):
```bash
node pipeline/index.js seed --collection <collection_name> --field careerUrl --batch-size 1000 --include-seeded --resume-from-results
```

- Seed from file:
```bash
npm run seed -- --file ./urls.txt
```

- Run links workers:
```bash
npm run worker:links
```

- Run HTML workers from Mongo (recommended):
```bash
npm run worker:html:mongo
```

- Run HTML workers from Redis queue:
```bash
npm run worker:html
```

- Check queue + retry health snapshot:
```bash
npm run health
```

- Health snapshot as JSON:
```bash
node pipeline/index.js health --json
```

- Queue Mongo job links into Redis HTML stream (optional path):
```bash
npm run seed:job-links -- --batch-size 2000
```

- Resume HTML queue after Redis loss (reseed pending/incomplete docs):
```bash
node pipeline/index.js seed:job-links --batch-size 2000 --include-queued
```

- Replay failed links-worker jobs from Redis failed stream:
```bash
node pipeline/index.js seed:failed-links --batch-size 2000
```

- Force reseed of pending HTML queue flags:
```bash
npm run seed:job-links -- --reset-queued --batch-size 2000
```

- Run resumable queue seeders in PM2 (auto-recover when stream is empty):
```bash
pm2 start pipeline/index.js --name seed-career-pages -- seed --collection <collection_name> --field careerUrl --batch-size 1000 --watch --poll-ms 30000 --recover-when-empty --resume-from-results
pm2 start pipeline/index.js --name seed-job-links -- seed:job-links --batch-size 2000 --watch --poll-ms 15000 --recover-when-empty --stale-queued-ms 14400000
```

## Quick Start (CLI Style You Are Using)

```bash
cd /home/careerlinkfinderNew/
git pull
npm i
export MONGODB_DATABSAE=careefinder_prod
export IMPORT_COLLECTION=carrerlinks_p1batch
node pipeline/index.js seed
pm2 start pipeline/index.js -i 8 --name worker-links -- worker:links
```

Then run HTML later:

```bash
pm2 start pipeline/index.js -i 8 --name worker-html-mongo -- worker:html-mongo
```

Note: `MONGODB_DATABSAE` spelling is kept as-is to match current config mapping.

## PM2 Examples

- Links workers:
```bash
pm2 start pipeline/index.js -i 8 --name worker-links -- worker:links
```

- HTML workers (Mongo mode, recommended):
```bash
pm2 start pipeline/index.js -i 8 --name worker-html-mongo -- worker:html-mongo
```

- HTML workers (Redis mode):
```bash
pm2 start pipeline/index.js -i 8 --name worker-html -- worker:html
```

## Data Safety Behavior

- Link data is preserved on link-fetch errors by default (`PRESERVE_LINKS_ON_ERROR=true`).
- Final links-worker failures are parked in Redis (`career:pages:failed`) for replay.
- Link arrays can merge across multiple runs (`MERGE_LINKS_ACROSS_RUNS=true`).
- URL variants are canonicalized for dedupe (for example `http`/`https`, default ports, trailing slash), with `https` preferred when both exist.
- The crawled career URL (and its `http`/`https` variants) is removed from stored `links` to avoid self-link duplicates.
- ATS links are canonicalized to main board URLs (for example Lever/Workday/Oracle/ADP), so `atsCareerLinks` does not store job-detail/filter-query variants.
- Seed commands support recovery mode to reseed pending work when Redis stream data is lost (`--recover-when-empty`, `--include-seeded`, `--resume-from-results`).
- Requests rotate across a larger desktop/mobile User-Agent pool to reduce provider-specific blocking.
- Domain-level exclusions are applied from `expireExcludeDomains.json`.
- Expired/no-jobs page detection is applied from `expireKeywords.json`.
- Discovered job links are durably upserted in `career_link_jobs`.
- HTML processing state is tracked in Mongo: `pending`, `processing`, `done`, `skipped`, `error`.
- Mongo HTML worker uses lock fields (`htmlLockBy`, `htmlLockUntil`) to coordinate multiple servers safely.

## Important Runtime Settings

Set these via environment variables (directly read by `pipeline/index.js`):

- `ENQUEUE_HTML_FROM_LINKS_WORKER`: `false` recommended for delayed HTML batch.
- `PRESERVE_LINKS_ON_ERROR`: keep existing links when a crawl fails.
- `MERGE_LINKS_ACROSS_RUNS`: union old + new discovered links.
- `KEEP_EXTERNAL_LIKELY_JOB_LINKS`: keep external links when they match job-link patterns (useful for iframe ATS boards).
- `LINKS_FETCH_MODE`: `auto` (default), `puppeteer`, or `got` for `worker:links`.
- `MIN_LIKELY_JOB_LINKS_FOR_GOT`: in `auto` mode, minimum likely job links required to accept Got result before Puppeteer fallback.
- `EXPAND_EXTERNAL_JOB_BOARD_LINKS`: when enabled, expand external career-home links (for example Paycor/Workday boards) to collect job-detail links.
- `JOB_BOARD_EXPANSION_MAX_SEEDS`: maximum external job-board seed links expanded per career page crawl.
- `FILTER_NON_JOB_LINKS_BY_ANCHOR_TEXT`: if `true` (default), drops links with non-job anchor text (for example Contact/FAQ/About/Help) while preserving ATS and likely-job links.
- `FILTER_CAREER_LINKS`: if `true` (default), filters stored `links` to career/job-like URLs and removes obvious non-career URLs.
- `FILTER_ATS_LINKS`: if `true` (default), filters `atsCareerLinks` to ATS career/job URLs and drops ATS non-job pages (for example privacy/help/about pages).
- `NON_JOB_ANCHOR_TEXT_PATTERNS`: custom non-job text phrases used by the anchor-text filter.
- `POSITIVE_JOB_ANCHOR_TEXT_PATTERNS`: text patterns that force links to be kept as job-relevant.
- `ENABLE_EXPIRE_KEYWORD_DETECTION`: detect expired/no-jobs pages using `expireKeywords.json`.
- `EXPIRE_KEYWORD_MATCH_LIMIT`: max keyword matches saved per page.
- `EXPIRE_DETECTION_TEXT_LIMIT`: max page text characters scanned for expire/no-jobs detection.
- `JOB_LINK_STRONG_PATTERNS`: include provider-specific markers (for example AppOne `jobcode=`) when needed.
- `JOB_LINK_EXCLUDE_PATTERNS`: use this to suppress non-job actions (for example `emailme.asp`, `jobcode=0`).
- `REDIS_ENQUEUE_BATCH_SIZE`: Redis enqueue batch size.
- `MONGO_BULK_WRITE_BATCH_SIZE`: Mongo bulk upsert batch size.
- `HTML_MONGO_LOCK_MS`: lock duration for Mongo HTML claim.
- `HTML_MONGO_POLL_MS`: idle poll delay in Mongo HTML worker.
- `HTML_MONGO_RETRY_ERRORS`: allow retry of error-state docs.
- `HTML_MONGO_RETRY_DELAY_MS`: delay before retrying error docs.
- `LINKS_WORKER_CONCURRENCY`: per-process message concurrency for links workers.
- `HTML_WORKER_CONCURRENCY`: per-process message concurrency for Redis HTML workers.
- `SKIP_NON_JOB_LINKS_IN_HTML_WORKERS`: `false` by default; set `true` only if you want pre-filter skip behavior in HTML workers.
- `PUPPETEER_ON_SHORT_HTML`: if `false`, accept non-empty Got HTML without Puppeteer fallback (lower load, faster).
- `STREAM_DELETE_ACKED_MESSAGES`: if `true` (default), remove processed stream entries from Redis after `XACK`.
- `STREAM_RETRY_ON_ERROR`: if `true` (default), requeue transient stream failures.
- `STREAM_MAX_RETRIES`: max Redis stream retries before final error persistence (default `3`).
- `STREAM_NON_RETRYABLE_EXTENSIONS`: comma-separated file extensions that should not be retried (for example `.zip,.pdf,.jpg`).
- `HEALTH_STATS_KEY`: Redis hash key used to store retry counters for health checks.
- `CAREER_PAGES_FAILED_QUEUE`: Redis stream key used for parked links-worker failures.
- `seed --include-seeded --resume-from-results`: reseed from source collection while skipping URLs already marked `crawlStatus=success` in `career_links`.
- `seed --watch --recover-when-empty`: keep a PM2 seeder running and auto-recover when `career:pages` stream is empty.
- `seed:job-links --watch --recover-when-empty`: keep HTML queue seeder running and auto-recover when `career:links` stream is empty.
- `seed:job-links --stale-queued-ms <n>`: requeue docs with old `htmlQueuedAt` even if `htmlQueued=true` (default `4h`).

## Mongo Collections and Meaning

- `career_links`: one document per `careerUrl`, stores discovered links and job links.
- `career_link_jobs`: one document per job `url`, tracks discovery metadata and HTML processing lifecycle.
- `career_html`: one document per job `url`, stores parsed output for job pages.

`career_links` status fields:
- `jobLinksStatus`: `job_links_found` | `no_job_links` | `error`
- `careerLinksStatus`: `job_links_found` | `ats_career_links_found` | `expired_or_no_jobs` | `excluded_domain` | `no_job_links` | `error`
- `jobDetailLinkCount`: count of detail-like job URLs (for example `/job/123`, `jobId=...`)
- `atsCareerLinks`: detected ATS career-board links (Workday, Oracle CE, ADP, Entertime, etc.)
- `finalCareerUrl`: preferred downstream career page URL; uses canonical ATS board URL when available, otherwise the best discovered career-like link, otherwise `finalUrl`.
- `atsTemplate`: detected ATS vendor/template for `finalCareerUrl` (for example `workday`, `lever`, `greenhouse`); empty when the selected career URL is not a recognized ATS board.
- `careerFilteredLinkCount`: number of URLs dropped by career-link URL filtering.
- `atsFilteredLinkCount`: number of URLs dropped by ATS-link filtering.
- `excludedDomainPattern`: matched pattern from `expireExcludeDomains.json` when domain exclusion is triggered.
- `expireKeywordMatches`: matched phrases from `expireKeywords.json` when page appears expired/no-jobs.
- `requestUrl`: original URL that entered the worker queue.
- `careerUrl`: canonical URL key for the page (scheme/trailing-slash normalized; prefers `https` when possible).
- `careerUrlKey`: scheme-insensitive dedupe key used to avoid duplicate docs for redirected variants.
- `httpStatusCode`: final page HTTP status (for example `200`, `404`).
- `redirectStatusCodes`: redirect/status chain (for example `[301,200]`).
- `finalUrl`: resolved destination URL after redirects.
- `crawlStatus`: `success` | `error`

Quick query for pages with no job links:
```js
db.career_links.find({ jobLinksStatus: "no_job_links", crawlStatus: "success" })
```

Quick query for pages where ATS board was found but no direct job detail links yet:
```js
db.career_links.find({ careerLinksStatus: "ats_career_links_found", crawlStatus: "success" })
```

Quick query for pages excluded by domain rules:
```js
db.career_links.find({ careerLinksStatus: "excluded_domain", crawlStatus: "success" })
```

Quick query for pages classified as expired/no-jobs:
```js
db.career_links.find({ careerLinksStatus: "expired_or_no_jobs", crawlStatus: "success" })
```

## Optional Redis HTML Mode Notes

Redis HTML queue mode is supported, but for very large delayed runs Mongo HTML mode is safer because job state is durable in Mongo.

If you use `seed:job-links`, default is `--use-dedupe false` so old Redis dedupe keys do not accidentally suppress queueing.

## Throughput Notes For 2M+ Batch

1. Use multiple workers on multiple servers for `worker:links`.
2. Keep `ENQUEUE_HTML_FROM_LINKS_WORKER=false` during link collection.
3. Run `worker:html-mongo` as a separate stage.
4. Tune `REDIS_ENQUEUE_BATCH_SIZE` and `MONGO_BULK_WRITE_BATCH_SIZE`.
5. Keep Mongo indexes healthy (`career_link_jobs.url`, `career_html.url`, `career_links.careerUrl`).
6. For Redis HTML workers, start with `HTML_WORKER_CONCURRENCY=1` and increase gradually only if CPU/RAM allow.

## Troubleshooting

- No records seeded: check `--collection`, `--field`, and mark fields (`redisSeeded`, `redisSeededAt`).
- HTML workers idle in mongo mode: check `career_link_jobs.htmlStatus` values and lock timestamps.
- Redis HTML workers not receiving jobs: ensure `seed:job-links` has run (or set `ENQUEUE_HTML_FROM_LINKS_WORKER=true`).
- Redis stream data flushed/deleted: rerun `seed --include-seeded --resume-from-results` and `seed:job-links --include-queued` to rebuild pending queues from Mongo state.
- For automatic recovery in PM2, run seeders in watch mode with `--recover-when-empty`.

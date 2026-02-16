const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const config = require("config");

const { fetchHtmlWithGot } = require("./http_client");
const { extractLinksFromHtml } = require("./link_extractor");
const {
  fetchLinksWithPuppeteer,
  fetchHtmlWithPuppeteer,
  closeBrowser
} = require("./puppeteer_client");
const {
  createRedisClient,
  ensureStreamGroup,
  streamAdd,
  streamAddBatch,
  streamReadGroupBatch,
  streamAutoClaimBatch,
  streamAck,
  saddAndStreamBatch
} = require("./redis_queue");
const { connectMongo } = require("./mongo_client");
const { extractJobPostingFromHtml } = require("./ldjson_extractor");
const parseLdJson = require("../ldjson_parser");
const { deleteSocialMediaUrls } = require("../delete_socialmedia_links");
const { filterJobLinks } = require("./job_link_filter");

const getConfig = (key, fallback) => {
  try {
    return config.get(key);
  } catch (error) {
    return fallback;
  }
};

const mongoConfig = getConfig("mongodb", {
  uri: "mongodb://mohan:CypriaCuteyZimmyCarts@104.251.217.22:27017/admin",
  database: "careerlinkfinder_prod"
});
const redisUrl = getConfig("redis_uri", "redis://appuser:strongpassword@104.251.217.22:6379/1");
const pipelineConfig = getConfig("pipeline", {});

const queues = {
  careerPages:
    process.env.CAREER_PAGES_QUEUE ||
    (pipelineConfig.queues && pipelineConfig.queues.career_pages) ||
    "career:pages",
  careerLinks:
    process.env.CAREER_LINKS_QUEUE ||
    (pipelineConfig.queues && pipelineConfig.queues.career_links) ||
    "career:links",
  careerLinksDedup:
    process.env.CAREER_LINKS_DEDUP_QUEUE ||
    (pipelineConfig.queues && pipelineConfig.queues.career_links_dedupe) ||
    "career:links:dedupe"
};

const collections = {
  careerPages:
    process.env.CAREER_PAGES_COLLECTION ||
    (pipelineConfig.collections && pipelineConfig.collections.career_pages) ||
    "newdata",
  careerLinks:
    process.env.CAREER_LINKS_COLLECTION ||
    (pipelineConfig.collections && pipelineConfig.collections.career_links) ||
    "career_links",
  jobHtml:
    process.env.JOB_HTML_COLLECTION ||
    (pipelineConfig.collections && pipelineConfig.collections.job_html) ||
    "career_html",
  jobLinks:
    process.env.JOB_LINKS_COLLECTION ||
    (pipelineConfig.collections && pipelineConfig.collections.job_links) ||
    "career_link_jobs"
};

const parseBoolean = (value, fallback) => {
  if (value === undefined || value === null) {
    return fallback;
  }
  return ["1", "true", "yes", "y"].includes(String(value).toLowerCase());
};

const settings = {
  sameDomainOnly: parseBoolean(
    process.env.SAME_DOMAIN_ONLY,
    pipelineConfig.same_domain_only !== undefined
      ? pipelineConfig.same_domain_only
      : true
  ),
  minLinksForGot: Number(
    process.env.MIN_GOT_LINKS ||
    pipelineConfig.min_links_for_got ||
    5
  ),
  minHtmlLength: Number(
    process.env.MIN_HTML_LENGTH ||
    pipelineConfig.min_html_length ||
    1000
  ),
  gotTimeoutMs: Number(
    process.env.GOT_TIMEOUT_MS || pipelineConfig.got_timeout_ms || 20000
  ),
  puppeteerTimeoutMs: Number(
    process.env.PUPPETEER_TIMEOUT_MS ||
    pipelineConfig.puppeteer_timeout_ms ||
    60000
  ),
  pollTimeoutSeconds: Number(
    process.env.POLL_TIMEOUT_SECONDS ||
    pipelineConfig.poll_timeout_seconds ||
    5
  ),
  jobLinkStrongPatterns:
    pipelineConfig.job_link_strong_patterns ||
    (process.env.JOB_LINK_STRONG_PATTERNS
      ? process.env.JOB_LINK_STRONG_PATTERNS.split("|")
      : undefined),
  jobLinkWeakPatterns:
    pipelineConfig.job_link_weak_patterns ||
    (process.env.JOB_LINK_WEAK_PATTERNS
      ? process.env.JOB_LINK_WEAK_PATTERNS.split("|")
      : undefined),
  jobLinkExcludePatterns:
    pipelineConfig.job_link_exclude_patterns ||
    (process.env.JOB_LINK_EXCLUDE_PATTERNS
      ? process.env.JOB_LINK_EXCLUDE_PATTERNS.split("|")
      : undefined),
  enqueueHtmlFromLinksWorker: parseBoolean(
    process.env.ENQUEUE_HTML_FROM_LINKS_WORKER,
    pipelineConfig.enqueue_html_from_links_worker !== undefined
      ? pipelineConfig.enqueue_html_from_links_worker
      : false
  ),
  preserveLinksOnError: parseBoolean(
    process.env.PRESERVE_LINKS_ON_ERROR,
    pipelineConfig.preserve_links_on_error !== undefined
      ? pipelineConfig.preserve_links_on_error
      : true
  ),
  mergeLinksAcrossRuns: parseBoolean(
    process.env.MERGE_LINKS_ACROSS_RUNS,
    pipelineConfig.merge_links_across_runs !== undefined
      ? pipelineConfig.merge_links_across_runs
      : true
  ),
  redisEnqueueBatchSize: Number(
    process.env.REDIS_ENQUEUE_BATCH_SIZE ||
      pipelineConfig.redis_enqueue_batch_size ||
      500
  ),
  mongoBulkWriteBatchSize: Number(
    process.env.MONGO_BULK_WRITE_BATCH_SIZE ||
      pipelineConfig.mongo_bulk_write_batch_size ||
      1000
  ),
  htmlMongoLockMs: Number(
    process.env.HTML_MONGO_LOCK_MS ||
      pipelineConfig.html_mongo_lock_ms ||
      300000
  ),
  htmlMongoPollMs: Number(
    process.env.HTML_MONGO_POLL_MS ||
      pipelineConfig.html_mongo_poll_ms ||
      2000
  ),
  htmlMongoRetryErrors: parseBoolean(
    process.env.HTML_MONGO_RETRY_ERRORS,
    pipelineConfig.html_mongo_retry_errors !== undefined
      ? pipelineConfig.html_mongo_retry_errors
      : false
  ),
  htmlMongoRetryDelayMs: Number(
    process.env.HTML_MONGO_RETRY_DELAY_MS ||
      pipelineConfig.html_mongo_retry_delay_ms ||
      300000
  ),
  linksWorkerConcurrency: Number(
    process.env.LINKS_WORKER_CONCURRENCY ||
      pipelineConfig.links_worker_concurrency ||
      1
  ),
  htmlWorkerConcurrency: Number(
    process.env.HTML_WORKER_CONCURRENCY ||
      pipelineConfig.html_worker_concurrency ||
      1
  ),
  skipNonJobLinksInHtmlWorkers: parseBoolean(
    process.env.SKIP_NON_JOB_LINKS_IN_HTML_WORKERS,
    pipelineConfig.skip_non_job_links_in_html_workers !== undefined
      ? pipelineConfig.skip_non_job_links_in_html_workers
      : false
  ),
  healthStatsKey:
    process.env.HEALTH_STATS_KEY ||
    (pipelineConfig.health && pipelineConfig.health.stats_key) ||
    "career:pipeline:health",
  fallBackToPuppeteerOnShortHtml: parseBoolean(
    process.env.PUPPETEER_ON_SHORT_HTML,
    pipelineConfig.puppeteer_on_short_html !== undefined
      ? pipelineConfig.puppeteer_on_short_html
      : false
  )
};

const streamConfig = pipelineConfig.streams || {};
const streamGroups = {
  careerPages:
    process.env.CAREER_PAGES_GROUP ||
    (streamConfig.groups && streamConfig.groups.career_pages) ||
    "career-pages",
  careerLinks:
    process.env.CAREER_LINKS_GROUP ||
    (streamConfig.groups && streamConfig.groups.career_links) ||
    "career-links"
};
const streamSettings = {
  blockMs: Number(
    process.env.STREAM_BLOCK_MS ||
      streamConfig.block_ms ||
      settings.pollTimeoutSeconds * 1000
  ),
  readCount: Number(
    process.env.STREAM_READ_COUNT || streamConfig.read_count || 1
  ),
  claimMinIdleMs: Number(
    process.env.STREAM_CLAIM_MIN_IDLE_MS ||
      streamConfig.claim_min_idle_ms ||
      60000
  ),
  claimCount: Number(
    process.env.STREAM_CLAIM_COUNT || streamConfig.claim_count || 1
  ),
  claimIntervalMs: Number(
    process.env.STREAM_CLAIM_INTERVAL_MS ||
      streamConfig.claim_interval_ms ||
      30000
  ),
  deleteAckedMessages: parseBoolean(
    process.env.STREAM_DELETE_ACKED_MESSAGES,
    streamConfig.delete_acked_messages !== undefined
      ? streamConfig.delete_acked_messages
      : true
  ),
  retryOnError: parseBoolean(
    process.env.STREAM_RETRY_ON_ERROR,
    streamConfig.retry_on_error !== undefined
      ? streamConfig.retry_on_error
      : true
  ),
  maxRetries: Number.isFinite(Number(process.env.STREAM_MAX_RETRIES))
    ? Math.max(0, Math.floor(Number(process.env.STREAM_MAX_RETRIES)))
    : Number.isFinite(Number(streamConfig.max_retries))
      ? Math.max(0, Math.floor(Number(streamConfig.max_retries)))
      : 3
};
const streamRetryNonRetryableExtensions = new Set(
  (
    process.env.STREAM_NON_RETRYABLE_EXTENSIONS
      ? process.env.STREAM_NON_RETRYABLE_EXTENSIONS.split(",")
      : streamConfig.non_retryable_extensions || [
        ".zip",
        ".rar",
        ".7z",
        ".tar",
        ".gz",
        ".pdf",
        ".doc",
        ".docx",
        ".xls",
        ".xlsx",
        ".ppt",
        ".pptx",
        ".csv",
        ".txt",
        ".xml",
        ".rss",
        ".atom",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
        ".webp",
        ".ico",
        ".bmp",
        ".mp3",
        ".wav",
        ".mp4",
        ".avi",
        ".mov",
        ".wmv",
        ".mkv",
        ".exe",
        ".dmg",
        ".apk"
      ]
  )
    .map((value) => String(value || "").trim().toLowerCase())
    .filter(Boolean)
);
const retryEnvelopeKeys = {
  payload: "_clf_payload",
  retry: "_clf_retry",
  firstSeenAt: "_clf_first_seen_at"
};
const streamAckOptions = {
  deleteMessage: streamSettings.deleteAckedMessages
};

function log(message, payload) {
  const ts = new Date().toISOString();
  if (payload) {
    console.log(`[${ts}] ${message}`, payload);
    return;
  }
  console.log(`[${ts}] ${message}`);
}

function buildConsumerName(prefix) {
  const host = process.env.HOSTNAME || "worker";
  return `${prefix}-${host}-${process.pid}`;
}

function parseArgs(argv) {
  const parsed = { _: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg.startsWith("--")) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) {
        parsed[key] = next;
        i += 1;
      } else {
        parsed[key] = true;
      }
    } else {
      parsed._.push(arg);
    }
  }
  return parsed;
}

function toPositiveInt(value, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) {
    return fallback;
  }
  return Math.floor(num);
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function buildQueuePayload(url, careerUrl) {
  return JSON.stringify({ url, careerUrl });
}

function decodeStreamMessageValue(rawValue) {
  if (!rawValue || typeof rawValue !== "string") {
    return { value: rawValue, retryCount: 0, firstSeenAt: null };
  }
  const trimmed = rawValue.trim();
  if (!trimmed || (trimmed[0] !== "{" && trimmed[0] !== "[")) {
    return { value: rawValue, retryCount: 0, firstSeenAt: null };
  }
  try {
    const parsed = JSON.parse(trimmed);
    if (
      !parsed ||
      typeof parsed !== "object" ||
      Array.isArray(parsed) ||
      !Object.prototype.hasOwnProperty.call(parsed, retryEnvelopeKeys.payload)
    ) {
      return { value: rawValue, retryCount: 0, firstSeenAt: null };
    }
    const retryRaw = Number(parsed[retryEnvelopeKeys.retry] || 0);
    const retryCount =
      Number.isFinite(retryRaw) && retryRaw > 0 ? Math.floor(retryRaw) : 0;
    const payload = parsed[retryEnvelopeKeys.payload];
    const firstSeenAt = parsed[retryEnvelopeKeys.firstSeenAt] || null;
    if (payload === null || payload === undefined) {
      return { value: payload, retryCount, firstSeenAt };
    }
    if (typeof payload === "string") {
      return { value: payload, retryCount, firstSeenAt };
    }
    if (typeof payload === "object") {
      return { value: JSON.stringify(payload), retryCount, firstSeenAt };
    }
    return { value: String(payload), retryCount, firstSeenAt };
  } catch (error) {
    return { value: rawValue, retryCount: 0, firstSeenAt: null };
  }
}

function buildRetryEnvelopePayload(value, retryCount, firstSeenAt) {
  let payload = value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed && (trimmed[0] === "{" || trimmed[0] === "[")) {
      try {
        payload = JSON.parse(trimmed);
      } catch (error) {
        payload = value;
      }
    }
  }
  return JSON.stringify({
    [retryEnvelopeKeys.payload]: payload,
    [retryEnvelopeKeys.retry]: retryCount,
    [retryEnvelopeKeys.firstSeenAt]: firstSeenAt || new Date().toISOString()
  });
}

function getUrlFileExtension(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") {
    return "";
  }
  try {
    const parsed = new URL(rawUrl);
    const ext = path.extname(parsed.pathname || "");
    return String(ext || "").toLowerCase();
  } catch (error) {
    return "";
  }
}

function extractHttpStatusCode(errorText) {
  if (!errorText) {
    return 0;
  }
  const match =
    errorText.match(/response code\s+(\d{3})/i) ||
    errorText.match(/\bstatus(?:\s+code)?\s*[:=]?\s*(\d{3})\b/i);
  if (!match) {
    return 0;
  }
  const statusCode = Number(match[1]);
  if (!Number.isFinite(statusCode)) {
    return 0;
  }
  return statusCode;
}

function getRetryDecision(error, targetUrl) {
  const ext = getUrlFileExtension(targetUrl);
  if (ext && streamRetryNonRetryableExtensions.has(ext)) {
    return {
      retryable: false,
      reason: `non_retryable_extension:${ext}`
    };
  }

  const errorText = (error && error.toString ? error.toString() : String(error || ""))
    .toLowerCase();
  const statusCode = extractHttpStatusCode(errorText);
  if (
    statusCode >= 400 &&
    statusCode < 500 &&
    statusCode !== 408 &&
    statusCode !== 429
  ) {
    return {
      retryable: false,
      reason: `http_${statusCode}`
    };
  }

  if (
    errorText.includes("invalid url") ||
    errorText.includes("err_invalid_url") ||
    errorText.includes("protocol \"mailto:\"") ||
    errorText.includes("protocol \"tel:\"") ||
    errorText.includes("protocol \"javascript:\"") ||
    errorText.includes("unsupported protocol")
  ) {
    return {
      retryable: false,
      reason: "invalid_or_unsupported_url"
    };
  }

  return {
    retryable: true,
    reason: "retryable"
  };
}

async function incrementHealthCounter(redisClient, field, value = 1) {
  try {
    await redisClient.hIncrBy(settings.healthStatsKey, field, value);
  } catch (error) {
    log("Health counter update warning.", {
      key: settings.healthStatsKey,
      field,
      error: error.toString()
    });
  }
}

async function retryStreamMessage(
  redisClient,
  streamKey,
  value,
  retryCount,
  firstSeenAt,
  context = {}
) {
  if (!streamSettings.retryOnError || streamSettings.maxRetries <= 0) {
    return { requeued: false, reason: "disabled" };
  }
  if (retryCount >= streamSettings.maxRetries) {
    return { requeued: false, reason: "max_retries" };
  }
  const nextRetryCount = retryCount + 1;
  const retryPayload = buildRetryEnvelopePayload(
    value,
    nextRetryCount,
    firstSeenAt
  );
  await streamAdd(redisClient, streamKey, retryPayload);
  await incrementHealthCounter(redisClient, "retry_requeued_total", 1);
  await incrementHealthCounter(
    redisClient,
    `${streamKey}:retry_requeued`,
    1
  );
  log("Queued stream retry.", {
    stream: streamKey,
    retryCount: nextRetryCount,
    maxRetries: streamSettings.maxRetries,
    ...context
  });
  return { requeued: true, reason: "requeued", retryCount: nextRetryCount };
}

function getBatchSize(value, fallback) {
  return toPositiveInt(value, fallback);
}

function chunkArray(items, size) {
  const out = [];
  if (!Array.isArray(items) || !items.length) {
    return out;
  }
  const safeSize = getBatchSize(size, 500);
  for (let i = 0; i < items.length; i += safeSize) {
    out.push(items.slice(i, i + safeSize));
  }
  return out;
}

async function processWithConcurrency(items, concurrency, handler) {
  if (!Array.isArray(items) || !items.length) {
    return;
  }
  const safeConcurrency = Math.min(
    items.length,
    getBatchSize(concurrency, 1)
  );
  let nextIndex = 0;
  const workers = Array.from({ length: safeConcurrency }, () =>
    (async () => {
      while (true) {
        const current = nextIndex;
        nextIndex += 1;
        if (current >= items.length) {
          return;
        }
        await handler(items[current], current);
      }
    })()
  );
  await Promise.all(workers);
}

async function readStreamWorkBatch(
  redisClient,
  streamKey,
  group,
  consumer,
  claimState
) {
  let messages = [];
  if (
    streamSettings.claimMinIdleMs > 0 &&
    streamSettings.claimIntervalMs > 0
  ) {
    const now = Date.now();
    if (now - claimState.lastClaimAt >= streamSettings.claimIntervalMs) {
      claimState.lastClaimAt = now;
      const claimResult = await streamAutoClaimBatch(
        redisClient,
        streamKey,
        group,
        consumer,
        streamSettings.claimMinIdleMs,
        claimState.claimStartId,
        streamSettings.claimCount
      );
      if (claimResult) {
        claimState.claimStartId = claimResult.nextId || claimState.claimStartId;
        if (Array.isArray(claimResult.messages) && claimResult.messages.length) {
          messages = claimResult.messages;
        }
      }
    }
  }
  if (!messages.length) {
    const readResult = await streamReadGroupBatch(
      redisClient,
      streamKey,
      group,
      consumer,
      streamSettings.blockMs,
      streamSettings.readCount
    );
    if (Array.isArray(readResult) && readResult.length) {
      messages = readResult;
    }
  }
  return messages;
}

function normalizeLink(link) {
  try {
    const urlObj = new URL(link);
    const hash = urlObj.hash || "";
    const lowerHash = hash.toLowerCase();
    const hashLooksLikeJobRoute =
      (lowerHash.startsWith("#/") || lowerHash.startsWith("#!")) &&
      /#(\/|!\/)(job|jobs|career|careers|position|positions|opening|openings|opportunity|opportunities|posting|apply)\b/.test(
        lowerHash
      );
    if (!hashLooksLikeJobRoute) {
      urlObj.hash = "";
    }
    return urlObj.toString();
  } catch (error) {
    return link;
  }
}

function filterSameDomain(links, baseUrl) {
  if (!settings.sameDomainOnly) {
    return links;
  }
  let baseHost = "";
  try {
    baseHost = new URL(baseUrl).hostname.replace(/^www\./i, "");
  } catch (error) {
    return links;
  }
  return links.filter((link) => {
    try {
      const host = new URL(link).hostname.replace(/^www\./i, "");
      return baseHost === host;
    } catch (error) {
      return false;
    }
  });
}

function applySocialMediaFilter(links, baseUrl) {
  let domain = "";
  try {
    domain = new URL(baseUrl).hostname;
  } catch (error) {
    domain = "";
  }
  const dataset = links.map((url) => ({ url }));
  const filtered = deleteSocialMediaUrls(dataset, domain);
  return filtered.map((item) => item.url).filter(Boolean);
}

function parseQueueItem(value) {
  if (!value || typeof value !== "string") {
    return { url: value };
  }
  const trimmed = value.trim();
  if (!trimmed || (trimmed[0] !== "{" && trimmed[0] !== "[")) {
    return { url: value };
  }
  try {
    const parsed = JSON.parse(trimmed);
    if (parsed && typeof parsed === "object") {
      const url = parsed.url || parsed.link || parsed.jobUrl;
      if (url) {
        return {
          url,
          careerUrl: parsed.careerUrl || parsed.career_url
        };
      }
    }
  } catch (error) {
    return { url: value };
  }
  return { url: value };
}

function buildJobUpdate(url, careerUrl, extraFields) {
  const update = { url, ...extraFields };
  if (careerUrl) {
    update.careerUrl = careerUrl;
  }
  return update;
}

function isLikelyJobLink(url) {
  return (
    filterJobLinks([url], {
      strongPatterns: settings.jobLinkStrongPatterns,
      weakPatterns: settings.jobLinkWeakPatterns,
      excludePatterns: settings.jobLinkExcludePatterns
    }).length > 0
  );
}

async function findAnyDocument(collection, query) {
  const doc = await collection.findOne(query, { projection: { _id: 1 } });
  return Boolean(doc);
}

async function selectSeedCollection(db, candidates, fieldName, markField) {
  const uniqueCandidates = Array.from(
    new Set((candidates || []).filter(Boolean))
  );
  if (!uniqueCandidates.length) {
    return { name: null, reason: "no_candidates" };
  }
  const markFieldName = markField || "redisSeeded";
  const baseQuery = { [fieldName]: { $exists: true, $ne: null } };
  const unseededQuery = { ...baseQuery, [markFieldName]: { $ne: true } };

  for (const name of uniqueCandidates) {
    const collection = db.collection(name);
    if (await findAnyDocument(collection, unseededQuery)) {
      return { name, reason: "has_unseeded_docs" };
    }
  }

  for (const name of uniqueCandidates) {
    const collection = db.collection(name);
    if (await findAnyDocument(collection, baseQuery)) {
      return { name, reason: "field_present" };
    }
  }

  return { name: uniqueCandidates[0], reason: "fallback" };
}

let ensureIndexesPromise = null;

async function ensurePipelineIndexes(db) {
  if (ensureIndexesPromise) {
    return ensureIndexesPromise;
  }
  ensureIndexesPromise = (async () => {
    const tasks = [
      {
        name: collections.careerLinks,
        key: { careerUrl: 1 },
        options: {}
      },
      {
        name: collections.jobHtml,
        key: { url: 1 },
        options: { unique: true }
      },
      {
        name: collections.jobLinks,
        key: { url: 1 },
        options: { unique: true }
      },
      {
        name: collections.jobLinks,
        key: { htmlStatus: 1, htmlLockUntil: 1, _id: 1 },
        options: {}
      },
      {
        name: collections.jobLinks,
        key: { lastDiscoveredAt: 1 },
        options: {}
      }
    ];

    for (const task of tasks) {
      try {
        await db.collection(task.name).createIndex(task.key, task.options);
      } catch (error) {
        log("Index creation warning.", {
          collection: task.name,
          key: task.key,
          error: error.toString()
        });
      }
    }
  })();
  return ensureIndexesPromise;
}

function buildCareerLinksSuccessUpdate(
  careerUrl,
  links,
  jobLinks,
  source,
  userAgent,
  startedAt
) {
  const now = new Date();
  if (!settings.mergeLinksAcrossRuns) {
    return {
      $set: {
        careerUrl,
        links,
        linkCount: links.length,
        jobLinks,
        jobLinkCount: jobLinks.length,
        source,
        userAgent,
        fetchedAt: now,
        startedAt,
        lastSuccessAt: now,
        updatedAt: now,
        error: null
      },
      $setOnInsert: {
        createdAt: now
      }
    };
  }

  return [
    {
      $set: {
        careerUrl,
        links: {
          $setUnion: [{ $ifNull: ["$links", []] }, links]
        },
        jobLinks: {
          $setUnion: [{ $ifNull: ["$jobLinks", []] }, jobLinks]
        },
        source,
        userAgent,
        fetchedAt: now,
        startedAt,
        lastSuccessAt: now,
        updatedAt: now,
        error: null,
        createdAt: { $ifNull: ["$createdAt", now] }
      }
    },
    {
      $set: {
        linkCount: { $size: "$links" },
        jobLinkCount: { $size: "$jobLinks" }
      }
    }
  ];
}

function buildCareerLinksErrorUpdate(careerUrl, error, startedAt) {
  const now = new Date();
  if (settings.preserveLinksOnError) {
    return {
      $set: {
        careerUrl,
        error: error.toString(),
        lastErrorAt: now,
        fetchedAt: now,
        startedAt,
        updatedAt: now
      },
      $setOnInsert: {
        links: [],
        linkCount: 0,
        jobLinks: [],
        jobLinkCount: 0,
        createdAt: now
      }
    };
  }
  return {
    $set: {
      careerUrl,
      links: [],
      linkCount: 0,
      jobLinks: [],
      jobLinkCount: 0,
      error: error.toString(),
      lastErrorAt: now,
      fetchedAt: now,
      startedAt,
      updatedAt: now
    },
    $setOnInsert: {
      createdAt: now
    }
  };
}

async function persistDiscoveredJobLinks(
  jobLinksCollection,
  careerUrl,
  jobLinks,
  discoveredAt
) {
  if (!Array.isArray(jobLinks) || !jobLinks.length) {
    return 0;
  }
  let total = 0;
  const batches = chunkArray(jobLinks, settings.mongoBulkWriteBatchSize);
  for (const batch of batches) {
    const operations = batch.map((jobUrl) => ({
      updateOne: {
        filter: { url: jobUrl },
        update: {
          $setOnInsert: {
            url: jobUrl,
            createdAt: discoveredAt,
            firstDiscoveredAt: discoveredAt,
            htmlStatus: "pending",
            htmlAttempts: 0
          },
          $set: {
            careerUrl,
            lastDiscoveredAt: discoveredAt,
            updatedAt: discoveredAt
          },
          $addToSet: {
            careerUrls: careerUrl
          },
          $inc: {
            discoveryCount: 1
          }
        },
        upsert: true
      }
    }));
    if (!operations.length) {
      continue;
    }
    await jobLinksCollection.bulkWrite(operations, { ordered: false });
    total += operations.length;
  }
  return total;
}

async function enqueueJobLinks(redisClient, careerUrl, jobLinks) {
  if (!settings.enqueueHtmlFromLinksWorker) {
    return 0;
  }
  if (!Array.isArray(jobLinks) || !jobLinks.length) {
    return 0;
  }
  let enqueued = 0;
  const entries = jobLinks.map((jobUrl) => ({
    value: jobUrl,
    streamValue: buildQueuePayload(jobUrl, careerUrl)
  }));
  const batches = chunkArray(entries, settings.redisEnqueueBatchSize);
  for (const batch of batches) {
    enqueued += await saddAndStreamBatch(
      redisClient,
      queues.careerLinksDedup,
      queues.careerLinks,
      batch
    );
  }
  return enqueued;
}

function buildMongoHtmlClaimQuery(now, options = {}) {
  const retryErrors =
    options.retryErrors !== undefined
      ? options.retryErrors
      : settings.htmlMongoRetryErrors;
  const retryDelayMs = toPositiveInt(
    options.retryDelayMs,
    settings.htmlMongoRetryDelayMs
  );
  const statusQuery = [
    { htmlStatus: "pending" },
    { htmlStatus: "processing" },
    { htmlStatus: { $exists: false } }
  ];
  if (retryErrors) {
    statusQuery.push({
      htmlStatus: "error",
      $or: [
        { htmlErrorAt: { $exists: false } },
        {
          htmlErrorAt: {
            $lte: new Date(now.getTime() - retryDelayMs)
          }
        }
      ]
    });
  }
  return {
    $and: [
      { $or: statusQuery },
      {
        $or: [
          { htmlLockUntil: { $exists: false } },
          { htmlLockUntil: { $lte: now } }
        ]
      }
    ]
  };
}

async function claimMongoHtmlJob(jobLinksCollection, consumer, options = {}) {
  const now = new Date();
  const lockMs = toPositiveInt(options.lockMs, settings.htmlMongoLockMs);
  const claimed = await jobLinksCollection.findOneAndUpdate(
    buildMongoHtmlClaimQuery(now, options),
    {
      $set: {
        htmlStatus: "processing",
        htmlLockBy: consumer,
        htmlLockAt: now,
        htmlLockUntil: new Date(now.getTime() + lockMs),
        updatedAt: now
      },
      $inc: {
        htmlAttempts: 1
      }
    },
    {
      sort: { lastDiscoveredAt: 1, _id: 1 },
      returnDocument: "after",
      includeResultMetadata: false
    }
  );
  if (claimed && claimed.value) {
    return claimed.value;
  }
  return claimed || null;
}

async function markMongoHtmlJobState(
  jobLinksCollection,
  jobUrl,
  careerUrl,
  htmlStatus,
  extraSet = {}
) {
  const now = new Date();
  const setPayload = {
    url: jobUrl,
    htmlStatus,
    updatedAt: now,
    ...extraSet
  };
  if (careerUrl) {
    setPayload.careerUrl = careerUrl;
  }
  await jobLinksCollection.updateOne(
    { url: jobUrl },
    {
      $set: setPayload,
      $unset: {
        htmlLockBy: "",
        htmlLockAt: "",
        htmlLockUntil: ""
      },
      $setOnInsert: {
        createdAt: now
      }
    },
    { upsert: true }
  );
}

async function fetchCareerLinks(url) {
  let gotResult = null;
  try {
    gotResult = await fetchHtmlWithGot(url, {
      timeoutMs: settings.gotTimeoutMs
    });
    const links = extractLinksFromHtml(gotResult.html, url, {
      sameDomainOnly: settings.sameDomainOnly
    });
    if (links.length >= settings.minLinksForGot) {
      return {
        links,
        source: "got",
        userAgent: gotResult.userAgent
      };
    }
    log("Got returned few links, falling back to puppeteer.", {
      url,
      count: links.length
    });
  } catch (error) {
    log("Got failed, falling back to puppeteer.", {
      url,
      error: error.toString()
    });
  }

  const puppeteerResult = await fetchLinksWithPuppeteer(url, {
    timeoutMs: settings.puppeteerTimeoutMs
  });
  return puppeteerResult;
}

async function fetchPageHtml(url) {
  try {
    const gotResult = await fetchHtmlWithGot(url, {
      timeoutMs: settings.gotTimeoutMs
    });
    const htmlLength = gotResult.html ? gotResult.html.length : 0;
    if (!gotResult.html) {
      throw new Error("Got returned empty html body.");
    }
    if (
      htmlLength >= settings.minHtmlLength ||
      !settings.fallBackToPuppeteerOnShortHtml
    ) {
      return {
        html: gotResult.html,
        source: "got",
        userAgent: gotResult.userAgent
      };
    }
    log("Got returned short html, falling back to puppeteer.", {
      url,
      length: htmlLength
    });
  } catch (error) {
    log("Got failed for html fetch, falling back to puppeteer.", {
      url,
      error: error.toString()
    });
  }

  const puppeteerResult = await fetchHtmlWithPuppeteer(url, {
    timeoutMs: settings.puppeteerTimeoutMs
  });
  return puppeteerResult;
}

async function seedFromFile(redisClient, filePath) {
  const resolvedPath = path.resolve(filePath);
  const content = fs.readFileSync(resolvedPath, "utf8");
  const urls = content
    .split(/\\r?\\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const batches = chunkArray(urls, settings.redisEnqueueBatchSize);
  for (const batch of batches) {
    await streamAddBatch(redisClient, queues.careerPages, batch);
  }
  log("Seeded urls from file.", { count: urls.length, queue: queues.careerPages });
}

async function seedFromMongo(redisClient, db, collectionName, fieldName, options = {}) {
  const {
    batchSize = 500,
    max = 0,
    markField = "redisSeeded",
    markAtField = "redisSeededAt"
  } = options;
  const resolvedBatchSize = Number.isFinite(Number(batchSize)) ? Number(batchSize) : 500;
  const safeBatchSize = resolvedBatchSize > 0 ? resolvedBatchSize : 500;
  const resolvedMax = Number.isFinite(Number(max)) ? Number(max) : 0;
  const safeMarkField = markField || "redisSeeded";
  const safeMarkAtField = markAtField || "redisSeededAt";
  const collection = db.collection(collectionName);
  const query = {
    [fieldName]: { $exists: true, $ne: null },
    [safeMarkField]: { $ne: true }
  };
  const hasCandidate = await collection.findOne(query, { projection: { _id: 1 } });
  if (!hasCandidate) {
    const hasField = await collection.findOne(
      { [fieldName]: { $exists: true, $ne: null } },
      { projection: { _id: 1 } }
    );
    if (!hasField) {
      log("No documents found with seed field.", {
        collection: collectionName,
        field: fieldName
      });
    } else {
      log("No unseeded documents found.", {
        collection: collectionName,
        field: fieldName,
        markField: safeMarkField
      });
    }
    return;
  }
  let cursor = collection.find(query, { projection: { [fieldName]: 1 } });
  if (safeBatchSize > 0) {
    cursor = cursor.batchSize(safeBatchSize);
  }
  if (resolvedMax > 0) {
    cursor = cursor.limit(resolvedMax);
  }

  let count = 0;
  let batch = [];

  const flushBatch = async () => {
    if (!batch.length) {
      return;
    }
    const now = new Date();
    const validDocs = batch.filter((doc) => Boolean(doc[fieldName]));
    const urls = validDocs.map((doc) => doc[fieldName]);
    if (!urls.length) {
      batch = [];
      return;
    }
    await streamAddBatch(redisClient, queues.careerPages, urls);
    const updates = [];
    for (const doc of validDocs) {
      updates.push({
        updateOne: {
          filter: { _id: doc._id },
          update: {
            $set: {
              [safeMarkField]: true,
              [safeMarkAtField]: now
            }
          }
        }
      });
    }
    if (updates.length) {
      await collection.bulkWrite(updates, { ordered: false });
    }
    count += urls.length;
    log("Seeded mongo batch.", {
      batchCount: urls.length,
      totalCount: count,
      queue: queues.careerPages,
      collection: collectionName
    });
    batch = [];
  };

  for await (const doc of cursor) {
    batch.push(doc);
    if (safeBatchSize > 0 && batch.length >= safeBatchSize) {
      await flushBatch();
    }
  }
  await flushBatch();

  log("Seeded urls from mongo.", {
    count,
    queue: queues.careerPages,
    collection: collectionName,
    markField: safeMarkField,
    markAtField: safeMarkAtField
  });
}

async function runLinksWorker(args) {
  const redisClient = await createRedisClient(redisUrl);
  const { client: mongoClient, db } = await connectMongo(
    mongoConfig.uri,
    mongoConfig.database
  );
  const collection = db.collection(collections.careerLinks);
  const jobLinksCollection = db.collection(collections.jobLinks);
  const consumer = buildConsumerName("links");
  await ensurePipelineIndexes(db);
  await ensureStreamGroup(
    redisClient,
    queues.careerPages,
    streamGroups.careerPages
  );
  if (settings.enqueueHtmlFromLinksWorker) {
    await ensureStreamGroup(
      redisClient,
      queues.careerLinks,
      streamGroups.careerLinks
    );
  }
  let processed = 0;
  const claimState = {
    claimStartId: "0-0",
    lastClaimAt: 0
  };

  try {
    while (true) {
      const messages = await readStreamWorkBatch(
        redisClient,
        queues.careerPages,
        streamGroups.careerPages,
        consumer,
        claimState
      );
      if (!messages.length) {
        if (args.once) {
          break;
        }
        continue;
      }
      await processWithConcurrency(
        messages,
        settings.linksWorkerConcurrency,
        async (message) => {
          if (!message || !message.value) {
            await streamAck(
              redisClient,
              queues.careerPages,
              streamGroups.careerPages,
              message ? message.id : null,
              streamAckOptions
            );
            return;
          }
          const decodedMessage = decodeStreamMessageValue(message.value);
          const url = decodedMessage.value;
          if (!url) {
            await streamAck(
              redisClient,
              queues.careerPages,
              streamGroups.careerPages,
              message.id,
              streamAckOptions
            );
            return;
          }

          let shouldAck = false;
          const startedAt = new Date();
          try {
            const { links, source, userAgent } = await fetchCareerLinks(url);
            let normalizedLinks = links.map(normalizeLink);
            normalizedLinks = filterSameDomain(normalizedLinks, url);
            normalizedLinks = applySocialMediaFilter(normalizedLinks, url);
            normalizedLinks = Array.from(new Set(normalizedLinks));
            const jobLinks = filterJobLinks(normalizedLinks, {
              strongPatterns: settings.jobLinkStrongPatterns,
              weakPatterns: settings.jobLinkWeakPatterns,
              excludePatterns: settings.jobLinkExcludePatterns
            });

            await collection.updateOne(
              { careerUrl: url },
              buildCareerLinksSuccessUpdate(
                url,
                normalizedLinks,
                jobLinks,
                source,
                userAgent,
                startedAt
              ),
              { upsert: true }
            );

            const discoveredAt = new Date();
            const persistedCount = await persistDiscoveredJobLinks(
              jobLinksCollection,
              url,
              jobLinks,
              discoveredAt
            );
            const queuedCount = await enqueueJobLinks(redisClient, url, jobLinks);
            if (settings.enqueueHtmlFromLinksWorker && jobLinks.length) {
              await jobLinksCollection.updateMany(
                { url: { $in: jobLinks } },
                {
                  $set: {
                    htmlQueued: true,
                    htmlQueuedAt: discoveredAt,
                    updatedAt: discoveredAt
                  }
                }
              );
            }

            processed += 1;
            log("Career links fetched.", {
              url,
              linkCount: normalizedLinks.length,
              jobLinkCount: jobLinks.length,
              source,
              persistedJobLinks: persistedCount,
              queuedJobLinks: queuedCount,
              enqueueHtmlFromLinksWorker: settings.enqueueHtmlFromLinksWorker,
              retryCount: decodedMessage.retryCount
            });
            shouldAck = true;
          } catch (error) {
            try {
              const retryDecision = getRetryDecision(
                error,
                decodedMessage.value || url
              );
              const requeued = retryDecision.retryable
                ? await retryStreamMessage(
                  redisClient,
                  queues.careerPages,
                  decodedMessage.value,
                  decodedMessage.retryCount,
                  decodedMessage.firstSeenAt,
                  {
                    id: message.id,
                    url,
                    error: error.toString()
                  }
                )
                : {
                  requeued: false,
                  reason: retryDecision.reason
                };
              if (requeued.requeued) {
                shouldAck = true;
                return;
              }
              if (requeued.reason === "max_retries") {
                await incrementHealthCounter(
                  redisClient,
                  "retry_exhausted_total",
                  1
                );
                await incrementHealthCounter(
                  redisClient,
                  `${queues.careerPages}:retry_exhausted`,
                  1
                );
              } else if (!retryDecision.retryable) {
                await incrementHealthCounter(
                  redisClient,
                  "retry_skipped_total",
                  1
                );
                await incrementHealthCounter(
                  redisClient,
                  `${queues.careerPages}:retry_skipped`,
                  1
                );
              }
              await collection.updateOne(
                { careerUrl: url },
                buildCareerLinksErrorUpdate(url, error, startedAt),
                { upsert: true }
              );
              log("Error fetching career links.", {
                url,
                error: error.toString(),
                retryCount: decodedMessage.retryCount,
                retryDecision: requeued.reason
              });
              shouldAck = true;
            } catch (handlingError) {
              log("Links failure handling failed. Leaving stream message pending.", {
                id: message.id,
                url,
                error: error.toString(),
                handlingError: handlingError.toString()
              });
            }
          } finally {
            if (shouldAck) {
              await streamAck(
                redisClient,
                queues.careerPages,
                streamGroups.careerPages,
                message.id,
                streamAckOptions
              );
            }
          }
        }
      );

      if (args.max && processed >= args.max) {
        break;
      }
    }
  } finally {
    await closeBrowser();
    await redisClient.quit();
    await mongoClient.close();
  }
}

async function runHtmlWorker(args) {
  const redisClient = await createRedisClient(redisUrl);
  const { client: mongoClient, db } = await connectMongo(
    mongoConfig.uri,
    mongoConfig.database
  );
  const collection = db.collection(collections.jobHtml);
  const jobLinksCollection = db.collection(collections.jobLinks);
  const consumer = buildConsumerName("html");
  await ensurePipelineIndexes(db);
  await ensureStreamGroup(
    redisClient,
    queues.careerLinks,
    streamGroups.careerLinks
  );
  let processed = 0;
  const claimState = {
    claimStartId: "0-0",
    lastClaimAt: 0
  };

  try {
    while (true) {
      const messages = await readStreamWorkBatch(
        redisClient,
        queues.careerLinks,
        streamGroups.careerLinks,
        consumer,
        claimState
      );
      if (!messages.length) {
        if (args.once) {
          break;
        }
        continue;
      }
      await processWithConcurrency(
        messages,
        settings.htmlWorkerConcurrency,
        async (message) => {
          if (!message || !message.value) {
            await streamAck(
              redisClient,
              queues.careerLinks,
              streamGroups.careerLinks,
              message ? message.id : null,
              streamAckOptions
            );
            return;
          }

          const decodedMessage = decodeStreamMessageValue(message.value);
          const messageValue = decodedMessage.value;
          let shouldAck = false;
          const startedAt = new Date();
          let jobUrl = null;
          let careerUrl = null;
          try {
            const parsed = parseQueueItem(messageValue);
            jobUrl = parsed.url;
            careerUrl = parsed.careerUrl;
            if (!jobUrl) {
              shouldAck = true;
              return;
            }
            if (
              settings.skipNonJobLinksInHtmlWorkers &&
              !isLikelyJobLink(jobUrl)
            ) {
              const fetchedAt = new Date();
              await collection.updateOne(
                { url: jobUrl },
                {
                  $set: {
                    ...buildJobUpdate(jobUrl, careerUrl, {
                      html: "",
                      skipped: true,
                      skipReason: "non_job_link",
                      fetchedAt,
                      startedAt
                    })
                  }
                },
                { upsert: true }
              );
              await markMongoHtmlJobState(
                jobLinksCollection,
                jobUrl,
                careerUrl,
                "skipped",
                {
                  skipReason: "non_job_link",
                  htmlFetchedAt: fetchedAt,
                  htmlError: null
                }
              );
              log("Skipped non-job link.", { url: jobUrl });
              shouldAck = true;
              return;
            }
            const { html, source, userAgent } = await fetchPageHtml(jobUrl);
            const { rawBlocks, jobPosting } = extractJobPostingFromHtml(html);
            let parsedLdJson = null;
            if (jobPosting) {
              try {
                parsedLdJson = await parseLdJson(jobPosting);
              } catch (error) {
                parsedLdJson = null;
              }
            }
            const fetchedAt = new Date();
            await collection.updateOne(
              { url: jobUrl },
              {
                $set: {
                  ...buildJobUpdate(jobUrl, careerUrl, {
                    // html,
                    source,
                    userAgent,
                    ldjsonRaw: jobPosting,
                    ldjsonParsed: parsedLdJson,
                    ldjsonBlocks: rawBlocks,
                    fetchedAt,
                    startedAt
                  })
                }
              },
              { upsert: true }
            );
            await markMongoHtmlJobState(
              jobLinksCollection,
              jobUrl,
              careerUrl,
              "done",
              {
                htmlFetchedAt: fetchedAt,
                htmlSource: source,
                htmlUserAgent: userAgent,
                htmlError: null
              }
            );
            processed += 1;
            log("HTML fetched.", {
              url: jobUrl,
              source,
              length: html.length,
              retryCount: decodedMessage.retryCount
            });
            shouldAck = true;
          } catch (error) {
            try {
              const retryDecision = getRetryDecision(
                error,
                jobUrl || messageValue
              );
              const requeued = retryDecision.retryable
                ? await retryStreamMessage(
                  redisClient,
                  queues.careerLinks,
                  messageValue,
                  decodedMessage.retryCount,
                  decodedMessage.firstSeenAt,
                  {
                    id: message.id,
                    url: jobUrl || messageValue,
                    error: error.toString()
                  }
                )
                : {
                  requeued: false,
                  reason: retryDecision.reason
                };
              if (requeued.requeued) {
                shouldAck = true;
                return;
              }
              if (requeued.reason === "max_retries") {
                await incrementHealthCounter(
                  redisClient,
                  "retry_exhausted_total",
                  1
                );
                await incrementHealthCounter(
                  redisClient,
                  `${queues.careerLinks}:retry_exhausted`,
                  1
                );
              } else if (!retryDecision.retryable) {
                await incrementHealthCounter(
                  redisClient,
                  "retry_skipped_total",
                  1
                );
                await incrementHealthCounter(
                  redisClient,
                  `${queues.careerLinks}:retry_skipped`,
                  1
                );
              }
              const fallbackUrl = jobUrl || messageValue;
              const fetchedAt = new Date();
              await collection.updateOne(
                { url: fallbackUrl },
                {
                  $set: {
                    ...buildJobUpdate(fallbackUrl, careerUrl, {
                      html: "",
                      error: error.toString(),
                      fetchedAt,
                      startedAt
                    })
                  }
                },
                { upsert: true }
              );
              await markMongoHtmlJobState(
                jobLinksCollection,
                fallbackUrl,
                careerUrl,
                "error",
                {
                  htmlError: error.toString(),
                  htmlErrorAt: fetchedAt
                }
              );
              log("Error fetching HTML.", {
                url: fallbackUrl,
                error: error.toString(),
                retryCount: decodedMessage.retryCount,
                retryDecision: requeued.reason
              });
              shouldAck = true;
            } catch (handlingError) {
              log("HTML failure handling failed. Leaving stream message pending.", {
                id: message.id,
                url: jobUrl || messageValue,
                error: error.toString(),
                handlingError: handlingError.toString()
              });
            }
          } finally {
            if (shouldAck) {
              await streamAck(
                redisClient,
                queues.careerLinks,
                streamGroups.careerLinks,
                message.id,
                streamAckOptions
              );
            }
          }
        }
      );

      if (args.max && processed >= args.max) {
        break;
      }
    }
  } finally {
    await closeBrowser();
    await redisClient.quit();
    await mongoClient.close();
  }
}

async function runHtmlMongoWorker(args) {
  const { client: mongoClient, db } = await connectMongo(
    mongoConfig.uri,
    mongoConfig.database
  );
  const collection = db.collection(collections.jobHtml);
  const jobLinksCollection = db.collection(collections.jobLinks);
  const consumer = buildConsumerName("html-mongo");
  const retryErrors = parseBoolean(args["retry-errors"], settings.htmlMongoRetryErrors);
  const retryDelayMs = toPositiveInt(
    args["retry-delay-ms"] || args.retryDelayMs,
    settings.htmlMongoRetryDelayMs
  );
  const lockMs = toPositiveInt(
    args["lock-ms"] || args.lockMs,
    settings.htmlMongoLockMs
  );
  const pollMs = toPositiveInt(
    args["poll-ms"] || args.pollMs,
    settings.htmlMongoPollMs
  );
  const max = Number(args.max || 0);
  let processed = 0;

  await ensurePipelineIndexes(db);

  try {
    while (true) {
      const claimed = await claimMongoHtmlJob(jobLinksCollection, consumer, {
        retryErrors,
        retryDelayMs,
        lockMs
      });
      if (!claimed) {
        if (args.once) {
          break;
        }
        await sleep(pollMs);
        continue;
      }

      const startedAt = new Date();
      const jobUrl = claimed.url;
      const careerUrl =
        claimed.careerUrl ||
        (Array.isArray(claimed.careerUrls) && claimed.careerUrls.length
          ? claimed.careerUrls[0]
          : null);

      if (!jobUrl) {
        await jobLinksCollection.updateOne(
          { _id: claimed._id },
          {
            $set: {
              htmlStatus: "error",
              htmlError: "Missing url in job links collection record.",
              htmlErrorAt: new Date(),
              updatedAt: new Date()
            },
            $unset: {
              htmlLockBy: "",
              htmlLockAt: "",
              htmlLockUntil: ""
            }
          }
        );
        continue;
      }

      try {
        if (
          settings.skipNonJobLinksInHtmlWorkers &&
          !isLikelyJobLink(jobUrl)
        ) {
          const fetchedAt = new Date();
          await collection.updateOne(
            { url: jobUrl },
            {
              $set: {
                ...buildJobUpdate(jobUrl, careerUrl, {
                  html: "",
                  skipped: true,
                  skipReason: "non_job_link",
                  fetchedAt,
                  startedAt
                })
              }
            },
            { upsert: true }
          );
          await markMongoHtmlJobState(
            jobLinksCollection,
            jobUrl,
            careerUrl,
            "skipped",
            {
              skipReason: "non_job_link",
              htmlFetchedAt: fetchedAt,
              htmlError: null
            }
          );
          processed += 1;
          log("Skipped non-job link.", { url: jobUrl, source: "mongo" });
        } else {
          const { html, source, userAgent } = await fetchPageHtml(jobUrl);
          const { rawBlocks, jobPosting } = extractJobPostingFromHtml(html);
          let parsedLdJson = null;
          if (jobPosting) {
            try {
              parsedLdJson = await parseLdJson(jobPosting);
            } catch (error) {
              parsedLdJson = null;
            }
          }
          const fetchedAt = new Date();
          await collection.updateOne(
            { url: jobUrl },
            {
              $set: {
                ...buildJobUpdate(jobUrl, careerUrl, {
                  // html,
                  source,
                  userAgent,
                  ldjsonRaw: jobPosting,
                  ldjsonParsed: parsedLdJson,
                  ldjsonBlocks: rawBlocks,
                  fetchedAt,
                  startedAt
                })
              }
            },
            { upsert: true }
          );
          await markMongoHtmlJobState(
            jobLinksCollection,
            jobUrl,
            careerUrl,
            "done",
            {
              htmlFetchedAt: fetchedAt,
              htmlSource: source,
              htmlUserAgent: userAgent,
              htmlError: null
            }
          );
          processed += 1;
          log("HTML fetched.", { url: jobUrl, source, length: html.length });
        }
      } catch (error) {
        const fetchedAt = new Date();
        await collection.updateOne(
          { url: jobUrl },
          {
            $set: {
              ...buildJobUpdate(jobUrl, careerUrl, {
                html: "",
                error: error.toString(),
                fetchedAt,
                startedAt
              })
            }
          },
          { upsert: true }
        );
        await markMongoHtmlJobState(
          jobLinksCollection,
          jobUrl,
          careerUrl,
          "error",
          {
            htmlError: error.toString(),
            htmlErrorAt: fetchedAt
          }
        );
        log("Error fetching HTML.", { url: jobUrl, error: error.toString() });
      }

      if (max > 0 && processed >= max) {
        break;
      }
    }
  } finally {
    await closeBrowser();
    await mongoClient.close();
  }
}

async function seedJobLinksToHtmlQueue(args) {
  const redisClient = await createRedisClient(redisUrl);
  const { client: mongoClient, db } = await connectMongo(
    mongoConfig.uri,
    mongoConfig.database
  );
  const jobLinksCollection = db.collection(collections.jobLinks);
  const batchSize = toPositiveInt(
    args["batch-size"] || args.batchSize,
    settings.redisEnqueueBatchSize
  );
  const max = Number(args.max || 0);
  const useDedupe = parseBoolean(args["use-dedupe"], false);
  const includeQueued = parseBoolean(args["include-queued"], false);
  const resetQueued = parseBoolean(args["reset-queued"], false);

  let totalQueued = 0;
  let batch = [];
  const baseStatusQuery = {
    url: { $exists: true, $ne: null },
    htmlStatus: { $nin: ["done", "skipped"] }
  };
  const query = {
    ...baseStatusQuery,
    ...(includeQueued ? {} : { htmlQueued: { $ne: true } })
  };
  let cursor = jobLinksCollection.find(query, {
    projection: { url: 1, careerUrl: 1 },
    sort: { _id: 1 }
  });
  if (max > 0) {
    cursor = cursor.limit(max);
  }
  cursor = cursor.batchSize(batchSize);
  await ensurePipelineIndexes(db);

  await ensureStreamGroup(
    redisClient,
    queues.careerLinks,
    streamGroups.careerLinks
  );
  if (resetQueued) {
    const resetResult = await jobLinksCollection.updateMany(
      baseStatusQuery,
      {
        $unset: {
          htmlQueued: "",
          htmlQueuedAt: ""
        }
      }
    );
    log("Reset htmlQueued flags for pending links.", {
      matched: resetResult.matchedCount,
      modified: resetResult.modifiedCount
    });
  }

  const flushBatch = async () => {
    if (!batch.length) {
      return;
    }
    const now = new Date();
    const valid = batch.filter((doc) => Boolean(doc.url));
    if (!valid.length) {
      batch = [];
      return;
    }
    let batchQueuedCount = valid.length;
    if (useDedupe) {
      const entries = valid.map((doc) => ({
        value: doc.url,
        streamValue: buildQueuePayload(doc.url, doc.careerUrl)
      }));
      batchQueuedCount = await saddAndStreamBatch(
        redisClient,
        queues.careerLinksDedup,
        queues.careerLinks,
        entries
      );
    } else {
      const payloads = valid.map((doc) => buildQueuePayload(doc.url, doc.careerUrl));
      await streamAddBatch(redisClient, queues.careerLinks, payloads);
    }

    const updates = valid.map((doc) => ({
      updateOne: {
        filter: { _id: doc._id },
        update: {
          $set: {
            htmlQueued: true,
            htmlQueuedAt: now,
            updatedAt: now
          }
        }
      }
    }));
    if (updates.length) {
      await jobLinksCollection.bulkWrite(updates, { ordered: false });
    }
    totalQueued += batchQueuedCount;
    log("Queued job links batch for html worker.", {
      batchSize: valid.length,
      queuedInRedis: batchQueuedCount,
      totalQueued
    });
    batch = [];
  };

  try {
    for await (const doc of cursor) {
      batch.push(doc);
      if (batch.length >= batchSize) {
        await flushBatch();
      }
    }
    await flushBatch();
    log("Queued job links for html worker.", {
      totalQueued,
      queue: queues.careerLinks,
      dedupe: useDedupe
    });
  } finally {
    await redisClient.quit();
    await mongoClient.close();
  }
}

function parseRedisInfoSection(raw) {
  const out = {};
  if (!raw || typeof raw !== "string") {
    return out;
  }
  const lines = raw.split(/\r?\n/);
  for (const line of lines) {
    if (!line || line[0] === "#") {
      continue;
    }
    const separatorIndex = line.indexOf(":");
    if (separatorIndex <= 0) {
      continue;
    }
    const key = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim();
    if (!key) {
      continue;
    }
    out[key] = value;
  }
  return out;
}

function maskRedisUrl(rawUrl) {
  if (!rawUrl) {
    return rawUrl;
  }
  try {
    const parsed = new URL(rawUrl);
    if (parsed.password) {
      parsed.password = "***";
    }
    return parsed.toString();
  } catch (error) {
    return rawUrl;
  }
}

function toNumberOrZero(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return parsed;
}

async function getStreamHealthSnapshot(redisClient, streamKey, groupName) {
  const type = await redisClient.type(streamKey);
  if (type !== "stream") {
    return {
      key: streamKey,
      type,
      length: 0,
      groupName,
      group: null,
      groups: []
    };
  }
  const [length, groups] = await Promise.all([
    redisClient.xLen(streamKey),
    redisClient.xInfoGroups(streamKey).catch(() => [])
  ]);
  const normalizedGroups = Array.isArray(groups)
    ? groups.map((group) => ({
      name: group.name,
      consumers: toNumberOrZero(group.consumers),
      pending: toNumberOrZero(group.pending),
      lastDeliveredId: group.lastDeliveredId || null
    }))
    : [];
  const currentGroup =
    normalizedGroups.find((group) => group.name === groupName) || null;
  return {
    key: streamKey,
    type,
    length: toNumberOrZero(length),
    groupName,
    group: currentGroup,
    groups: normalizedGroups
  };
}

async function collectRedisHealth(redisClient) {
  const [
    memoryInfoRaw,
    statsInfoRaw,
    serverInfoRaw,
    persistenceInfoRaw,
    pagesStream,
    linksStream,
    dedupeType,
    retryCounters
  ] = await Promise.all([
    redisClient.info("memory"),
    redisClient.info("stats"),
    redisClient.info("server"),
    redisClient.info("persistence"),
    getStreamHealthSnapshot(
      redisClient,
      queues.careerPages,
      streamGroups.careerPages
    ),
    getStreamHealthSnapshot(
      redisClient,
      queues.careerLinks,
      streamGroups.careerLinks
    ),
    redisClient.type(queues.careerLinksDedup),
    redisClient.hGetAll(settings.healthStatsKey).catch(() => ({}))
  ]);
  let dedupeSize = 0;
  if (dedupeType === "set") {
    dedupeSize = toNumberOrZero(await redisClient.sCard(queues.careerLinksDedup));
  }
  const memoryInfo = parseRedisInfoSection(memoryInfoRaw);
  const statsInfo = parseRedisInfoSection(statsInfoRaw);
  const serverInfo = parseRedisInfoSection(serverInfoRaw);
  const persistenceInfo = parseRedisInfoSection(persistenceInfoRaw);
  const retryStats = {};
  for (const [key, value] of Object.entries(retryCounters || {})) {
    retryStats[key] = toNumberOrZero(value);
  }
  return {
    url: maskRedisUrl(redisUrl),
    memory: {
      usedMemoryHuman: memoryInfo.used_memory_human || null,
      usedMemoryPeakHuman: memoryInfo.used_memory_peak_human || null,
      maxMemoryHuman: memoryInfo.maxmemory_human || null,
      fragmentationRatio: memoryInfo.mem_fragmentation_ratio || null
    },
    stats: {
      evictedKeys: toNumberOrZero(statsInfo.evicted_keys),
      expiredKeys: toNumberOrZero(statsInfo.expired_keys),
      keyspaceHits: toNumberOrZero(statsInfo.keyspace_hits),
      keyspaceMisses: toNumberOrZero(statsInfo.keyspace_misses)
    },
    server: {
      redisVersion: serverInfo.redis_version || null,
      uptimeSeconds: toNumberOrZero(serverInfo.uptime_in_seconds),
      role: serverInfo.role || null
    },
    persistence: {
      aofEnabled: persistenceInfo.aof_enabled || null,
      rdbLastBgsaveStatus: persistenceInfo.rdb_last_bgsave_status || null
    },
    streams: {
      careerPages: pagesStream,
      careerLinks: linksStream
    },
    dedupe: {
      key: queues.careerLinksDedup,
      type: dedupeType,
      size: dedupeSize
    },
    retryStatsKey: settings.healthStatsKey,
    retryCounters: retryStats
  };
}

async function collectMongoHealth(db) {
  const jobLinksCollection = db.collection(collections.jobLinks);
  const jobHtmlCollection = db.collection(collections.jobHtml);
  const baseQuery = {
    url: { $exists: true, $ne: null }
  };
  const [
    total,
    pending,
    processing,
    done,
    skipped,
    error,
    missingStatus,
    queuedPending,
    htmlDocs
  ] = await Promise.all([
    jobLinksCollection.countDocuments(baseQuery),
    jobLinksCollection.countDocuments({ ...baseQuery, htmlStatus: "pending" }),
    jobLinksCollection.countDocuments({ ...baseQuery, htmlStatus: "processing" }),
    jobLinksCollection.countDocuments({ ...baseQuery, htmlStatus: "done" }),
    jobLinksCollection.countDocuments({ ...baseQuery, htmlStatus: "skipped" }),
    jobLinksCollection.countDocuments({ ...baseQuery, htmlStatus: "error" }),
    jobLinksCollection.countDocuments({
      ...baseQuery,
      htmlStatus: { $exists: false }
    }),
    jobLinksCollection.countDocuments({
      ...baseQuery,
      htmlQueued: true,
      htmlStatus: { $nin: ["done", "skipped"] }
    }),
    jobHtmlCollection.estimatedDocumentCount()
  ]);
  return {
    database: mongoConfig.database,
    collections,
    jobLinks: {
      total,
      pending,
      processing,
      done,
      skipped,
      error,
      missingStatus,
      queuedPending
    },
    jobHtml: {
      total: htmlDocs
    }
  };
}

function printHealthReport(report) {
  const formatInt = (value) => toNumberOrZero(value).toLocaleString("en-US");
  const redis = report.redis;
  console.log(`Health Timestamp: ${report.timestamp}`);
  console.log(`Redis URL: ${redis.url}`);
  console.log(
    `Redis Memory: ${redis.memory.usedMemoryHuman} / ${redis.memory.maxMemoryHuman} (peak ${redis.memory.usedMemoryPeakHuman})`
  );
  console.log(
    `Redis Stats: evicted=${formatInt(redis.stats.evictedKeys)} expired=${formatInt(redis.stats.expiredKeys)} hits=${formatInt(redis.stats.keyspaceHits)} misses=${formatInt(redis.stats.keyspaceMisses)}`
  );
  for (const stream of [redis.streams.careerPages, redis.streams.careerLinks]) {
    const group = stream.group || {
      name: stream.groupName,
      pending: 0,
      consumers: 0,
      lastDeliveredId: null
    };
    console.log(
      `Stream ${stream.key}: len=${formatInt(stream.length)} group=${group.name} pending=${formatInt(group.pending)} consumers=${formatInt(group.consumers)} lastDeliveredId=${group.lastDeliveredId || "n/a"}`
    );
  }
  console.log(
    `Dedupe Set ${redis.dedupe.key}: type=${redis.dedupe.type} size=${formatInt(redis.dedupe.size)}`
  );
  const retryCounters = redis.retryCounters || {};
  if (Object.keys(retryCounters).length) {
    console.log(`Retry Counters (${redis.retryStatsKey}):`);
    for (const [key, value] of Object.entries(retryCounters)) {
      console.log(`  ${key}: ${formatInt(value)}`);
    }
  } else {
    console.log(`Retry Counters (${redis.retryStatsKey}): empty`);
  }

  if (report.mongo && !report.mongo.error) {
    const mongo = report.mongo;
    console.log(`Mongo DB: ${mongo.database}`);
    console.log(
      `Mongo jobLinks: total=${formatInt(mongo.jobLinks.total)} pending=${formatInt(mongo.jobLinks.pending)} processing=${formatInt(mongo.jobLinks.processing)} done=${formatInt(mongo.jobLinks.done)} skipped=${formatInt(mongo.jobLinks.skipped)} error=${formatInt(mongo.jobLinks.error)} missingStatus=${formatInt(mongo.jobLinks.missingStatus)} queuedPending=${formatInt(mongo.jobLinks.queuedPending)}`
    );
    console.log(`Mongo jobHtml docs: ${formatInt(mongo.jobHtml.total)}`);
  } else if (report.mongo && report.mongo.error) {
    console.log(`Mongo Health Error: ${report.mongo.error}`);
  }
}

async function runHealthCommand(args) {
  const outputJson = parseBoolean(args.json, false);
  const redisClient = await createRedisClient(redisUrl);
  let mongoClient = null;
  try {
    const redisHealth = await collectRedisHealth(redisClient);
    let mongoHealth = null;
    try {
      const mongoConnection = await connectMongo(
        mongoConfig.uri,
        mongoConfig.database
      );
      mongoClient = mongoConnection.client;
      mongoHealth = await collectMongoHealth(mongoConnection.db);
    } catch (error) {
      mongoHealth = { error: error.toString() };
    }
    const report = {
      timestamp: new Date().toISOString(),
      redis: redisHealth,
      mongo: mongoHealth
    };
    if (outputJson) {
      console.log(JSON.stringify(report, null, 2));
      return;
    }
    printHealthReport(report);
  } finally {
    await redisClient.quit();
    if (mongoClient) {
      await mongoClient.close();
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const command = args._[0];

  if (!command) {
    console.log("Usage:");
    console.log("  node pipeline/index.js seed --file <path>");
    console.log("  node pipeline/index.js seed --collection <name> --field <field> [--batch-size <n>] [--max <n>]");
    console.log("                             [--mark-field <name>] [--mark-at-field <name>]");
    console.log("  node pipeline/index.js worker:links [--once] [--max <n>]");
    console.log("  node pipeline/index.js seed:job-links [--batch-size <n>] [--max <n>] [--use-dedupe]");
    console.log("                             [--include-queued] [--reset-queued]");
    console.log("  node pipeline/index.js worker:html [--once] [--max <n>] [--source redis|mongo]");
    console.log("  node pipeline/index.js worker:html-mongo [--once] [--max <n>]");
    console.log("       optional: [--retry-errors] [--retry-delay-ms <n>] [--poll-ms <n>] [--lock-ms <n>]");
    console.log("  node pipeline/index.js health [--json]");
    process.exit(1);
  }

  if (command === "seed") {
    const redisClient = await createRedisClient(redisUrl);
    const filePath = args.file;
    const explicitCollectionName = args.collection;
    const fallbackCollections = [
      mongoConfig.import_collection,
      collections.careerPages
    ];
    const fieldName = args.field || "careerUrl";
    const batchSize = Number(
      args["batch-size"] ||
      args.batchSize ||
      (pipelineConfig.seed && pipelineConfig.seed.batch_size) ||
      500
    );
    const max = Number(args.max || (pipelineConfig.seed && pipelineConfig.seed.max) || 0);
    const markField =
      args["mark-field"] ||
      args.markField ||
      (pipelineConfig.seed && pipelineConfig.seed.mark_field) ||
      "redisSeeded";
    const markAtField =
      args["mark-at-field"] ||
      args.markAtField ||
      (pipelineConfig.seed && pipelineConfig.seed.mark_at_field) ||
      "redisSeededAt";

    try {
      await ensureStreamGroup(
        redisClient,
        queues.careerPages,
        streamGroups.careerPages
      );
      if (filePath) {
        await seedFromFile(redisClient, filePath);
      } else {
        const { client: mongoClient, db } = await connectMongo(
          mongoConfig.uri,
          mongoConfig.database
        );
        try {
          let collectionName = explicitCollectionName;
          if (!collectionName) {
            const selection = await selectSeedCollection(
              db,
              fallbackCollections,
              fieldName,
              markField
            );
            collectionName = selection.name || fallbackCollections[0];
            log("Selected seed collection.", {
              collection: collectionName,
              reason: selection.reason
            });
          }
          if (!collectionName) {
            throw new Error("No mongo collection available for seeding.");
          }
          log("Seed settings.", {
            collection: collectionName,
            field: fieldName,
            batchSize,
            max,
            markField,
            markAtField
          });
          await seedFromMongo(redisClient, db, collectionName, fieldName, {
            batchSize,
            max,
            markField,
            markAtField
          });
        } finally {
          await mongoClient.close();
        }
      }
    } finally {
      await redisClient.quit();
    }
    return;
  }

  if (command === "worker:links") {
    await runLinksWorker(args);
    return;
  }

  if (command === "seed:job-links") {
    await seedJobLinksToHtmlQueue(args);
    return;
  }

  if (command === "worker:html") {
    const source = String(args.source || "").toLowerCase();
    if (source === "mongo") {
      await runHtmlMongoWorker(args);
    } else {
      await runHtmlWorker(args);
    }
    return;
  }

  if (command === "worker:html-mongo") {
    await runHtmlMongoWorker(args);
    return;
  }

  if (command === "health") {
    await runHealthCommand(args);
    return;
  }

  console.log(`Unknown command: ${command}`);
  process.exit(1);
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});

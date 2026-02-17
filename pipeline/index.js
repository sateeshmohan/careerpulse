const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const config = require("config");

const { fetchHtmlWithGot } = require("./http_client");
const {
  extractLinkEntriesFromHtml,
  extractPageTextSampleFromHtml
} = require("./link_extractor");
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
  streamMoveBatch,
  saddAndStreamBatch
} = require("./redis_queue");
const { connectMongo } = require("./mongo_client");
const { extractJobPostingFromHtml } = require("./ldjson_extractor");
const parseLdJson = require("../ldjson_parser");
const { deleteSocialMediaUrls } = require("../delete_socialmedia_links");
const { filterJobLinks } = require("./job_link_filter");
const {
  detectExpiredOrNoJobs,
  extractAtsCareerLinks: extractAtsCareerLinksFromRules,
  filterAtsCareerLinks: filterAtsCareerLinksFromRules,
  filterCareerLinks: filterCareerLinksFromRules,
  filterLinksByAnchorText,
  isAtsCareerLink: isAtsCareerLinkFromRules,
  isExcludedDomain
} = require("./career_link_rules");

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
  careerPagesFailed:
    process.env.CAREER_PAGES_FAILED_QUEUE ||
    (pipelineConfig.queues && pipelineConfig.queues.career_pages_failed) ||
    "career:pages:failed",
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

function parseFetchMode(value, fallback = "auto") {
  const normalized = String(value || fallback).toLowerCase();
  if (normalized === "puppeteer" || normalized === "got" || normalized === "auto") {
    return normalized;
  }
  return fallback;
}

const settings = {
  sameDomainOnly: parseBoolean(
    process.env.SAME_DOMAIN_ONLY,
    pipelineConfig.same_domain_only !== undefined
      ? pipelineConfig.same_domain_only
      : true
  ),
  keepExternalLikelyJobLinks: parseBoolean(
    process.env.KEEP_EXTERNAL_LIKELY_JOB_LINKS,
    pipelineConfig.keep_external_likely_job_links !== undefined
      ? pipelineConfig.keep_external_likely_job_links
      : true
  ),
  minLinksForGot: Number(
    process.env.MIN_GOT_LINKS ||
    pipelineConfig.min_links_for_got ||
    5
  ),
  minLikelyJobLinksForGot: Number(
    process.env.MIN_LIKELY_JOB_LINKS_FOR_GOT ||
    pipelineConfig.min_likely_job_links_for_got ||
    1
  ),
  expandExternalJobBoardLinks: parseBoolean(
    process.env.EXPAND_EXTERNAL_JOB_BOARD_LINKS,
    pipelineConfig.expand_external_job_board_links !== undefined
      ? pipelineConfig.expand_external_job_board_links
      : true
  ),
  jobBoardExpansionMaxSeeds: Number(
    process.env.JOB_BOARD_EXPANSION_MAX_SEEDS ||
    pipelineConfig.job_board_expansion_max_seeds ||
    1
  ),
  filterNonJobLinksByAnchorText: parseBoolean(
    process.env.FILTER_NON_JOB_LINKS_BY_ANCHOR_TEXT,
    pipelineConfig.filter_non_job_links_by_anchor_text !== undefined
      ? pipelineConfig.filter_non_job_links_by_anchor_text
      : true
  ),
  filterCareerLinks: parseBoolean(
    process.env.FILTER_CAREER_LINKS,
    pipelineConfig.filter_career_links !== undefined
      ? pipelineConfig.filter_career_links
      : true
  ),
  filterAtsLinks: parseBoolean(
    process.env.FILTER_ATS_LINKS,
    pipelineConfig.filter_ats_links !== undefined
      ? pipelineConfig.filter_ats_links
      : true
  ),
  linksFetchMode: parseFetchMode(
    process.env.LINKS_FETCH_MODE ||
      pipelineConfig.links_fetch_mode ||
      "auto",
    "auto"
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
  nonJobAnchorTextPatterns:
    pipelineConfig.non_job_anchor_text_patterns ||
    (process.env.NON_JOB_ANCHOR_TEXT_PATTERNS
      ? process.env.NON_JOB_ANCHOR_TEXT_PATTERNS.split("|")
      : undefined),
  positiveJobAnchorTextPatterns:
    pipelineConfig.positive_job_anchor_text_patterns ||
    (process.env.POSITIVE_JOB_ANCHOR_TEXT_PATTERNS
      ? process.env.POSITIVE_JOB_ANCHOR_TEXT_PATTERNS.split("|")
      : undefined),
  enableExpireKeywordDetection: parseBoolean(
    process.env.ENABLE_EXPIRE_KEYWORD_DETECTION,
    pipelineConfig.enable_expire_keyword_detection !== undefined
      ? pipelineConfig.enable_expire_keyword_detection
      : true
  ),
  expireKeywordMatchLimit: Number(
    process.env.EXPIRE_KEYWORD_MATCH_LIMIT ||
      pipelineConfig.expire_keyword_match_limit ||
      8
  ),
  expireDetectionTextLimit: Number(
    process.env.EXPIRE_DETECTION_TEXT_LIMIT ||
      pipelineConfig.expire_detection_text_limit ||
      60000
  ),
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

async function parkFailedStreamMessage(redisClient, streamKey, value, context = {}) {
  await streamAdd(redisClient, streamKey, value);
  await incrementHealthCounter(redisClient, "failed_parked_total", 1);
  await incrementHealthCounter(
    redisClient,
    `${streamKey}:failed_parked`,
    1
  );
  log("Parked failed stream message.", {
    stream: streamKey,
    ...context
  });
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
    if (urlObj.pathname && urlObj.pathname !== "/") {
      urlObj.pathname = urlObj.pathname.replace(/\/+$/, "");
    }
    if (
      (urlObj.protocol === "http:" && urlObj.port === "80") ||
      (urlObj.protocol === "https:" && urlObj.port === "443")
    ) {
      urlObj.port = "";
    }
    return urlObj.toString();
  } catch (error) {
    return String(link || "").trim();
  }
}

function buildLinkDedupeKey(link) {
  const normalized = normalizeLink(link);
  if (!normalized) {
    return "";
  }
  try {
    const parsed = new URL(normalized);
    const host = parsed.hostname.toLowerCase();
    const port = parsed.port ? `:${parsed.port}` : "";
    const pathname =
      parsed.pathname && parsed.pathname !== "/"
        ? parsed.pathname.replace(/\/+$/, "")
        : parsed.pathname || "/";
    return `${host}${port}${pathname}${parsed.search || ""}${parsed.hash || ""}`;
  } catch (error) {
    return normalized.toLowerCase().replace(/\/+$/, "");
  }
}

function scoreLinkPreference(link) {
  const normalized = normalizeLink(link);
  if (!normalized) {
    return Number.NEGATIVE_INFINITY;
  }
  try {
    const parsed = new URL(normalized);
    let score = 0;
    if (parsed.protocol === "https:") {
      score += 100;
    }
    if (parsed.pathname && parsed.pathname !== "/" && !parsed.pathname.endsWith("/")) {
      score += 10;
    }
    if (!parsed.search) {
      score += 1;
    }
    return score;
  } catch (error) {
    return 0;
  }
}

function selectPreferredLink(current, candidate) {
  const currentNormalized = normalizeLink(current);
  const candidateNormalized = normalizeLink(candidate);
  if (!currentNormalized) {
    return candidateNormalized;
  }
  if (!candidateNormalized) {
    return currentNormalized;
  }
  const currentScore = scoreLinkPreference(currentNormalized);
  const candidateScore = scoreLinkPreference(candidateNormalized);
  if (candidateScore > currentScore) {
    return candidateNormalized;
  }
  if (candidateScore < currentScore) {
    return currentNormalized;
  }
  if (candidateNormalized.length < currentNormalized.length) {
    return candidateNormalized;
  }
  if (candidateNormalized.length > currentNormalized.length) {
    return currentNormalized;
  }
  return candidateNormalized < currentNormalized
    ? candidateNormalized
    : currentNormalized;
}

function dedupeLinksPreferHttps(links) {
  if (!Array.isArray(links) || !links.length) {
    return [];
  }
  const buckets = new Map();
  for (const link of links) {
    const normalized = normalizeLink(link);
    if (!normalized) {
      continue;
    }
    const key = buildLinkDedupeKey(normalized);
    if (!key) {
      continue;
    }
    const current = buckets.get(key);
    buckets.set(key, selectPreferredLink(current, normalized));
  }
  return Array.from(buckets.values()).filter(Boolean);
}

function dedupeLinkEntriesPreferHttps(linkEntries) {
  if (!Array.isArray(linkEntries) || !linkEntries.length) {
    return [];
  }
  const buckets = new Map();
  for (const entry of linkEntries) {
    const normalizedUrl = normalizeLink(entry && entry.url);
    if (!normalizedUrl) {
      continue;
    }
    const key = buildLinkDedupeKey(normalizedUrl);
    if (!key) {
      continue;
    }
    const text = String((entry && entry.text) || "").trim();
    if (!buckets.has(key)) {
      buckets.set(key, {
        url: normalizedUrl,
        texts: new Set()
      });
    }
    const bucket = buckets.get(key);
    bucket.url = selectPreferredLink(bucket.url, normalizedUrl);
    if (text) {
      bucket.texts.add(text);
    }
  }

  const deduped = [];
  for (const bucket of buckets.values()) {
    if (!bucket.texts.size) {
      deduped.push({
        url: bucket.url,
        text: ""
      });
      continue;
    }
    for (const text of bucket.texts.values()) {
      deduped.push({
        url: bucket.url,
        text
      });
    }
  }
  return deduped;
}

function buildCareerLinksDocumentFilter(urlCandidates = []) {
  const aliasesSet = new Set(
    (urlCandidates || [])
      .map((candidate) => normalizeLink(candidate))
      .filter(Boolean)
  );
  for (const alias of Array.from(aliasesSet)) {
    try {
      const parsed = new URL(alias);
      if (!/^https?:$/i.test(parsed.protocol)) {
        continue;
      }
      const httpVariant = new URL(parsed.toString());
      httpVariant.protocol = "http:";
      aliasesSet.add(normalizeLink(httpVariant.toString()));

      const httpsVariant = new URL(parsed.toString());
      httpsVariant.protocol = "https:";
      aliasesSet.add(normalizeLink(httpsVariant.toString()));
    } catch (error) {
      // ignore non-URL values
    }
  }
  const aliases = Array.from(aliasesSet).filter(Boolean);
  const keyCandidates = Array.from(
    new Set(aliases.map((url) => buildLinkDedupeKey(url)).filter(Boolean))
  );
  const clauses = [];
  if (keyCandidates.length) {
    clauses.push({ careerUrlKey: { $in: keyCandidates } });
  }
  if (aliases.length) {
    clauses.push({ careerUrl: { $in: aliases } });
  }
  if (!clauses.length) {
    return {};
  }
  if (clauses.length === 1) {
    return clauses[0];
  }
  return { $or: clauses };
}

function buildSelfLinkKeySet(urlCandidates = []) {
  const keys = new Set();
  const normalizedCandidates = Array.from(
    new Set(
      (urlCandidates || [])
        .map((candidate) => normalizeLink(candidate))
        .filter(Boolean)
    )
  );
  for (const candidate of normalizedCandidates) {
    const key = buildLinkDedupeKey(candidate);
    if (key) {
      keys.add(key);
    }
    try {
      const parsed = new URL(candidate);
      if (/^https?:$/i.test(parsed.protocol)) {
        const httpVariant = new URL(parsed.toString());
        httpVariant.protocol = "http:";
        const httpKey = buildLinkDedupeKey(httpVariant.toString());
        if (httpKey) {
          keys.add(httpKey);
        }
        const httpsVariant = new URL(parsed.toString());
        httpsVariant.protocol = "https:";
        const httpsKey = buildLinkDedupeKey(httpsVariant.toString());
        if (httpsKey) {
          keys.add(httpsKey);
        }
      }
    } catch (error) {
      // ignore invalid URL variants
    }
  }
  return keys;
}

function isAtsCareerLink(link) {
  return isAtsCareerLinkFromRules(link);
}

function extractAtsCareerLinks(links) {
  return dedupeLinksPreferHttps(extractAtsCareerLinksFromRules(links));
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
      if (baseHost === host) {
        return true;
      }
      if (settings.keepExternalLikelyJobLinks) {
        if (isLikelyJobLink(link)) {
          return true;
        }
        if (isJobBoardLandingLink(link)) {
          return true;
        }
        if (isAtsCareerLink(link)) {
          return true;
        }
      }
      return false;
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

function applyAnchorTextFilter(links, linkEntries) {
  if (!settings.filterNonJobLinksByAnchorText) {
    return {
      links: dedupeLinksPreferHttps(links),
      dropped: []
    };
  }
  const result = filterLinksByAnchorText(links, linkEntries, {
    nonJobPatterns: settings.nonJobAnchorTextPatterns,
    positivePatterns: settings.positiveJobAnchorTextPatterns
  });
  const forcedKeep = (links || []).filter(
    (link) => isLikelyJobLink(link) || isAtsCareerLink(link)
  );
  return {
    links: dedupeLinksPreferHttps([...(result.links || []), ...forcedKeep]),
    dropped: result.dropped || []
  };
}

function applyCareerLinksFilter(links, forcedLinks = []) {
  const uniqueInput = dedupeLinksPreferHttps((links || []).filter(Boolean));
  const uniqueForced = dedupeLinksPreferHttps(
    (forcedLinks || []).filter(Boolean)
  );
  if (!settings.filterCareerLinks) {
    return {
      links: dedupeLinksPreferHttps([...uniqueInput, ...uniqueForced]),
      dropped: []
    };
  }
  const result = filterCareerLinksFromRules(uniqueInput, {
    excludePatterns: settings.jobLinkExcludePatterns,
    forceInclude: uniqueForced
  });
  return {
    links: dedupeLinksPreferHttps([...(result.links || []), ...uniqueForced]),
    dropped: result.dropped || []
  };
}

function applyAtsLinksFilter(links) {
  const uniqueInput = dedupeLinksPreferHttps((links || []).filter(Boolean));
  if (!settings.filterAtsLinks) {
    return {
      links: uniqueInput,
      dropped: []
    };
  }
  const result = filterAtsCareerLinksFromRules(uniqueInput, {
    excludePatterns: settings.jobLinkExcludePatterns
  });
  return {
    links: dedupeLinksPreferHttps(result.links || []),
    dropped: result.dropped || []
  };
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

function isBotChallengeLink(link) {
  if (!link) {
    return false;
  }
  const lower = String(link).toLowerCase();
  return (
    lower.includes("/.well-known/sgcaptcha") ||
    lower.includes("sgcaptcha") ||
    lower.includes("cf-chl") ||
    lower.includes("challenge-platform") ||
    lower.includes("_incapsula_resource") ||
    lower.includes("distil_r_captcha") ||
    lower.includes("/captcha")
  );
}

function isLikelyBotChallengeResult(links, jobLinks) {
  if (!Array.isArray(links) || !links.length) {
    return false;
  }
  if (Array.isArray(jobLinks) && jobLinks.length) {
    return false;
  }
  const challengeCount = links.reduce(
    (count, link) => count + (isBotChallengeLink(link) ? 1 : 0),
    0
  );
  if (challengeCount <= 0) {
    return false;
  }
  if (links.length <= 3) {
    return true;
  }
  const nonChallengeCount = links.length - challengeCount;
  return nonChallengeCount <= 0;
}

function isExternalLinkToCareer(link, careerUrl) {
  try {
    const linkHost = new URL(link).hostname.replace(/^www\./i, "").toLowerCase();
    const careerHost = new URL(careerUrl)
      .hostname
      .replace(/^www\./i, "")
      .toLowerCase();
    return Boolean(linkHost && careerHost && linkHost !== careerHost);
  } catch (error) {
    return false;
  }
}

const JOB_DETAIL_LINK_REGEX =
  /\/job\/|\/jobs\/details\/|jobdetails|jobintroduction\.action|jobid=|job_id=|gh_jid=|jid=|requisition|req=|positionid=|postingid=|\/apply\/jobs\/details\//i;

function isJobDetailLikeLink(link) {
  if (!link) {
    return false;
  }
  return JOB_DETAIL_LINK_REGEX.test(String(link));
}

function countJobDetailLinks(links) {
  if (!Array.isArray(links) || !links.length) {
    return 0;
  }
  let count = 0;
  for (const link of links) {
    if (isJobDetailLikeLink(link)) {
      count += 1;
    }
  }
  return count;
}

function buildJobDetailLinkCountExpression(fieldPath = "$jobLinks") {
  return {
    $size: {
      $filter: {
        input: fieldPath,
        as: "link",
        cond: {
          $regexMatch: {
            input: "$$link",
            regex: JOB_DETAIL_LINK_REGEX
          }
        }
      }
    }
  };
}

function isJobBoardLandingLink(link) {
  if (!link) {
    return false;
  }
  if (isAtsCareerLink(link)) {
    return true;
  }
  const lower = String(link).toLowerCase();
  if (/careerhome\.action|searchjobs|jobsearch|search\.aspx|\/jobs\/?$|\/jobs\?|\/career\/?$|\/career\?|\/careers\/?$|\/careers\?/.test(lower)) {
    return true;
  }
  try {
    const parsed = new URL(link);
    const host = parsed.hostname.toLowerCase();
    const pathname = parsed.pathname.toLowerCase();
    if (host.includes("myworkdayjobs.com") && !isJobDetailLikeLink(link)) {
      return true;
    }
    if (
      host.includes("oraclecloud.com") &&
      /\/hcmui\/candidateexperience\/[^/]+\/sites\/[^/]+\/?$/.test(pathname)
    ) {
      return true;
    }
    if (
      host.includes("oraclecloud.com") &&
      /\/hcmui\/candidateexperience\/[^/]+\/sites\/[^/]+\/jobs\/?$/.test(
        pathname
      )
    ) {
      return true;
    }
  } catch (error) {
    return false;
  }
  return false;
}

async function expandExternalJobBoardJobLinks(careerUrl, jobLinks, candidateLinks = []) {
  if (!settings.expandExternalJobBoardLinks) {
    return [];
  }
  if (!Array.isArray(jobLinks)) {
    return [];
  }
  if (jobLinks.some((link) => isJobDetailLikeLink(link))) {
    return [];
  }

  const maxSeeds = Number.isFinite(Number(settings.jobBoardExpansionMaxSeeds))
    ? Math.max(0, Math.floor(Number(settings.jobBoardExpansionMaxSeeds)))
    : 1;
  if (maxSeeds <= 0) {
    return [];
  }

  const seedCandidates = dedupeLinksPreferHttps([
    ...(Array.isArray(jobLinks) ? jobLinks : []),
    ...(Array.isArray(candidateLinks) ? candidateLinks : [])
  ]);
  const seedLinks = dedupeLinksPreferHttps(
    seedCandidates.filter(
      (link) =>
        isExternalLinkToCareer(link, careerUrl) && isJobBoardLandingLink(link)
    )
  ).slice(0, maxSeeds);
  if (!seedLinks.length) {
    return [];
  }

  const expanded = new Set();
  const fetchedSeeds = new Set();
  const fetchSeedJobLinks = async (seedUrl) => {
    if (!seedUrl || fetchedSeeds.has(seedUrl)) {
      return [];
    }
    fetchedSeeds.add(seedUrl);
    const result = await fetchCareerLinks(seedUrl);
    const seedLinksRaw = Array.isArray(result.links) ? result.links : [];
    return dedupeLinksPreferHttps(
      filterJobLinks(dedupeLinksPreferHttps(seedLinksRaw), {
        strongPatterns: settings.jobLinkStrongPatterns,
        weakPatterns: settings.jobLinkWeakPatterns,
        excludePatterns: settings.jobLinkExcludePatterns
      })
    );
  };

  for (const seedUrl of seedLinks) {
    try {
      const seedJobLinks = await fetchSeedJobLinks(seedUrl);
      for (const link of seedJobLinks) {
        expanded.add(link);
      }
      if (!seedJobLinks.some((link) => isJobDetailLikeLink(link))) {
        const listingLinks = seedJobLinks
          .filter((link) => isJobBoardLandingLink(link))
          .slice(0, maxSeeds);
        for (const listingUrl of listingLinks) {
          try {
            const listingJobLinks = await fetchSeedJobLinks(listingUrl);
            for (const listingJobLink of listingJobLinks) {
              expanded.add(listingJobLink);
            }
          } catch (listingError) {
            log("External job board listing expansion failed.", {
              careerUrl,
              seedUrl,
              listingUrl,
              error: listingError.toString()
            });
          }
        }
      }
    } catch (error) {
      log("External job board expansion failed.", {
        careerUrl,
        seedUrl,
        error: error.toString()
      });
    }
  }

  const current = new Set(dedupeLinksPreferHttps(jobLinks).map(buildLinkDedupeKey));
  return dedupeLinksPreferHttps(Array.from(expanded)).filter(
    (link) => !current.has(buildLinkDedupeKey(link))
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
        name: collections.careerLinks,
        key: { careerUrlKey: 1 },
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

function resolveCareerLinksStatus({
  hasJobDetailLinks,
  hasAtsCareerLinks,
  isExpiredOrNoJobs,
  excludedDomainPattern
}) {
  if (excludedDomainPattern) {
    return "excluded_domain";
  }
  if (hasJobDetailLinks) {
    return "job_links_found";
  }
  if (isExpiredOrNoJobs) {
    return "expired_or_no_jobs";
  }
  if (hasAtsCareerLinks) {
    return "ats_career_links_found";
  }
  return "no_job_links";
}

function buildCareerLinksSuccessUpdate(
  careerUrl,
  links,
  jobLinks,
  atsCareerLinks,
  source,
  userAgent,
  startedAt,
  analysis = {}
) {
  const now = new Date();
  const atsLinks = Array.isArray(atsCareerLinks) ? atsCareerLinks : [];
  const hasJobLinks = jobLinks.length > 0;
  const jobDetailLinkCount = countJobDetailLinks(jobLinks);
  const hasJobDetailLinks = jobDetailLinkCount > 0;
  const hasAtsCareerLinks = atsLinks.length > 0;
  const isExpiredOrNoJobs = Boolean(analysis.isExpiredOrNoJobs);
  const excludedDomainPattern = String(analysis.excludedDomainPattern || "");
  const expireKeywordMatches = Array.isArray(analysis.expireKeywordMatches)
    ? analysis.expireKeywordMatches.slice(0, 20)
    : [];
  const expireKeywordMatchCount = Number(
    analysis.expireKeywordMatchCount || expireKeywordMatches.length || 0
  );
  const statusCode = Number(analysis.statusCode || 0);
  const finalUrl = String(analysis.finalUrl || "");
  const requestUrl = String(analysis.requestUrl || careerUrl);
  const careerUrlKey = String(
    analysis.careerUrlKey || buildLinkDedupeKey(careerUrl) || ""
  );
  const redirectChain = Array.isArray(analysis.redirectChain)
    ? analysis.redirectChain.slice(0, 20).map((step) => ({
      url: String((step && step.url) || ""),
      statusCode: Number((step && step.statusCode) || 0),
      location: String((step && step.location) || "")
    }))
    : [];
  const redirectStatusCodes = (
    Array.isArray(analysis.redirectStatusCodes)
      ? analysis.redirectStatusCodes
      : redirectChain.map((step) => step.statusCode)
  )
    .map((code) => Number(code || 0))
    .filter((code) => Number.isFinite(code) && code > 0)
    .slice(0, 20);
  const redirectCount = Number.isFinite(Number(analysis.redirectCount))
    ? Math.max(0, Math.floor(Number(analysis.redirectCount)))
    : Math.max(0, redirectChain.length - 1);
  const redirected =
    Boolean(analysis.redirected) ||
    redirectCount > 0 ||
    (Boolean(finalUrl) && normalizeLink(finalUrl) !== normalizeLink(careerUrl));
  const initialHttpStatusCode = redirectStatusCodes.length
    ? redirectStatusCodes[0]
    : statusCode;
  const pageTitle = String(analysis.pageTitle || "").slice(0, 300);
  const textFilteredLinkCount = Number(analysis.textFilteredLinkCount || 0);
  const careerFilteredLinkCount = Number(analysis.careerFilteredLinkCount || 0);
  const atsFilteredLinkCount = Number(analysis.atsFilteredLinkCount || 0);
  const jobLinksStatus = hasJobLinks ? "job_links_found" : "no_job_links";
  const careerLinksStatus = resolveCareerLinksStatus({
    hasJobDetailLinks,
    hasAtsCareerLinks,
    isExpiredOrNoJobs,
    excludedDomainPattern
  });
  return {
    $set: {
      careerUrl,
      careerUrlKey,
      requestUrl,
      links,
      linkCount: links.length,
      jobLinks,
      jobLinkCount: jobLinks.length,
      hasJobLinks,
      jobDetailLinkCount,
      hasJobDetailLinks,
      jobLinksStatus,
      atsCareerLinks: atsLinks,
      atsCareerLinkCount: atsLinks.length,
      hasAtsCareerLinks,
      careerLinksStatus,
      expiredOrNoJobs: isExpiredOrNoJobs,
      expireKeywordMatches,
      expireKeywordMatchCount,
      excludedDomainPattern,
      textFilteredLinkCount,
      careerFilteredLinkCount,
      atsFilteredLinkCount,
      initialHttpStatusCode,
      httpStatusCode: statusCode,
      redirectChain,
      redirectStatusCodes,
      redirectCount,
      redirected,
      finalUrl: finalUrl || careerUrl,
      pageTitle,
      crawlStatus: "success",
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

function buildCareerLinksExcludedUpdate(
  careerUrl,
  excludedDomainPattern,
  startedAt,
  options = {}
) {
  const now = new Date();
  const requestUrl = String(options.requestUrl || careerUrl);
  const finalUrl = String(options.finalUrl || careerUrl);
  const statusCode = Number(options.statusCode || 0);
  const redirectChain = Array.isArray(options.redirectChain)
    ? options.redirectChain.slice(0, 20).map((step) => ({
      url: String((step && step.url) || ""),
      statusCode: Number((step && step.statusCode) || 0),
      location: String((step && step.location) || "")
    }))
    : [];
  const redirectStatusCodes = (
    Array.isArray(options.redirectStatusCodes)
      ? options.redirectStatusCodes
      : redirectChain.map((step) => step.statusCode)
  )
    .map((code) => Number(code || 0))
    .filter((code) => Number.isFinite(code) && code > 0)
    .slice(0, 20);
  const redirectCount = Number.isFinite(Number(options.redirectCount))
    ? Math.max(0, Math.floor(Number(options.redirectCount)))
    : Math.max(0, redirectChain.length - 1);
  const redirected =
    Boolean(options.redirected) ||
    redirectCount > 0 ||
    normalizeLink(finalUrl) !== normalizeLink(careerUrl);
  return {
    $set: {
      careerUrl,
      careerUrlKey: String(options.careerUrlKey || buildLinkDedupeKey(careerUrl) || ""),
      requestUrl,
      links: [],
      linkCount: 0,
      jobLinks: [],
      jobLinkCount: 0,
      jobDetailLinkCount: 0,
      hasJobLinks: false,
      hasJobDetailLinks: false,
      atsCareerLinks: [],
      atsCareerLinkCount: 0,
      hasAtsCareerLinks: false,
      expiredOrNoJobs: false,
      expireKeywordMatches: [],
      expireKeywordMatchCount: 0,
      excludedDomainPattern: String(excludedDomainPattern || ""),
      textFilteredLinkCount: 0,
      careerFilteredLinkCount: 0,
      atsFilteredLinkCount: 0,
      initialHttpStatusCode: redirectStatusCodes.length
        ? redirectStatusCodes[0]
        : statusCode,
      httpStatusCode: statusCode,
      redirectChain,
      redirectStatusCodes,
      redirectCount,
      redirected,
      finalUrl: finalUrl || careerUrl,
      crawlStatus: "success",
      jobLinksStatus: "no_job_links",
      careerLinksStatus: "excluded_domain",
      source: "rules",
      userAgent: null,
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

function buildCareerLinksErrorUpdate(careerUrl, error, startedAt, options = {}) {
  const now = new Date();
  const requestUrl = String(options.requestUrl || careerUrl);
  const finalUrl = String(options.finalUrl || careerUrl);
  const statusCode = Number(options.statusCode || 0);
  const redirectChain = Array.isArray(options.redirectChain)
    ? options.redirectChain.slice(0, 20).map((step) => ({
      url: String((step && step.url) || ""),
      statusCode: Number((step && step.statusCode) || 0),
      location: String((step && step.location) || "")
    }))
    : [];
  const redirectStatusCodes = (
    Array.isArray(options.redirectStatusCodes)
      ? options.redirectStatusCodes
      : redirectChain.map((step) => step.statusCode)
  )
    .map((code) => Number(code || 0))
    .filter((code) => Number.isFinite(code) && code > 0)
    .slice(0, 20);
  const redirectCount = Number.isFinite(Number(options.redirectCount))
    ? Math.max(0, Math.floor(Number(options.redirectCount)))
    : Math.max(0, redirectChain.length - 1);
  const redirected =
    Boolean(options.redirected) ||
    redirectCount > 0 ||
    normalizeLink(finalUrl) !== normalizeLink(careerUrl);
  if (settings.preserveLinksOnError) {
    return {
      $set: {
        careerUrl,
        careerUrlKey: String(options.careerUrlKey || buildLinkDedupeKey(careerUrl) || ""),
        requestUrl,
        initialHttpStatusCode: redirectStatusCodes.length
          ? redirectStatusCodes[0]
          : statusCode,
        httpStatusCode: statusCode,
        redirectChain,
        redirectStatusCodes,
        redirectCount,
        redirected,
        finalUrl: finalUrl || careerUrl,
        crawlStatus: "error",
        jobLinksStatus: "error",
        careerLinksStatus: "error",
        expiredOrNoJobs: false,
        expireKeywordMatches: [],
        expireKeywordMatchCount: 0,
        excludedDomainPattern: "",
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
        jobDetailLinkCount: 0,
        hasJobDetailLinks: false,
        atsCareerLinks: [],
        atsCareerLinkCount: 0,
        hasAtsCareerLinks: false,
        textFilteredLinkCount: 0,
        careerFilteredLinkCount: 0,
        atsFilteredLinkCount: 0,
        createdAt: now
      }
    };
  }
  return {
    $set: {
      careerUrl,
      careerUrlKey: String(options.careerUrlKey || buildLinkDedupeKey(careerUrl) || ""),
      requestUrl,
      links: [],
      linkCount: 0,
      jobLinks: [],
      jobLinkCount: 0,
      jobDetailLinkCount: 0,
      atsCareerLinks: [],
      atsCareerLinkCount: 0,
      hasJobLinks: false,
      hasJobDetailLinks: false,
      hasAtsCareerLinks: false,
      expiredOrNoJobs: false,
      expireKeywordMatches: [],
      expireKeywordMatchCount: 0,
      excludedDomainPattern: "",
      textFilteredLinkCount: 0,
      careerFilteredLinkCount: 0,
      atsFilteredLinkCount: 0,
      initialHttpStatusCode: redirectStatusCodes.length
        ? redirectStatusCodes[0]
        : statusCode,
      httpStatusCode: statusCode,
      redirectChain,
      redirectStatusCodes,
      redirectCount,
      redirected,
      finalUrl: finalUrl || careerUrl,
      crawlStatus: "error",
      jobLinksStatus: "error",
      careerLinksStatus: "error",
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
  const uniqueJobLinks = dedupeLinksPreferHttps(jobLinks);
  if (!uniqueJobLinks.length) {
    return 0;
  }
  let total = 0;
  const batches = chunkArray(uniqueJobLinks, settings.mongoBulkWriteBatchSize);
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
  const uniqueJobLinks = dedupeLinksPreferHttps(jobLinks);
  if (!uniqueJobLinks.length) {
    return 0;
  }
  let enqueued = 0;
  const entries = uniqueJobLinks.map((jobUrl) => ({
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

function normalizeFetchedLinksResult(result, fallbackUrl = "") {
  const linkEntries = dedupeLinkEntriesPreferHttps(
    Array.isArray(result && result.linkEntries) ? result.linkEntries : []
  );
  const links = dedupeLinksPreferHttps([
    ...(Array.isArray(result && result.links) ? result.links : []),
    ...linkEntries.map((entry) => entry.url)
  ]);
  const finalUrlRaw = (result && result.finalUrl) || fallbackUrl;
  const finalUrl = normalizeLink(finalUrlRaw) || finalUrlRaw;
  const redirectChain = Array.isArray(result && result.redirectChain)
    ? result.redirectChain
      .map((step) => ({
        url: normalizeLink(step && step.url) || String((step && step.url) || ""),
        statusCode: Number((step && step.statusCode) || 0),
        location: String((step && step.location) || "")
      }))
      .filter((step) => Boolean(step.url) || step.statusCode > 0)
    : [];
  const redirectStatusCodes = (
    Array.isArray(result && result.redirectStatusCodes)
      ? result.redirectStatusCodes
      : redirectChain.map((step) => step.statusCode)
  )
    .map((code) => Number(code || 0))
    .filter((code) => Number.isFinite(code) && code > 0);
  const redirectCount = Number.isFinite(Number(result && result.redirectCount))
    ? Math.max(0, Math.floor(Number(result.redirectCount)))
    : Math.max(0, redirectChain.length - 1);
  const redirected =
    Boolean(result && result.redirected) ||
    redirectCount > 0 ||
    (Boolean(finalUrl) && normalizeLink(finalUrl) !== normalizeLink(fallbackUrl));
  return {
    ...(result || {}),
    links,
    linkEntries,
    finalUrl,
    redirectChain,
    redirectStatusCodes,
    redirectCount,
    redirected
  };
}

async function fetchCareerLinks(url) {
  const buildGotLinksResult = (gotResult) => {
    const rawLinkEntries = extractLinkEntriesFromHtml(gotResult.html, url, {
      sameDomainOnly: false
    });
    const linkEntries = dedupeLinkEntriesPreferHttps(rawLinkEntries);
    const links = dedupeLinksPreferHttps(
      linkEntries.map((entry) => entry.url).filter(Boolean)
    );
    const pageSignals = extractPageTextSampleFromHtml(gotResult.html, {
      maxLength: settings.expireDetectionTextLimit
    });
    return {
      links,
      linkEntries,
      source: "got",
      userAgent: gotResult.userAgent,
      statusCode: gotResult.statusCode || 0,
      finalUrl: normalizeLink(gotResult.finalUrl || url) || gotResult.finalUrl || url,
      redirectChain: Array.isArray(gotResult.redirectChain)
        ? gotResult.redirectChain
        : [],
      redirectStatusCodes: Array.isArray(gotResult.redirectStatusCodes)
        ? gotResult.redirectStatusCodes
        : [],
      redirectCount: Number(gotResult.redirectCount || 0),
      redirected: Boolean(gotResult.redirected),
      pageTitle: pageSignals.title || "",
      pageTextSample: pageSignals.textSample || ""
    };
  };

  if (settings.linksFetchMode === "puppeteer") {
    const puppeteerResult = await fetchLinksWithPuppeteer(url, {
      timeoutMs: settings.puppeteerTimeoutMs,
      pageTextLimit: settings.expireDetectionTextLimit
    });
    return normalizeFetchedLinksResult(
      {
      ...puppeteerResult,
      finalUrl: puppeteerResult.finalUrl || url
      },
      url
    );
  }

  if (settings.linksFetchMode === "got") {
    const gotResult = await fetchHtmlWithGot(url, {
      timeoutMs: settings.gotTimeoutMs
    });
    return normalizeFetchedLinksResult(buildGotLinksResult(gotResult), url);
  }

  let gotResult = null;
  try {
    gotResult = await fetchHtmlWithGot(url, {
      timeoutMs: settings.gotTimeoutMs
    });
    const gotLinksResult = buildGotLinksResult(gotResult);
    const links = gotLinksResult.links;
    const likelyJobLinks = filterJobLinks(links, {
      strongPatterns: settings.jobLinkStrongPatterns,
      weakPatterns: settings.jobLinkWeakPatterns,
      excludePatterns: settings.jobLinkExcludePatterns
    });
    const hasEnoughLikelyJobs =
      settings.minLikelyJobLinksForGot <= 0 ||
      likelyJobLinks.length >= settings.minLikelyJobLinksForGot;
    if (Number(gotLinksResult.statusCode || 0) >= 400) {
      return normalizeFetchedLinksResult(gotLinksResult, url);
    }
    if (links.length >= settings.minLinksForGot && hasEnoughLikelyJobs) {
      return normalizeFetchedLinksResult(gotLinksResult, url);
    }
    log("Got did not meet link quality threshold, falling back to puppeteer.", {
      url,
      count: links.length,
      likelyJobLinkCount: likelyJobLinks.length,
      minLinksForGot: settings.minLinksForGot,
      minLikelyJobLinksForGot: settings.minLikelyJobLinksForGot
    });
  } catch (error) {
    log("Got failed, falling back to puppeteer.", {
      url,
      error: error.toString()
    });
  }

  const puppeteerResult = await fetchLinksWithPuppeteer(url, {
    timeoutMs: settings.puppeteerTimeoutMs,
    pageTextLimit: settings.expireDetectionTextLimit
  });
  const normalizedPuppeteerResult = normalizeFetchedLinksResult(
    {
      ...puppeteerResult,
      finalUrl: puppeteerResult.finalUrl || url
    },
    url
  );
  const initialLinks = Array.isArray(normalizedPuppeteerResult.links)
    ? normalizedPuppeteerResult.links
    : [];
  const initialLikelyJobLinks = filterJobLinks(initialLinks, {
    strongPatterns: settings.jobLinkStrongPatterns,
    weakPatterns: settings.jobLinkWeakPatterns,
    excludePatterns: settings.jobLinkExcludePatterns
  }).length;
  if (initialLinks.length > 1 && initialLikelyJobLinks > 0) {
    return normalizedPuppeteerResult;
  }
  try {
    const retryResult = await fetchLinksWithPuppeteer(url, {
      timeoutMs: settings.puppeteerTimeoutMs,
      pageTextLimit: settings.expireDetectionTextLimit
    });
    const normalizedRetryResult = normalizeFetchedLinksResult(
      {
        ...retryResult,
        finalUrl: retryResult.finalUrl || url
      },
      url
    );
    const retryLinks = Array.isArray(normalizedRetryResult.links)
      ? normalizedRetryResult.links
      : [];
    const retryLikelyJobLinks = filterJobLinks(retryLinks, {
      strongPatterns: settings.jobLinkStrongPatterns,
      weakPatterns: settings.jobLinkWeakPatterns,
      excludePatterns: settings.jobLinkExcludePatterns
    }).length;
    const retryIsBetter =
      retryLikelyJobLinks > initialLikelyJobLinks ||
      (retryLikelyJobLinks === initialLikelyJobLinks &&
        retryLinks.length > initialLinks.length);
    if (retryIsBetter) {
      log("Puppeteer retry returned more links.", {
        url,
        initialCount: initialLinks.length,
        retryCount: retryLinks.length,
        initialLikelyJobLinks,
        retryLikelyJobLinks
      });
      return normalizedRetryResult;
    }
  } catch (error) {
    log("Puppeteer retry failed.", {
      url,
      error: error.toString()
    });
  }
  return normalizedPuppeteerResult;
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
  const urls = dedupeLinksPreferHttps(
    content
    .split(/\\r?\\n/)
    .map((line) => line.trim())
    .filter(Boolean)
  );
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
    const urls = dedupeLinksPreferHttps(validDocs.map((doc) => doc[fieldName]));
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
          const rawUrl = decodedMessage.value;
          const url = normalizeLink(rawUrl);
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
          const sourceCandidateUrls = Array.from(
            new Set([rawUrl, url].map((candidate) => normalizeLink(candidate)).filter(Boolean))
          );
          const sourceFilter = buildCareerLinksDocumentFilter(sourceCandidateUrls);

          let shouldAck = false;
          const startedAt = new Date();
          try {
            const excludedSourceDomain = isExcludedDomain(url);
            if (excludedSourceDomain.excluded) {
              await collection.updateOne(
                sourceFilter,
                buildCareerLinksExcludedUpdate(
                  url,
                  excludedSourceDomain.pattern,
                  startedAt,
                  {
                    requestUrl: rawUrl,
                    careerUrlKey: buildLinkDedupeKey(url),
                    finalUrl: url
                  }
                ),
                { upsert: true }
              );
              processed += 1;
              log("Career url excluded by domain rules.", {
                url,
                excludedDomainPattern: excludedSourceDomain.pattern,
                retryCount: decodedMessage.retryCount
              });
              shouldAck = true;
              return;
            }

            const {
              links,
              linkEntries,
              source,
              userAgent,
              statusCode,
              finalUrl,
              redirectChain,
              redirectStatusCodes,
              redirectCount,
              redirected,
              pageTitle,
              pageTextSample
            } = await fetchCareerLinks(url);
            const canonicalCareerUrl = selectPreferredLink(url, finalUrl || url);
            const canonicalCandidateUrls = Array.from(
              new Set(
                [rawUrl, url, canonicalCareerUrl, finalUrl || ""]
                  .map((candidate) => normalizeLink(candidate))
                  .filter(Boolean)
              )
            );
            const canonicalFilter = buildCareerLinksDocumentFilter(
              canonicalCandidateUrls
            );
            const canonicalDocFilter =
              canonicalFilter && Object.keys(canonicalFilter).length
                ? canonicalFilter
                : { careerUrl: canonicalCareerUrl };
            const careerUrlKey = buildLinkDedupeKey(canonicalCareerUrl);
            const normalizedLinkEntries = dedupeLinkEntriesPreferHttps(
              (Array.isArray(linkEntries) ? linkEntries : [])
                .map((entry) => ({
                  url: entry && entry.url,
                  text: String((entry && entry.text) || "").trim()
                }))
                .filter((entry) => Boolean(entry.url))
            );
            const rawNormalizedLinks = dedupeLinksPreferHttps([
              ...(Array.isArray(links) ? links : []),
              ...normalizedLinkEntries.map((entry) => entry.url)
            ]);
            let normalizedLinks = rawNormalizedLinks;
            normalizedLinks = filterSameDomain(normalizedLinks, url);
            normalizedLinks = applySocialMediaFilter(normalizedLinks, url);
            normalizedLinks = dedupeLinksPreferHttps(normalizedLinks);
            const textFilterResult = applyAnchorTextFilter(
              normalizedLinks,
              normalizedLinkEntries
            );
            normalizedLinks = textFilterResult.links;
            let jobLinks = dedupeLinksPreferHttps(
              filterJobLinks(normalizedLinks, {
                strongPatterns: settings.jobLinkStrongPatterns,
                weakPatterns: settings.jobLinkWeakPatterns,
                excludePatterns: settings.jobLinkExcludePatterns
              })
            );
            const expandedJobLinks = await expandExternalJobBoardJobLinks(
              url,
              jobLinks,
              rawNormalizedLinks
            );
            if (expandedJobLinks.length) {
              jobLinks = dedupeLinksPreferHttps([...jobLinks, ...expandedJobLinks]);
              normalizedLinks = dedupeLinksPreferHttps([
                ...normalizedLinks,
                ...expandedJobLinks
              ]);
            }
            let jobDetailLinkCount = countJobDetailLinks(jobLinks);
            let detailJobLinks = dedupeLinksPreferHttps(
              jobLinks.filter((link) => isJobDetailLikeLink(link))
            );
            const rawAtsCareerLinks = extractAtsCareerLinks(
              dedupeLinksPreferHttps([...rawNormalizedLinks, ...normalizedLinks])
            );
            const atsFilterResult = applyAtsLinksFilter(rawAtsCareerLinks);
            let atsCareerLinks = atsFilterResult.links;
            const careerFilterResult = applyCareerLinksFilter(normalizedLinks, [
              canonicalCareerUrl,
              finalUrl || "",
              ...jobLinks,
              ...atsCareerLinks
            ]);
            const selfLinkKeys = buildSelfLinkKeySet([
              rawUrl,
              url,
              canonicalCareerUrl,
              finalUrl || ""
            ]);
            normalizedLinks = dedupeLinksPreferHttps(careerFilterResult.links).filter(
              (link) => !selfLinkKeys.has(buildLinkDedupeKey(link))
            );
            if (isLikelyBotChallengeResult(rawNormalizedLinks, jobLinks)) {
              throw new Error(
                "Bot challenge detected while fetching career page; retrying."
              );
            }
            const finalUrlExcluded = isExcludedDomain(finalUrl || "");
            if (finalUrlExcluded.excluded) {
              normalizedLinks = [];
              jobLinks = [];
              jobDetailLinkCount = 0;
              detailJobLinks = [];
              atsCareerLinks = [];
            } else if (settings.mergeLinksAcrossRuns) {
              const existingDoc = await collection.findOne(canonicalDocFilter, {
                projection: {
                  links: 1,
                  jobLinks: 1,
                  atsCareerLinks: 1
                }
              });
              if (existingDoc) {
                normalizedLinks = dedupeLinksPreferHttps([
                  ...(Array.isArray(existingDoc.links) ? existingDoc.links : []),
                  ...normalizedLinks
                ]);
                jobLinks = dedupeLinksPreferHttps([
                  ...(Array.isArray(existingDoc.jobLinks) ? existingDoc.jobLinks : []),
                  ...jobLinks
                ]);
                atsCareerLinks = dedupeLinksPreferHttps([
                  ...(Array.isArray(existingDoc.atsCareerLinks)
                    ? existingDoc.atsCareerLinks
                    : []),
                  ...atsCareerLinks
                ]);
              }
            }
            normalizedLinks = normalizedLinks.filter(
              (link) => !selfLinkKeys.has(buildLinkDedupeKey(link))
            );
            jobDetailLinkCount = countJobDetailLinks(jobLinks);
            detailJobLinks = dedupeLinksPreferHttps(
              jobLinks.filter((link) => isJobDetailLikeLink(link))
            );
            let expireSignals = {
              isExpiredOrNoJobs: false,
              matchedKeywords: [],
              matchedKeywordCount: 0
            };
            if (
              settings.enableExpireKeywordDetection &&
              !jobDetailLinkCount &&
              !finalUrlExcluded.excluded
            ) {
              expireSignals = detectExpiredOrNoJobs(pageTextSample || "", {
                title: pageTitle || "",
                statusCode: statusCode || 0,
                limit: settings.expireKeywordMatchLimit
              });
            }
            const careerLinksStatus = resolveCareerLinksStatus({
              hasJobDetailLinks: jobDetailLinkCount > 0,
              hasAtsCareerLinks: atsCareerLinks.length > 0,
              isExpiredOrNoJobs: expireSignals.isExpiredOrNoJobs,
              excludedDomainPattern: finalUrlExcluded.excluded
                ? finalUrlExcluded.pattern
                : ""
            });

            await collection.updateOne(
              canonicalDocFilter,
              buildCareerLinksSuccessUpdate(
                canonicalCareerUrl,
                normalizedLinks,
                jobLinks,
                atsCareerLinks,
                source,
                userAgent,
                startedAt,
                {
                  isExpiredOrNoJobs: expireSignals.isExpiredOrNoJobs,
                  expireKeywordMatches: expireSignals.matchedKeywords,
                  expireKeywordMatchCount: expireSignals.matchedKeywordCount,
                  excludedDomainPattern: finalUrlExcluded.excluded
                    ? finalUrlExcluded.pattern
                    : "",
                  statusCode: statusCode || 0,
                  requestUrl: rawUrl,
                  careerUrlKey,
                  finalUrl: finalUrl || canonicalCareerUrl,
                  redirectChain,
                  redirectStatusCodes,
                  redirectCount,
                  redirected,
                  pageTitle: pageTitle || "",
                  textFilteredLinkCount: textFilterResult.dropped.length,
                  careerFilteredLinkCount: careerFilterResult.dropped.length,
                  atsFilteredLinkCount: atsFilterResult.dropped.length
                }
              ),
              { upsert: true }
            );

            const discoveredAt = new Date();
            const persistedCount = await persistDiscoveredJobLinks(
              jobLinksCollection,
              canonicalCareerUrl,
              detailJobLinks,
              discoveredAt
            );
            const queuedCount = await enqueueJobLinks(
              redisClient,
              canonicalCareerUrl,
              detailJobLinks
            );
            if (settings.enqueueHtmlFromLinksWorker && detailJobLinks.length) {
              await jobLinksCollection.updateMany(
                { url: { $in: detailJobLinks } },
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
              url: canonicalCareerUrl,
              sourceUrl: rawUrl,
              statusCode: statusCode || 0,
              finalUrl: finalUrl || canonicalCareerUrl,
              redirectStatusCodes,
              linkCount: normalizedLinks.length,
              jobLinkCount: jobLinks.length,
              jobDetailLinkCount,
              atsCareerLinkCount: atsCareerLinks.length,
              careerLinksStatus,
              expiredOrNoJobs: expireSignals.isExpiredOrNoJobs,
              expireKeywordMatchCount: expireSignals.matchedKeywordCount,
              textFilteredLinkCount: textFilterResult.dropped.length,
              careerFilteredLinkCount: careerFilterResult.dropped.length,
              atsFilteredLinkCount: atsFilterResult.dropped.length,
              expandedJobLinkCount: expandedJobLinks.length,
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
              await parkFailedStreamMessage(
                redisClient,
                queues.careerPagesFailed,
                decodedMessage.value || url,
                {
                  id: message.id,
                  url,
                  retryCount: decodedMessage.retryCount,
                  reason: requeued.reason
                }
              );
              await collection.updateOne(
                sourceFilter,
                buildCareerLinksErrorUpdate(url, error, startedAt, {
                  requestUrl: rawUrl,
                  careerUrlKey: buildLinkDedupeKey(url),
                  finalUrl: url
                }),
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
    const dedupedMap = new Map();
    for (const doc of valid) {
      const normalizedUrl = normalizeLink(doc.url);
      if (!normalizedUrl) {
        continue;
      }
      const dedupeKey = buildLinkDedupeKey(normalizedUrl);
      if (!dedupeKey) {
        continue;
      }
      if (!dedupedMap.has(dedupeKey)) {
        dedupedMap.set(dedupeKey, {
          url: normalizedUrl,
          careerUrl: doc.careerUrl || ""
        });
        continue;
      }
      const existing = dedupedMap.get(dedupeKey);
      existing.url = selectPreferredLink(existing.url, normalizedUrl);
      if (!existing.careerUrl && doc.careerUrl) {
        existing.careerUrl = doc.careerUrl;
      }
    }
    const dedupedValid = Array.from(dedupedMap.values()).filter(
      (doc) => Boolean(doc.url)
    );
    if (!dedupedValid.length) {
      batch = [];
      return;
    }
    let batchQueuedCount = dedupedValid.length;
    if (useDedupe) {
      const entries = dedupedValid.map((doc) => ({
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
      const payloads = dedupedValid.map((doc) =>
        buildQueuePayload(doc.url, doc.careerUrl)
      );
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
      dedupedBatchSize: dedupedValid.length,
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

async function seedFailedCareerPages(args) {
  const redisClient = await createRedisClient(redisUrl);
  const batchSize = toPositiveInt(
    args["batch-size"] || args.batchSize,
    settings.redisEnqueueBatchSize
  );
  const max = Number(args.max || 0);
  const keepFailed = parseBoolean(
    args["keep-failed"] !== undefined ? args["keep-failed"] : args.keepFailed,
    false
  );

  let movedTotal = 0;
  try {
    await ensureStreamGroup(
      redisClient,
      queues.careerPages,
      streamGroups.careerPages
    );

    if (keepFailed) {
      const limit = max > 0 ? Math.min(batchSize, max) : batchSize;
      movedTotal = await streamMoveBatch(
        redisClient,
        queues.careerPagesFailed,
        queues.careerPages,
        limit,
        { deleteSource: false }
      );
      log("Requeued failed links batch (source retained).", {
        moved: movedTotal,
        source: queues.careerPagesFailed,
        target: queues.careerPages
      });
      return;
    }

    while (true) {
      const remaining = max > 0 ? max - movedTotal : batchSize;
      if (remaining <= 0) {
        break;
      }
      const limit = max > 0 ? Math.min(batchSize, remaining) : batchSize;
      const moved = await streamMoveBatch(
        redisClient,
        queues.careerPagesFailed,
        queues.careerPages,
        limit,
        { deleteSource: true }
      );
      if (!moved) {
        break;
      }
      movedTotal += moved;
      log("Requeued failed links batch.", {
        moved,
        totalMoved: movedTotal,
        source: queues.careerPagesFailed,
        target: queues.careerPages
      });
    }
  } finally {
    await redisClient.quit();
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
    failedPagesStream,
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
    getStreamHealthSnapshot(
      redisClient,
      queues.careerPagesFailed,
      streamGroups.careerPages
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
      careerLinks: linksStream,
      careerPagesFailed: failedPagesStream
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
  for (const stream of [
    redis.streams.careerPages,
    redis.streams.careerLinks,
    redis.streams.careerPagesFailed
  ]) {
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
    console.log("  node pipeline/index.js seed:failed-links [--batch-size <n>] [--max <n>] [--keep-failed]");
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

  if (command === "seed:failed-links") {
    await seedFailedCareerPages(args);
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

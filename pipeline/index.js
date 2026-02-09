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
  streamReadGroup,
  streamAutoClaimOne,
  streamAck,
  saddAndStream
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
    "career_html"
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
      : undefined)
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
  )
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
    if (gotResult.html && gotResult.html.length >= settings.minHtmlLength) {
      return {
        html: gotResult.html,
        source: "got",
        userAgent: gotResult.userAgent
      };
    }
    log("Got returned short html, falling back to puppeteer.", {
      url,
      length: gotResult.html ? gotResult.html.length : 0
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
  for (const url of urls) {
    await streamAdd(redisClient, queues.careerPages, url);
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
    const updates = [];
    let pushed = 0;
    for (const doc of batch) {
      const url = doc[fieldName];
      if (!url) {
        continue;
      }
      try {
        await streamAdd(redisClient, queues.careerPages, url);
        updates.push({
          updateOne: {
            filter: { _id: doc._id },
            update: {
              $set: {
                [safeMarkField]: true,
                [safeMarkAtField]: new Date()
              }
            }
          }
        });
        pushed += 1;
      } catch (error) {
        if (updates.length) {
          await collection.bulkWrite(updates, { ordered: false });
        }
        throw error;
      }
    }
    if (updates.length) {
      await collection.bulkWrite(updates, { ordered: false });
    }
    count += pushed;
    log("Seeded mongo batch.", {
      batchCount: pushed,
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
  const consumer = buildConsumerName("links");
  await ensureStreamGroup(
    redisClient,
    queues.careerPages,
    streamGroups.careerPages
  );
  await ensureStreamGroup(
    redisClient,
    queues.careerLinks,
    streamGroups.careerLinks
  );
  let processed = 0;
  let claimStartId = "0-0";
  let lastClaimAt = 0;

  try {
    while (true) {
      let message = null;
      if (
        streamSettings.claimMinIdleMs > 0 &&
        streamSettings.claimIntervalMs > 0
      ) {
        const now = Date.now();
        if (now - lastClaimAt >= streamSettings.claimIntervalMs) {
          lastClaimAt = now;
          const claimResult = await streamAutoClaimOne(
            redisClient,
            queues.careerPages,
            streamGroups.careerPages,
            consumer,
            streamSettings.claimMinIdleMs,
            claimStartId,
            streamSettings.claimCount
          );
          if (claimResult) {
            claimStartId = claimResult.nextId || claimStartId;
            message = claimResult.message;
          }
        }
      }
      if (!message) {
        message = await streamReadGroup(
          redisClient,
          queues.careerPages,
          streamGroups.careerPages,
          consumer,
          streamSettings.blockMs,
          streamSettings.readCount
        );
      }
      if (!message) {
        if (args.once) {
          break;
        }
        continue;
      }
      if (!message.value) {
        await streamAck(
          redisClient,
          queues.careerPages,
          streamGroups.careerPages,
          message.id
        );
        continue;
      }
      const url = message.value;
      if (!url) {
        await streamAck(
          redisClient,
          queues.careerPages,
          streamGroups.careerPages,
          message.id
        );
        continue;
      }

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
          {
            $set: {
              careerUrl: url,
              links: normalizedLinks,
              linkCount: normalizedLinks.length,
              jobLinks,
              jobLinkCount: jobLinks.length,
              source,
              userAgent,
              fetchedAt: new Date(),
              startedAt
            }
          },
          { upsert: true }
        );

        for (const link of jobLinks) {
          const payload = JSON.stringify({ url: link, careerUrl: url });
          await saddAndStream(
            redisClient,
            queues.careerLinksDedup,
            queues.careerLinks,
            link,
            payload
          );
        }

        processed += 1;
        log("Career links fetched.", {
          url,
          linkCount: normalizedLinks.length,
          jobLinkCount: jobLinks.length,
          source
        });
      } catch (error) {
        await collection.updateOne(
          { careerUrl: url },
          {
            $set: {
              careerUrl: url,
              links: [],
              linkCount: 0,
              jobLinks: [],
              jobLinkCount: 0,
              error: error.toString(),
              fetchedAt: new Date(),
              startedAt
            }
          },
          { upsert: true }
        );
        log("Error fetching career links.", { url, error: error.toString() });
      } finally {
        await streamAck(
          redisClient,
          queues.careerPages,
          streamGroups.careerPages,
          message.id
        );
      }

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
  const consumer = buildConsumerName("html");
  await ensureStreamGroup(
    redisClient,
    queues.careerLinks,
    streamGroups.careerLinks
  );
  let processed = 0;
  let claimStartId = "0-0";
  let lastClaimAt = 0;

  try {
    while (true) {
      let message = null;
      if (
        streamSettings.claimMinIdleMs > 0 &&
        streamSettings.claimIntervalMs > 0
      ) {
        const now = Date.now();
        if (now - lastClaimAt >= streamSettings.claimIntervalMs) {
          lastClaimAt = now;
          const claimResult = await streamAutoClaimOne(
            redisClient,
            queues.careerLinks,
            streamGroups.careerLinks,
            consumer,
            streamSettings.claimMinIdleMs,
            claimStartId,
            streamSettings.claimCount
          );
          if (claimResult) {
            claimStartId = claimResult.nextId || claimStartId;
            message = claimResult.message;
          }
        }
      }
      if (!message) {
        message = await streamReadGroup(
          redisClient,
          queues.careerLinks,
          streamGroups.careerLinks,
          consumer,
          streamSettings.blockMs,
          streamSettings.readCount
        );
      }
      if (!message) {
        if (args.once) {
          break;
        }
        continue;
      }
      if (!message.value) {
        await streamAck(
          redisClient,
          queues.careerLinks,
          streamGroups.careerLinks,
          message.id
        );
        continue;
      }

      const startedAt = new Date();
      let jobUrl = null;
      let careerUrl = null;
      try {
        const parsed = parseQueueItem(message.value);
        jobUrl = parsed.url;
        careerUrl = parsed.careerUrl;
        if (!jobUrl) {
          continue;
        }
        const isJobLink =
          filterJobLinks([jobUrl], {
            strongPatterns: settings.jobLinkStrongPatterns,
            weakPatterns: settings.jobLinkWeakPatterns,
            excludePatterns: settings.jobLinkExcludePatterns
          }).length > 0;
        if (!isJobLink) {
          await collection.updateOne(
            { url: jobUrl },
            {
              $set: {
                ...buildJobUpdate(jobUrl, careerUrl, {
                  html: "",
                  skipped: true,
                  skipReason: "non_job_link",
                  fetchedAt: new Date(),
                  startedAt
                })
              }
            },
            { upsert: true }
          );
          log("Skipped non-job link.", { url: jobUrl });
          continue;
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
                fetchedAt: new Date(),
                startedAt
              })
            }
          },
          { upsert: true }
        );
        processed += 1;
        log("HTML fetched.", { url: jobUrl, source, length: html.length });
      } catch (error) {
        const fallbackUrl = jobUrl || message.value;
        await collection.updateOne(
          { url: fallbackUrl },
          {
            $set: {
              ...buildJobUpdate(fallbackUrl, careerUrl, {
                html: "",
                error: error.toString(),
                fetchedAt: new Date(),
                startedAt
              })
            }
          },
          { upsert: true }
        );
        log("Error fetching HTML.", { url: fallbackUrl, error: error.toString() });
      } finally {
        await streamAck(
          redisClient,
          queues.careerLinks,
          streamGroups.careerLinks,
          message.id
        );
      }

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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const command = args._[0];

  if (!command) {
    console.log("Usage:");
    console.log("  node pipeline/index.js seed --file <path>");
    console.log("  node pipeline/index.js seed --collection <name> --field <field> [--batch-size <n>] [--max <n>]");
    console.log("                             [--mark-field <name>] [--mark-at-field <name>]");
    console.log("  node pipeline/index.js worker:links [--once] [--max <n>]");
    console.log("  node pipeline/index.js worker:html [--once] [--max <n>]");
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

  if (command === "worker:html") {
    await runHtmlWorker(args);
    return;
  }

  console.log(`Unknown command: ${command}`);
  process.exit(1);
}

main().catch((error) => {
  console.error("Fatal error:", error);
  process.exit(1);
});

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
const { createRedisClient, blpop, rpush, saddAndQueue } = require("./redis_queue");
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

function log(message, payload) {
  const ts = new Date().toISOString();
  if (payload) {
    console.log(`[${ts}] ${message}`, payload);
    return;
  }
  console.log(`[${ts}] ${message}`);
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
    await rpush(redisClient, queues.careerPages, url);
  }
  log("Seeded urls from file.", { count: urls.length, queue: queues.careerPages });
}

async function seedFromMongo(redisClient, db, collectionName, fieldName) {
  const collection = db.collection(collectionName);
  const cursor = collection.find(
    { [fieldName]: { $exists: true } },
    { projection: { [fieldName]: 1 } }
  );
  let count = 0;
  for await (const doc of cursor) {
    const url = doc[fieldName];
    if (!url) {
      continue;
    }
    await rpush(redisClient, queues.careerPages, url);
    count += 1;
  }
  log("Seeded urls from mongo.", {
    count,
    queue: queues.careerPages,
    collection: collectionName
  });
}

async function runLinksWorker(args) {
  const redisClient = await createRedisClient(redisUrl);
  const { client: mongoClient, db } = await connectMongo(
    mongoConfig.uri,
    mongoConfig.database
  );
  const collection = db.collection(collections.careerLinks);
  let processed = 0;

  try {
    while (true) {
      const url = await blpop(redisClient, queues.careerPages, settings.pollTimeoutSeconds);
      if (!url) {
        if (args.once) {
          break;
        }
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
          await saddAndQueue(
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
  let processed = 0;

  try {
    while (true) {
      const queueItem = await blpop(
        redisClient,
        queues.careerLinks,
        settings.pollTimeoutSeconds
      );
      if (!queueItem) {
        if (args.once) {
          break;
        }
        continue;
      }

      const { url: jobUrl, careerUrl } = parseQueueItem(queueItem);
      if (!jobUrl) {
        continue;
      }
      const startedAt = new Date();
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
      try {
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
        await collection.updateOne(
          { url: jobUrl },
          {
            $set: {
              ...buildJobUpdate(jobUrl, careerUrl, {
                html: "",
                error: error.toString(),
                fetchedAt: new Date(),
                startedAt
              })
            }
          },
          { upsert: true }
        );
        log("Error fetching HTML.", { url: jobUrl, error: error.toString() });
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
    console.log("  node pipeline/index.js seed --collection <name> --field <field>");
    console.log("  node pipeline/index.js worker:links [--once] [--max <n>]");
    console.log("  node pipeline/index.js worker:html [--once] [--max <n>]");
    process.exit(1);
  }

  if (command === "seed") {
    const redisClient = await createRedisClient(redisUrl);
    const filePath = args.file;
    const collectionName =
      args.collection || mongoConfig.import_collection || collections.careerPages;
    const fieldName = args.field || "careerUrl";

    try {
      if (filePath) {
        await seedFromFile(redisClient, filePath);
      } else {
        const { client: mongoClient, db } = await connectMongo(
          mongoConfig.uri,
          mongoConfig.database
        );
        try {
          await seedFromMongo(redisClient, db, collectionName, fieldName);
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

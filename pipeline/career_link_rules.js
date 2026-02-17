const fs = require("fs");
const path = require("path");

const EXPIRE_KEYWORDS_PATH = path.resolve(__dirname, "..", "expireKeywords.json");
const EXCLUDE_DOMAINS_PATH = path.resolve(
  __dirname,
  "..",
  "expireExcludeDomains.json"
);

const DEFAULT_NON_JOB_ANCHOR_TEXT_PATTERNS = [
  "about",
  "about us",
  "about-us",
  "our story",
  "who we are",
  "our company",
  "company",
  "contact",
  "contact us",
  "contact-us",
  "faq",
  "help",
  "help center",
  "support center",
  "customer support",
  "support",
  "connect",
  "connect with us",
  "team",
  "our team",
  "leadership",
  "news",
  "blog",
  "press",
  "investor",
  "privacy",
  "privacy policy",
  "cookie policy",
  "terms",
  "terms of use",
  "cookies",
  "legal",
  "security",
  "compliance",
  "accessibility",
  "sitemap",
  "site map",
  "home",
  "learn more",
  "services",
  "products",
  "solutions",
  "resources",
  "resource center",
  "community",
  "events",
  "webinars",
  "case studies",
  "testimonials",
  "partners",
  "subscribe",
  "newsletter",
  "follow us",
  "facebook",
  "instagram",
  "linkedin",
  "twitter",
  "x",
  "youtube",
  "tiktok",
  "employee login",
  "client login",
  "member login",
  "portal",
  "back to top"
];

const POSITIVE_JOB_ANCHOR_TEXT_PATTERNS = [
  "job",
  "jobs",
  "career",
  "careers",
  "opening",
  "openings",
  "position",
  "positions",
  "vacancy",
  "vacancies",
  "requisition",
  "apply",
  "hiring",
  "opportunit",
  "talent"
];

const DEFAULT_NON_CAREER_URL_PATTERNS = [
  "privacy",
  "privacy-policy",
  "terms",
  "terms-of-use",
  "cookies",
  "cookie-policy",
  "accessibility",
  "benefits",
  "about",
  "about-us",
  "news",
  "newsroom",
  "blog",
  "press",
  "press-release",
  "media",
  "media-kit",
  "investor",
  "investors",
  "investor-relations",
  "sustainability",
  "esg",
  "location",
  "locations",
  "contact",
  "contact-us",
  "support",
  "support-center",
  "help",
  "help-center",
  "faq",
  "customer-support",
  "knowledge-base",
  "kb/",
  "team",
  "leadership",
  "resource-center",
  "resources",
  "events",
  "webinar",
  "case-study",
  "case-studies",
  "testimonials",
  "partners",
  "partner",
  "login",
  "signin",
  "signup",
  "register",
  "account",
  "policy",
  "mailto:",
  "tel:",
  "javascript:",
  "search?",
  "search/",
  "/search/",
  "?s=",
  "sitemap",
  "sitemap.xml",
  "rss",
  "feed",
  "/tag/",
  "/category/",
  "/author/",
  "wp-json",
  "wp-content",
  "wp-admin",
  "cdn-cgi",
  "facebook.com",
  "linkedin.com",
  "instagram.com",
  "twitter.com",
  "x.com",
  "youtube.com",
  "tiktok.com",
  "pinterest.com",
  "home"
];

const ATS_DOMAIN_PATTERNS = [
  "myworkdayjobs.com",
  "myworkdaysite.com",
  "greenhouse.io",
  "lever.co",
  "icims.com",
  "oraclecloud.com",
  "oraclecloudapps.com",
  "workforcenow.adp.com",
  "adp.com",
  "smartrecruiters.com",
  "successfactors.com",
  "successfactors.eu",
  "successfactors.cn",
  "jobvite.com",
  "taleo.net",
  "bamboohr.com",
  "entertimeonline.com",
  "dayforcehcm.com",
  "dayforce.com",
  "paycomonline.net",
  "ultipro.com",
  "clearcompany.com",
  "ashbyhq.com",
  "workable.com",
  "recruitee.com",
  "paylocity.com",
  "isolvedhire.com",
  "ukg.com",
  "ceridian.com",
  "jobs.net",
  "eightfold.ai",
  "teamtailor.com",
  "jobscore.com",
  "applytojob.com",
  "breezy.hr",
  "jazzhr.com",
  "brassring.com",
  "avature.net",
  "jobsoid.com",
  "applicantpro.com",
  "applicantstack.com",
  "pageuppeople.com",
  "talentreef.com",
  "recruiterbox.com",
  "hireology.com",
  "catsone.com",
  "pinpointhq.com",
  "paycor.com",
  "personio.com",
  "personio.de",
  "personio.io",
  "ceipal.com",
  "neogov.com",
  "governmentjobs.com",
  "manatal.com",
  "join.com",
  "newtonsoftware.com",
  "hirebridge.com",
  "peopleadmin.com",
  "careerplug.com",
  "jobadder.com",
  "homerun.co",
  "jobylon.com",
  "gohire.io",
  "workstream.us",
  "hirehive.com",
  "occupop.com",
  "recruitcrm.io",
  "zohorecruit.com"
];

const ATS_HOST_HINT_REGEX =
  /(^|\.)(jobs?|careers?|recruit(?:ing|ment)?|talent|hiring|apply)(\.|$)/i;
const ATS_PATH_HINT_REGEX =
  /(career|careers|jobs?|opening|openings|position|positions|apply|recruit|candidateexperience|candidateportal|talent|opportunit|vacanc|requisition|employment|jobsearch|job-board|jobboard|joblist|searchjobs|jobdetail|current-openings|join-us|work-with-us)/i;
const ATS_QUERY_HINT_REGEX =
  /(gh_jid|gh_src|lever[-_](origin|source)|jobid|job_id|jobreq|requisition|selectedmenukey=currentopenings|careerssearch|career_ns|in_iframe|familyid|jobboard|openingid|postingid|positionid|partnerid=|siteid=|jobpostid=|company=|cid=|ccid=)/i;
const NON_PAGE_FILE_EXT_REGEX =
  /\.(?:pdf|docx?|xlsx?|zip|rar|7z|png|jpe?g|gif|svg|webp|bmp|ico|css|js|json|xml|txt|mp3|mp4|avi|mov|wmv)$/i;
const CAREER_PATH_HINT_REGEX =
  /(career|careers|jobs?|opening|openings|position|positions|apply|recruit|candidateexperience|candidateportal|talent|opportunit|vacanc|requisition|employment|jobsearch|job-board|jobboard|joblist|searchjobs|jobdetail|current-openings|join-us|work-with-us|hiring)/i;
const STRONG_JOB_SIGNAL_REGEX =
  /(jobid=|job_id=|gh_jid=|jid=|jobpostid=|openingid=|postingid=|positionid=|requisition|careersection|searchjobs|jobsearch|candidateexperience|candidateportal|\/job\/|\/jobs\/|\/careers?\/|\/openings?\/|\/positions?\/|\/apply(?:\/|\?|$))/i;
const ATS_HOSTED_SUBDOMAIN_PATTERNS = [
  "breezy.hr",
  "teamtailor.com",
  "recruitee.com",
  "jobscore.com",
  "ashbyhq.com",
  "workable.com",
  "applytojob.com",
  "pinpointhq.com",
  "applicantstack.com",
  "applicantpro.com",
  "talentreef.com",
  "catsone.com",
  "jobsoid.com",
  "avature.net",
  "brassring.com",
  "homerun.co",
  "jobylon.com",
  "gohire.io",
  "careerplug.com",
  "jobadder.com",
  "hirehive.com",
  "occupop.com",
  "workstream.us",
  "recruitcrm.io",
  "zohorecruit.com"
];
const HARD_NON_CAREER_PATH_SEGMENTS = new Set([
  "about",
  "aboutus",
  "contact",
  "contactus",
  "faq",
  "help",
  "support",
  "privacy",
  "terms",
  "cookie",
  "cookies",
  "sitemap",
  "home",
  "news",
  "blog",
  "press",
  "investor",
  "team",
  "leadership",
  "benefits",
  "locations",
  "location",
  "accessibility",
  "login",
  "signin",
  "signup",
  "register",
  "policy"
]);

const FAST_EXPIRE_HINT_REGEX =
  /\b(404|403|410|500|502|503|504|error|expired|no jobs?|not found|forbidden|access denied|job closed|position filled|vacancy closed)\b/i;

function readJsonArray(filePath) {
  try {
    const content = fs.readFileSync(filePath, "utf8");
    const parsed = JSON.parse(content);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed;
  } catch (error) {
    return [];
  }
}

function normalizeTextForMatch(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/&nbsp;/gi, " ")
    .replace(/&#(\d+);/g, (_, dec) => {
      const code = Number(dec);
      if (!Number.isFinite(code)) {
        return " ";
      }
      return String.fromCharCode(code);
    })
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
      const code = Number.parseInt(hex, 16);
      if (!Number.isFinite(code)) {
        return " ";
      }
      return String.fromCharCode(code);
    })
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function keywordTokens(value) {
  const normalized = normalizeTextForMatch(value);
  if (!normalized) {
    return [];
  }
  return normalized.match(/[a-z0-9]+/g) || [];
}

function normalizeHost(input) {
  try {
    const host = String(input || "")
      .replace(/^https?:\/\//i, "")
      .replace(/\/.*$/, "");
    return host.replace(/^www\./i, "").toLowerCase();
  } catch (error) {
    return "";
  }
}

function matchesDomain(host, pattern) {
  return host === pattern || host.endsWith(`.${pattern}`);
}

function normalizePatterns(patterns, fallback = []) {
  const source = Array.isArray(patterns) && patterns.length ? patterns : fallback;
  return source
    .filter((pattern) => typeof pattern === "string" && pattern.trim())
    .map((pattern) => pattern.toLowerCase());
}

function matchesAnyPattern(value, patterns) {
  if (!value || !Array.isArray(patterns) || !patterns.length) {
    return false;
  }
  for (const pattern of patterns) {
    if (value.includes(pattern)) {
      return true;
    }
  }
  return false;
}

function normalizePathSegment(segment) {
  try {
    return decodeURIComponent(String(segment || ""))
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "");
  } catch (error) {
    return String(segment || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "");
  }
}

function hasHardNonCareerTerminal(pathname) {
  const segments = String(pathname || "")
    .split("/")
    .map((segment) => normalizePathSegment(segment))
    .filter(Boolean);
  if (!segments.length) {
    return false;
  }
  const last = segments[segments.length - 1];
  if (HARD_NON_CAREER_PATH_SEGMENTS.has(last)) {
    return true;
  }
  if (
    segments.length <= 2 &&
    segments.every((segment) => HARD_NON_CAREER_PATH_SEGMENTS.has(segment))
  ) {
    return true;
  }
  return false;
}

function hasStrongJobSignal(link) {
  const lower = String(link || "").toLowerCase();
  if (!lower) {
    return false;
  }
  return STRONG_JOB_SIGNAL_REGEX.test(lower) || ATS_QUERY_HINT_REGEX.test(lower);
}

function createExpireKeywordIndex(rawKeywords) {
  const uniqueMap = new Map();
  for (const keyword of rawKeywords || []) {
    if (typeof keyword !== "string") {
      continue;
    }
    const normalized = normalizeTextForMatch(keyword);
    if (!normalized || normalized.length < 3) {
      continue;
    }
    if (!uniqueMap.has(normalized)) {
      uniqueMap.set(normalized, keyword.trim());
    }
  }
  const items = Array.from(uniqueMap.entries()).map(([normalized, original], index) => {
    const tokens = keywordTokens(normalized);
    const key =
      tokens.length >= 2
        ? `${tokens[0]} ${tokens[1]}`
        : tokens.length === 1
          ? tokens[0]
          : normalized.slice(0, 10);
    return {
      id: index,
      original,
      normalized,
      key
    };
  });
  const map = new Map();
  for (const item of items) {
    if (!map.has(item.key)) {
      map.set(item.key, []);
    }
    map.get(item.key).push(item);
  }
  return {
    items,
    map
  };
}

const expireKeywordIndex = createExpireKeywordIndex(readJsonArray(EXPIRE_KEYWORDS_PATH));
const excludedDomainPatterns = readJsonArray(EXCLUDE_DOMAINS_PATH)
  .filter((item) => typeof item === "string" && item.trim())
  .map((item) => normalizeHost(item))
  .filter(Boolean);

function buildTextKeys(normalizedText) {
  const tokens = normalizedText.match(/[a-z0-9]+/g) || [];
  const keys = new Set();
  for (let i = 0; i < tokens.length; i += 1) {
    const current = tokens[i];
    keys.add(current);
    if (i + 1 < tokens.length) {
      keys.add(`${current} ${tokens[i + 1]}`);
    }
  }
  return keys;
}

function findExpireKeywordMatches(text, options = {}) {
  const limit = Number(options.limit || 8);
  const normalizedText = normalizeTextForMatch(text);
  if (!normalizedText) {
    return [];
  }
  const keys = buildTextKeys(normalizedText);
  const candidates = new Map();
  for (const key of keys) {
    const list = expireKeywordIndex.map.get(key);
    if (!list || !list.length) {
      continue;
    }
    for (const item of list) {
      if (!candidates.has(item.id)) {
        candidates.set(item.id, item);
      }
    }
  }
  const matches = [];
  for (const item of candidates.values()) {
    if (normalizedText.includes(item.normalized)) {
      matches.push(item.original);
      if (matches.length >= limit) {
        break;
      }
    }
  }
  return matches;
}

function detectExpiredOrNoJobs(content, options = {}) {
  const statusCode = Number(options.statusCode || 0);
  const text = `${options.title || ""} ${content || ""}`;
  const normalized = normalizeTextForMatch(text);
  const fastHit = FAST_EXPIRE_HINT_REGEX.test(normalized);
  const matches = findExpireKeywordMatches(normalized, {
    limit: options.limit || 8
  });
  const statusCodeExpired = statusCode >= 400;
  return {
    statusCode,
    fastHintMatched: fastHit,
    statusCodeExpired,
    matchedKeywords: matches,
    matchedKeywordCount: matches.length,
    isExpiredOrNoJobs: statusCodeExpired || matches.length > 0
  };
}

function isExcludedDomain(url) {
  if (!url) {
    return {
      excluded: false,
      pattern: ""
    };
  }
  let host = "";
  try {
    host = normalizeHost(new URL(url).hostname);
  } catch (error) {
    host = normalizeHost(url);
  }
  if (!host) {
    return {
      excluded: false,
      pattern: ""
    };
  }
  for (const pattern of excludedDomainPatterns) {
    if (!pattern) {
      continue;
    }
    if (host === pattern || host.endsWith(`.${pattern}`) || host.includes(pattern)) {
      return {
        excluded: true,
        pattern
      };
    }
  }
  return {
    excluded: false,
    pattern: ""
  };
}

function isAtsCareerLink(link) {
  if (!link) {
    return false;
  }
  let parsed;
  try {
    parsed = new URL(link);
  } catch (error) {
    return false;
  }
  if (!/^https?:$/i.test(parsed.protocol)) {
    return false;
  }
  const host = normalizeHost(parsed.hostname);
  const pathname = parsed.pathname.toLowerCase();
  const search = parsed.search.toLowerCase();
  const combined = `${pathname}${search}`;
  if (!host) {
    return false;
  }
  if (NON_PAGE_FILE_EXT_REGEX.test(pathname)) {
    return false;
  }

  // High-confidence ATS URL signatures.
  if (host === "jobs.lever.co" || host.endsWith(".jobs.lever.co")) {
    return true;
  }
  if (matchesDomain(host, "myworkdayjobs.com")) {
    return true;
  }
  if (matchesDomain(host, "myworkdaysite.com")) {
    return true;
  }
  if (matchesDomain(host, "entertimeonline.com") && /\/ta\/[^/]+\.careers/.test(pathname)) {
    return true;
  }
  if (
    (matchesDomain(host, "oraclecloud.com") || matchesDomain(host, "oraclecloudapps.com")) &&
    pathname.includes("/hcmui/candidateexperience/")
  ) {
    return true;
  }
  if (
    matchesDomain(host, "workforcenow.adp.com") &&
    /recruitment\/recruitment\.html/.test(pathname)
  ) {
    return true;
  }
  if (
    matchesDomain(host, "greenhouse.io") &&
    (host.startsWith("boards.") ||
      host.startsWith("job-boards.") ||
      /\/(jobs|embed\/job_board|job_board|board)/.test(pathname))
  ) {
    return true;
  }
  if (
    matchesDomain(host, "icims.com") &&
    (host.startsWith("jobs.") || /\/jobs?(\/|$)/.test(pathname))
  ) {
    return true;
  }
  if (
    matchesDomain(host, "brassring.com") &&
    /(\/tgnewui\/|\/search\/home|homewithpreload|partnerid=|siteid=)/.test(combined)
  ) {
    return true;
  }
  if (
    matchesDomain(host, "taleo.net") &&
    /(careersection|jobdetail|search\.ftl|requisition|jobsearch)/.test(combined)
  ) {
    return true;
  }
  if (
    (matchesDomain(host, "successfactors.com") ||
      matchesDomain(host, "successfactors.eu") ||
      matchesDomain(host, "successfactors.cn")) &&
    /(career|jobs?|career_ns|company=)/.test(combined)
  ) {
    return true;
  }
  if (
    (matchesDomain(host, "dayforcehcm.com") || matchesDomain(host, "dayforce.com")) &&
    /(candidateportal|careers?|jobs?|recruiting)/.test(combined)
  ) {
    return true;
  }
  if (
    matchesDomain(host, "jobvite.com") &&
    (host.startsWith("jobs.") || /\/(job|jobs|careers?|openings?)/.test(pathname))
  ) {
    return true;
  }
  if (
    ATS_HOSTED_SUBDOMAIN_PATTERNS.some(
      (pattern) => matchesDomain(host, pattern) && host !== pattern
    )
  ) {
    return true;
  }

  if (!ATS_DOMAIN_PATTERNS.some((pattern) => matchesDomain(host, pattern))) {
    return false;
  }

  if (ATS_PATH_HINT_REGEX.test(combined) || ATS_QUERY_HINT_REGEX.test(combined)) {
    return true;
  }

  if (ATS_HOST_HINT_REGEX.test(host)) {
    return true;
  }

  if (
    matchesDomain(host, "smartrecruiters.com") &&
    (pathname !== "/" || host.startsWith("careers.") || host.startsWith("jobs."))
  ) {
    return true;
  }

  return false;
}

function hasCareerLikeSignal(link) {
  if (!link) {
    return false;
  }
  const lower = String(link).toLowerCase();
  if (!lower) {
    return false;
  }
  if (isAtsCareerLink(link)) {
    return true;
  }
  return CAREER_PATH_HINT_REGEX.test(lower) || hasStrongJobSignal(lower);
}

function isClearlyNonCareerLink(link, options = {}) {
  if (!link) {
    return true;
  }
  const raw = String(link).trim();
  if (!raw) {
    return true;
  }
  const lower = raw.toLowerCase();
  if (/^(?:javascript:|mailto:|tel:)/.test(lower)) {
    return true;
  }

  let pathname = "";
  let combined = lower;
  try {
    const parsed = new URL(raw);
    pathname = String(parsed.pathname || "").toLowerCase();
    const search = String(parsed.search || "").toLowerCase();
    const hash = String(parsed.hash || "").toLowerCase();
    combined = `${pathname}${search}${hash}`;
  } catch (error) {
    pathname = "";
    combined = lower;
  }

  if (pathname && NON_PAGE_FILE_EXT_REGEX.test(pathname)) {
    return true;
  }

  const hasJobSignal = hasStrongJobSignal(combined);
  const hardTerminal = hasHardNonCareerTerminal(pathname);
  if (hardTerminal && !hasJobSignal) {
    return true;
  }

  const excludePatterns = normalizePatterns(
    options.excludePatterns,
    DEFAULT_NON_CAREER_URL_PATTERNS
  );
  if (excludePatterns.length && matchesAnyPattern(combined, excludePatterns)) {
    if (!hasJobSignal) {
      return true;
    }
    if (hardTerminal) {
      return true;
    }
  }

  return false;
}

function filterCareerLinks(links, options = {}) {
  if (!Array.isArray(links) || !links.length) {
    return {
      links: [],
      dropped: []
    };
  }

  const uniqueLinks = Array.from(
    new Set(
      links
        .map((link) => String(link || "").trim())
        .filter(Boolean)
    )
  );
  const forced = new Set(
    (Array.isArray(options.forceInclude) ? options.forceInclude : [])
      .map((link) => String(link || "").trim())
      .filter(Boolean)
  );
  const kept = [];
  const dropped = [];
  for (const link of uniqueLinks) {
    if (forced.has(link)) {
      kept.push(link);
      continue;
    }
    if (isClearlyNonCareerLink(link, options)) {
      dropped.push({ url: link, reason: "non_career_pattern" });
      continue;
    }
    if (!hasCareerLikeSignal(link)) {
      dropped.push({ url: link, reason: "not_career_like" });
      continue;
    }
    kept.push(link);
  }

  for (const link of forced) {
    kept.push(link);
  }

  return {
    links: Array.from(new Set(kept)),
    dropped
  };
}

function filterAtsCareerLinks(links, options = {}) {
  if (!Array.isArray(links) || !links.length) {
    return {
      links: [],
      dropped: []
    };
  }

  const uniqueLinks = Array.from(
    new Set(
      links
        .map((link) => String(link || "").trim())
        .filter(Boolean)
    )
  );
  const kept = [];
  const dropped = [];

  for (const link of uniqueLinks) {
    if (!isAtsCareerLink(link)) {
      dropped.push({ url: link, reason: "not_ats" });
      continue;
    }
    if (isClearlyNonCareerLink(link, options) && !hasStrongJobSignal(link)) {
      dropped.push({ url: link, reason: "non_career_pattern" });
      continue;
    }
    kept.push(link);
  }

  return {
    links: Array.from(new Set(kept)),
    dropped
  };
}

function normalizeAnchorText(text) {
  return normalizeTextForMatch(text);
}

function isNonJobAnchorText(text, options = {}) {
  const normalized = normalizeAnchorText(text);
  if (!normalized) {
    return false;
  }
  if (normalized.length > 140) {
    return false;
  }

  const positivePatterns =
    options.positivePatterns || POSITIVE_JOB_ANCHOR_TEXT_PATTERNS;
  if (positivePatterns.some((pattern) => normalized.includes(pattern))) {
    return false;
  }

  const nonJobPatterns =
    options.nonJobPatterns || DEFAULT_NON_JOB_ANCHOR_TEXT_PATTERNS;
  if (nonJobPatterns.some((pattern) => normalized === pattern || normalized.includes(pattern))) {
    return true;
  }

  const stopWords = new Set([
    "about",
    "us",
    "contact",
    "faq",
    "help",
    "support",
    "news",
    "blog",
    "privacy",
    "terms",
    "cookie",
    "cookies",
    "sitemap",
    "home",
    "team",
    "investor",
    "connect"
  ]);
  const words = normalized.split(" ").filter(Boolean);
  if (words.length && words.length <= 4 && words.every((word) => stopWords.has(word))) {
    return true;
  }
  return false;
}

function filterLinksByAnchorText(links, linkEntries, options = {}) {
  if (!Array.isArray(links) || !links.length) {
    return {
      links: [],
      dropped: []
    };
  }
  if (!Array.isArray(linkEntries) || !linkEntries.length) {
    return {
      links: Array.from(new Set(links)),
      dropped: []
    };
  }

  const textMap = new Map();
  for (const entry of linkEntries) {
    if (!entry || !entry.url) {
      continue;
    }
    const url = String(entry.url);
    const text = String(entry.text || "").trim();
    if (!textMap.has(url)) {
      textMap.set(url, []);
    }
    if (text) {
      textMap.get(url).push(text);
    }
  }

  const kept = [];
  const dropped = [];
  for (const link of links) {
    const texts = textMap.get(link) || [];
    if (!texts.length) {
      kept.push(link);
      continue;
    }
    const nonJobTexts = texts.filter((text) =>
      isNonJobAnchorText(text, {
        nonJobPatterns: options.nonJobPatterns,
        positivePatterns: options.positivePatterns
      })
    );
    if (nonJobTexts.length === texts.length) {
      dropped.push({ url: link, texts: nonJobTexts.slice(0, 3) });
      continue;
    }
    kept.push(link);
  }

  return {
    links: Array.from(new Set(kept)),
    dropped
  };
}

module.exports = {
  detectExpiredOrNoJobs,
  excludedDomainPatterns,
  extractAtsCareerLinks: (links) =>
    Array.from(new Set((links || []).filter((link) => isAtsCareerLink(link)))),
  filterAtsCareerLinks,
  filterCareerLinks,
  findExpireKeywordMatches,
  filterLinksByAnchorText,
  hasCareerLikeSignal,
  isAtsCareerLink,
  isClearlyNonCareerLink,
  isExcludedDomain,
  isNonJobAnchorText,
  normalizeTextForMatch
};

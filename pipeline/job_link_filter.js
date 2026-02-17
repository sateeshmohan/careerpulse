const DEFAULT_STRONG_PATTERNS = [
  "jobid=",
  "job_id=",
  "jid=",
  "gh_jid=",
  "req=",
  "requisition",
  "jobdetail",
  "jobdetails",
  "job-detail",
  "job-details",
  "jobdescription",
  "job-description",
  "job_desc",
  "apply?job",
  "apply/job",
  "apply?jid"
];

const DEFAULT_WEAK_PATTERNS = [
  "/job/",
  "/jobs/",
  "/career/",
  "/careers/",
  "/position/",
  "/positions/",
  "/opening/",
  "/openings/",
  "/opportunity/",
  "/opportunities/",
  "/posting",
  "/apply"
];

const DEFAULT_EXCLUDE_PATTERNS = [
  "privacy",
  "terms",
  "cookies",
  "accessibility",
  "benefits",
  "about",
  "news",
  "blog",
  "press",
  "investor",
  "sustainability",
  "location",
  "locations",
  "contact",
  "support",
  "faq",
  "login",
  "signin",
  "signup",
  "register",
  "policy",
  "mailto:",
  "tel:",
  "javascript:",
  "search?",
  "search/"
];

function normalizePatterns(patterns) {
  if (!patterns || !Array.isArray(patterns)) {
    return [];
  }
  return patterns
    .filter((pattern) => typeof pattern === "string" && pattern.trim().length)
    .map((pattern) => pattern.toLowerCase());
}

function matchesAny(value, patterns) {
  for (const pattern of patterns) {
    if (value.includes(pattern)) {
      return true;
    }
  }
  return false;
}

function isLikelyJobLink(link, options = {}) {
  if (!link) {
    return false;
  }
  const lower = String(link).toLowerCase();
  if (lower.startsWith("javascript:")) {
    return false;
  }
  if (lower.includes("emailme.asp")) {
    return false;
  }
  if (/jobcode=0(?:[^0-9]|$)/.test(lower)) {
    return false;
  }
  if (/\/jobs?(?:\/|$|\?)/.test(lower)) {
    return true;
  }
  const strongPatterns =
    normalizePatterns(options.strongPatterns) ||
    normalizePatterns(DEFAULT_STRONG_PATTERNS);
  const weakPatterns =
    normalizePatterns(options.weakPatterns) ||
    normalizePatterns(DEFAULT_WEAK_PATTERNS);
  const excludePatterns =
    normalizePatterns(options.excludePatterns) ||
    normalizePatterns(DEFAULT_EXCLUDE_PATTERNS);

  const strong =
    strongPatterns.length ? strongPatterns : DEFAULT_STRONG_PATTERNS;
  const weak = weakPatterns.length ? weakPatterns : DEFAULT_WEAK_PATTERNS;
  const exclude =
    excludePatterns.length ? excludePatterns : DEFAULT_EXCLUDE_PATTERNS;

  if (matchesAny(lower, strong)) {
    return true;
  }
  if (matchesAny(lower, exclude)) {
    return false;
  }
  return matchesAny(lower, weak);
}

function filterJobLinks(links, options = {}) {
  if (!Array.isArray(links) || !links.length) {
    return [];
  }
  return links.filter((link) => isLikelyJobLink(link, options));
}

module.exports = {
  isLikelyJobLink,
  filterJobLinks
};

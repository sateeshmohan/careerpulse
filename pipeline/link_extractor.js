const { URL } = require("url");

const DEFAULT_STRIP_TAGS = ["header", "footer", "nav", "aside"];

function stripSections(html, tags = DEFAULT_STRIP_TAGS) {
  if (!html) {
    return "";
  }
  let cleaned = html;
  for (const tag of tags) {
    const regex = new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*?<\\/${tag}>`, "gi");
    cleaned = cleaned.replace(regex, " ");
  }
  return cleaned;
}

function stripScriptsAndStyles(html) {
  if (!html) {
    return "";
  }
  return html
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ");
}

function normalizeHost(hostname) {
  if (!hostname) {
    return "";
  }
  return hostname.replace(/^www\./i, "").toLowerCase();
}

function isSkippableHref(href) {
  if (!href) {
    return true;
  }
  const lower = href.trim().toLowerCase();
  return (
    lower.startsWith("#") ||
    lower.startsWith("javascript:") ||
    lower.startsWith("mailto:") ||
    lower.startsWith("tel:") ||
    lower.startsWith("data:")
  );
}

function extractLinksFromHtml(html, baseUrl, options = {}) {
  const {
    sameDomainOnly = true,
    stripTags = DEFAULT_STRIP_TAGS,
    stripSectionsFirst = true
  } = options;

  if (!html || !baseUrl) {
    return [];
  }

  let cleaned = html;
  if (stripSectionsFirst) {
    cleaned = stripSections(cleaned, stripTags);
  }
  cleaned = stripScriptsAndStyles(cleaned);

  const baseHost = normalizeHost(new URL(baseUrl).hostname);
  const links = new Set();
  const regex = /<a\b[^>]*\bhref\s*=\s*(['"]?)([^'"\s>]+)\1/gi;
  let match = null;
  while ((match = regex.exec(cleaned)) !== null) {
    const rawHref = match[2];
    if (isSkippableHref(rawHref)) {
      continue;
    }
    let absoluteUrl = "";
    try {
      absoluteUrl = new URL(rawHref, baseUrl).toString();
    } catch (error) {
      continue;
    }
    if (sameDomainOnly) {
      const targetHost = normalizeHost(new URL(absoluteUrl).hostname);
      if (baseHost && targetHost && baseHost !== targetHost) {
        continue;
      }
    }
    links.add(absoluteUrl);
  }

  return Array.from(links);
}

module.exports = {
  extractLinksFromHtml,
  stripSections
};

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

function extractInlineLinksFromHandler(handlerText) {
  if (!handlerText || typeof handlerText !== "string") {
    return [];
  }
  const links = [];
  const quotedRegex = /(['"])(https?:\/\/[^'"]+|\/\/[^'"]+|\/[^'"]+|\.\.?\/[^'"]+)\1/g;
  let match = null;
  while ((match = quotedRegex.exec(handlerText)) !== null) {
    links.push(match[2]);
  }
  const absoluteRegex = /https?:\/\/[^\s"'`<>)]+/g;
  while ((match = absoluteRegex.exec(handlerText)) !== null) {
    links.push(match[0]);
  }
  return links;
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
  const addResolvedLink = (rawHref) => {
    if (isSkippableHref(rawHref)) {
      return;
    }
    let absoluteUrl = "";
    try {
      absoluteUrl = new URL(rawHref, baseUrl).toString();
    } catch (error) {
      return;
    }
    if (sameDomainOnly) {
      const targetHost = normalizeHost(new URL(absoluteUrl).hostname);
      if (baseHost && targetHost && baseHost !== targetHost) {
        return;
      }
    }
    links.add(absoluteUrl);
  };

  const regex = /<a\b[^>]*\bhref\s*=\s*(['"]?)([^'"\s>]+)\1/gi;
  let match = null;
  while ((match = regex.exec(cleaned)) !== null) {
    addResolvedLink(match[2]);
  }
  const frameRegex = /<(?:iframe|frame)\b[^>]*\bsrc\s*=\s*(['"]?)([^'"\s>]+)\1/gi;
  while ((match = frameRegex.exec(cleaned)) !== null) {
    addResolvedLink(match[2]);
  }

  const dataUrlRegex =
    /\b(?:data-href|data-url|data-link|data-job-url)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/gi;
  while ((match = dataUrlRegex.exec(cleaned)) !== null) {
    addResolvedLink(match[1] || match[2] || match[3]);
  }

  const handlerRegex = /\bon(?:click|keydown)\s*=\s*(?:"([^"]*)"|'([^']*)')/gi;
  while ((match = handlerRegex.exec(cleaned)) !== null) {
    const handler = match[1] || match[2] || "";
    const inlineLinks = extractInlineLinksFromHandler(handler);
    for (const inlineLink of inlineLinks) {
      addResolvedLink(inlineLink);
    }
  }

  return Array.from(links);
}

module.exports = {
  extractLinksFromHtml,
  stripSections
};

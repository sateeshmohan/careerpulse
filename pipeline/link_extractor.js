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

function decodeHtmlEntities(value) {
  if (!value || typeof value !== "string") {
    return "";
  }
  return value
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#(\d+);/g, (_, dec) => {
      const code = Number(dec);
      if (!Number.isFinite(code)) {
        return _;
      }
      try {
        return String.fromCharCode(code);
      } catch (error) {
        return _;
      }
    })
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
      const code = Number.parseInt(hex, 16);
      if (!Number.isFinite(code)) {
        return _;
      }
      try {
        return String.fromCharCode(code);
      } catch (error) {
        return _;
      }
    });
}

function collapseWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function stripTagsToText(value) {
  if (!value || typeof value !== "string") {
    return "";
  }
  return collapseWhitespace(decodeHtmlEntities(value.replace(/<[^>]+>/g, " ")));
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

function resolveLink(rawHref, baseUrl) {
  if (!rawHref || isSkippableHref(rawHref)) {
    return "";
  }
  try {
    return new URL(rawHref, baseUrl).toString();
  } catch (error) {
    return "";
  }
}

function getAttributeValue(attributes, name) {
  if (!attributes || !name) {
    return "";
  }
  const escaped = String(name).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const regex = new RegExp(
    `\\b${escaped}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`,
    "i"
  );
  const match = attributes.match(regex);
  if (!match) {
    return "";
  }
  return match[1] || match[2] || match[3] || "";
}

function extractLinkEntriesFromHtml(html, baseUrl, options = {}) {
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
  const entryMap = new Map();
  const addResolvedLink = (rawHref, text = "") => {
    const absoluteUrl = resolveLink(rawHref, baseUrl);
    if (!absoluteUrl) {
      return;
    }
    if (sameDomainOnly) {
      const targetHost = normalizeHost(new URL(absoluteUrl).hostname);
      if (baseHost && targetHost && baseHost !== targetHost) {
        return;
      }
    }
    const normalizedText = collapseWhitespace(text).slice(0, 240);
    if (!entryMap.has(absoluteUrl)) {
      entryMap.set(absoluteUrl, new Set());
    }
    if (normalizedText) {
      entryMap.get(absoluteUrl).add(normalizedText);
    }
  };

  const anchorRegex = /<a\b([^>]*)>([\s\S]*?)<\/a>/gi;
  let match = null;
  while ((match = anchorRegex.exec(cleaned)) !== null) {
    const attributes = match[1] || "";
    const rawHref = getAttributeValue(attributes, "href");
    if (!rawHref) {
      continue;
    }
    const bodyText = stripTagsToText(match[2] || "");
    const titleText = stripTagsToText(getAttributeValue(attributes, "title"));
    const ariaLabel = stripTagsToText(getAttributeValue(attributes, "aria-label"));
    const anchorText = collapseWhitespace(bodyText || ariaLabel || titleText);
    addResolvedLink(rawHref, anchorText);
    if (rawHref.trim().toLowerCase().startsWith("javascript:")) {
      const inlineLinks = extractInlineLinksFromHandler(rawHref);
      for (const inlineLink of inlineLinks) {
        addResolvedLink(inlineLink, anchorText);
      }
    }
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

  return Array.from(entryMap.entries()).flatMap(([url, texts]) => {
    if (!texts || !texts.size) {
      return [{ url, text: "" }];
    }
    return Array.from(texts).map((text) => ({ url, text }));
  });
}

function extractLinksFromHtml(html, baseUrl, options = {}) {
  const entries = extractLinkEntriesFromHtml(html, baseUrl, options);
  return Array.from(new Set(entries.map((entry) => entry.url).filter(Boolean)));
}

function extractPageTextSampleFromHtml(html, options = {}) {
  const maxLength = Number(options.maxLength || 60000);
  if (!html) {
    return {
      title: "",
      textSample: ""
    };
  }
  const titleMatch = String(html).match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  const title = stripTagsToText(titleMatch ? titleMatch[1] : "");
  const cleaned = stripScriptsAndStyles(String(html));
  const text = stripTagsToText(cleaned).slice(
    0,
    Number.isFinite(maxLength) && maxLength > 0 ? Math.floor(maxLength) : 60000
  );
  return {
    title,
    textSample: text
  };
}

module.exports = {
  extractLinksFromHtml,
  extractLinkEntriesFromHtml,
  extractPageTextSampleFromHtml,
  stripSections
};

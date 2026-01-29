function parseJsonSafely(text) {
  try {
    return JSON.parse(text);
  } catch (error) {
    return null;
  }
}

function collectObjects(value, bucket) {
  if (!value) {
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectObjects(item, bucket));
    return;
  }
  if (typeof value !== "object") {
    return;
  }
  bucket.push(value);
  if (value["@graph"]) {
    collectObjects(value["@graph"], bucket);
  }
}

function isJobPostingType(typeValue) {
  if (!typeValue) {
    return false;
  }
  if (Array.isArray(typeValue)) {
    return typeValue.some((entry) => String(entry).toLowerCase() === "jobposting");
  }
  return String(typeValue).toLowerCase() === "jobposting";
}

function extractLdJsonBlocks(html) {
  if (!html) {
    return [];
  }
  const blocks = [];
  const regex = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let match = null;
  while ((match = regex.exec(html)) !== null) {
    blocks.push(match[1]);
  }
  return blocks;
}

function extractJobPostingFromHtml(html) {
  const blocks = extractLdJsonBlocks(html);
  const parsed = [];
  for (const block of blocks) {
    const cleaned = block
      .replace(/\uFEFF/g, "")
      .replace(/\u00A0/g, " ")
      .trim();
    if (!cleaned) {
      continue;
    }
    const json = parseJsonSafely(cleaned);
    if (!json) {
      continue;
    }
    collectObjects(json, parsed);
  }

  if (!parsed.length) {
    return {
      rawBlocks: blocks,
      jobPosting: null
    };
  }

  const jobPosting = parsed.find((obj) => isJobPostingType(obj["@type"])) || null;
  return {
    rawBlocks: blocks,
    jobPosting
  };
}

module.exports = {
  extractJobPostingFromHtml
};

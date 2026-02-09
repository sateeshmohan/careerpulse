const { createClient } = require("redis");

async function createRedisClient(url) {
  const client = createClient({ url });
  client.on("error", (error) => {
    console.error("Redis error:", error);
  });
  await client.connect();
  return client;
}

async function blpop(client, key, timeoutSeconds = 5) {
  const result = await client.blPop(key, timeoutSeconds);
  return result ? result.element : null;
}

async function rpush(client, key, value) {
  await client.rPush(key, value);
}

async function ensureStreamGroup(client, streamKey, group, startId = "0-0") {
  const type = await client.type(streamKey);
  if (type !== "none" && type !== "stream") {
    throw new Error(
      `Key ${streamKey} is type ${type}. Streams require an empty key or a stream.`
    );
  }
  try {
    await client.xGroupCreate(streamKey, group, startId, { MKSTREAM: true });
  } catch (error) {
    const message = error && error.message ? error.message : "";
    if (!message.includes("BUSYGROUP")) {
      throw error;
    }
  }
}

async function streamAdd(client, streamKey, value) {
  const payload =
    typeof value === "string" ? value : JSON.stringify(value);
  await client.xAdd(streamKey, "*", { value: payload });
}

async function streamReadGroup(
  client,
  streamKey,
  group,
  consumer,
  blockMs = 5000,
  count = 1
) {
  const streams = await client.xReadGroup(
    group,
    consumer,
    { key: streamKey, id: ">" },
    { COUNT: count, BLOCK: blockMs }
  );
  if (!streams || !streams.length) {
    return null;
  }
  const stream = streams[0];
  if (!stream || !stream.messages || !stream.messages.length) {
    return null;
  }
  const message = stream.messages[0];
  return {
    id: message.id,
    value: message.message ? message.message.value : undefined
  };
}

let autoClaimSupported = true;

function normalizeAutoClaimResult(result) {
  if (!result) {
    return { nextId: "0-0", messages: [] };
  }
  if (Array.isArray(result)) {
    const nextId = result[0];
    const messages = result[1] || [];
    return { nextId, messages };
  }
  const nextId = result.nextId || result.next_id || "0-0";
  const messages = result.messages || result.entries || [];
  return { nextId, messages };
}

function normalizeMessageFields(fields) {
  if (!fields) {
    return {};
  }
  if (!Array.isArray(fields)) {
    return fields;
  }
  const out = {};
  for (let i = 0; i < fields.length; i += 2) {
    out[fields[i]] = fields[i + 1];
  }
  return out;
}

function normalizeClaimedMessages(messages) {
  if (!messages || !messages.length) {
    return [];
  }
  return messages
    .map((entry) => {
      if (!entry) {
        return null;
      }
      const id = entry.id || entry[0];
      const rawFields = entry.message || entry[1];
      const fields = normalizeMessageFields(rawFields);
      return {
        id,
        value: fields ? fields.value : undefined
      };
    })
    .filter(Boolean);
}

async function streamAutoClaimFallback(
  client,
  streamKey,
  group,
  consumer,
  minIdleMs,
  startId,
  count
) {
  const pending = await client.sendCommand([
    "XPENDING",
    streamKey,
    group,
    startId,
    "+",
    String(count)
  ]);
  if (!pending || !pending.length) {
    return { nextId: startId, message: null };
  }

  let nextId = startId;
  let candidateId = null;
  for (let i = 0; i < pending.length; i += 1) {
    const entry = pending[i];
    if (!entry || entry.length < 3) {
      continue;
    }
    const id = entry[0];
    const idle = Number(entry[2] || 0);
    nextId = id;
    if (!candidateId && idle >= minIdleMs) {
      candidateId = id;
    }
  }

  if (!candidateId) {
    return { nextId, message: null };
  }

  const claimed = await client.sendCommand([
    "XCLAIM",
    streamKey,
    group,
    consumer,
    String(minIdleMs),
    candidateId
  ]);
  const normalized = normalizeClaimedMessages(claimed);
  if (!normalized.length) {
    return { nextId, message: null };
  }
  return { nextId, message: normalized[0] };
}

async function streamAutoClaimOne(
  client,
  streamKey,
  group,
  consumer,
  minIdleMs,
  startId = "0-0",
  count = 1
) {
  if (autoClaimSupported) {
    try {
      const result = await client.xAutoClaim(
        streamKey,
        group,
        consumer,
        minIdleMs,
        startId,
        { COUNT: count }
      );
      const { nextId, messages } = normalizeAutoClaimResult(result);
      if (!messages || !messages.length) {
        return { nextId, message: null };
      }
      const message = messages[0];
      return {
        nextId,
        message: {
          id: message.id,
          value: message.message ? message.message.value : undefined
        }
      };
    } catch (error) {
      const message = error && error.message ? error.message : "";
      if (message.includes("unknown command") || message.includes("XAUTOCLAIM")) {
        autoClaimSupported = false;
      } else {
        throw error;
      }
    }
  }
  return streamAutoClaimFallback(
    client,
    streamKey,
    group,
    consumer,
    minIdleMs,
    startId,
    count
  );
}

async function streamAck(client, streamKey, group, id) {
  if (!id) {
    return 0;
  }
  return client.xAck(streamKey, group, id);
}

async function saddAndQueue(client, setKey, queueKey, value, queueValue = value) {
  const added = await client.sAdd(setKey, value);
  if (added) {
    await client.rPush(queueKey, queueValue);
  }
  return added;
}

async function saddAndStream(
  client,
  setKey,
  streamKey,
  value,
  streamValue = value
) {
  const added = await client.sAdd(setKey, value);
  if (added) {
    await streamAdd(client, streamKey, streamValue);
  }
  return added;
}

module.exports = {
  createRedisClient,
  blpop,
  rpush,
  ensureStreamGroup,
  streamAdd,
  streamReadGroup,
  streamAutoClaimOne,
  streamAck,
  saddAndQueue,
  saddAndStream
};

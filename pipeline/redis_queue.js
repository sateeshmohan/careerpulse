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

async function streamAddBatch(client, streamKey, values) {
  if (!Array.isArray(values) || !values.length) {
    return 0;
  }
  const multi = client.multi();
  for (const value of values) {
    const payload =
      typeof value === "string" ? value : JSON.stringify(value);
    multi.xAdd(streamKey, "*", { value: payload });
  }
  const replies = await multi.exec();
  return Array.isArray(replies) ? replies.length : 0;
}

async function streamReadGroup(
  client,
  streamKey,
  group,
  consumer,
  blockMs = 5000,
  count = 1
) {
  const messages = await streamReadGroupBatch(
    client,
    streamKey,
    group,
    consumer,
    blockMs,
    count
  );
  if (!messages.length) {
    return null;
  }
  return messages[0];
}

async function streamReadGroupBatch(
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
    return [];
  }
  const stream = streams[0];
  if (!stream || !stream.messages || !stream.messages.length) {
    return [];
  }
  return stream.messages
    .map((message) => ({
      id: message.id,
      value: message.message ? message.message.value : undefined
    }))
    .filter((message) => Boolean(message && message.id));
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
    return { nextId: startId, messages: [] };
  }

  let nextId = startId;
  const candidateIds = [];
  for (let i = 0; i < pending.length; i += 1) {
    const entry = pending[i];
    if (!entry || entry.length < 3) {
      continue;
    }
    const id = entry[0];
    const idle = Number(entry[2] || 0);
    nextId = id;
    if (idle >= minIdleMs) {
      candidateIds.push(id);
    }
  }

  if (!candidateIds.length) {
    return { nextId, messages: [] };
  }

  const command = [
    "XCLAIM",
    streamKey,
    group,
    consumer,
    String(minIdleMs),
    ...candidateIds
  ];
  const claimed = await client.sendCommand(command);
  const normalized = normalizeClaimedMessages(claimed);
  return { nextId, messages: normalized };
}

async function streamAutoClaimBatch(
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
      const normalizedMessages = (messages || [])
        .map((message) => ({
          id: message.id,
          value: message.message ? message.message.value : undefined
        }))
        .filter((message) => Boolean(message && message.id));
      return {
        nextId,
        messages: normalizedMessages
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

async function streamAutoClaimOne(
  client,
  streamKey,
  group,
  consumer,
  minIdleMs,
  startId = "0-0",
  count = 1
) {
  const batchResult = await streamAutoClaimBatch(
    client,
    streamKey,
    group,
    consumer,
    minIdleMs,
    startId,
    count
  );
  if (!batchResult || !batchResult.messages || !batchResult.messages.length) {
    return {
      nextId: batchResult ? batchResult.nextId : startId,
      message: null
    };
  }
  return {
    nextId: batchResult.nextId,
    message: batchResult.messages[0]
  };
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

async function saddAndStreamBatch(
  client,
  setKey,
  streamKey,
  entries
) {
  if (!Array.isArray(entries) || !entries.length) {
    return 0;
  }

  const normalized = entries
    .map((entry) => {
      if (
        entry &&
        typeof entry === "object" &&
        !Array.isArray(entry)
      ) {
        const dedupeValue = entry.value;
        const streamValue =
          entry.streamValue === undefined ? entry.value : entry.streamValue;
        return { dedupeValue, streamValue };
      }
      return {
        dedupeValue: entry,
        streamValue: entry
      };
    })
    .filter((entry) => entry.dedupeValue !== undefined && entry.dedupeValue !== null);

  if (!normalized.length) {
    return 0;
  }

  const args = [];
  for (const entry of normalized) {
    const streamPayload =
      typeof entry.streamValue === "string"
        ? entry.streamValue
        : JSON.stringify(entry.streamValue);
    args.push(String(entry.dedupeValue), streamPayload);
  }
  // Atomic dedupe + enqueue to avoid losing jobs if process crashes mid-batch.
  const added = await client.eval(
    `
      local setKey = KEYS[1]
      local streamKey = KEYS[2]
      local total = 0
      for i = 1, #ARGV, 2 do
        local dedupeValue = ARGV[i]
        local streamValue = ARGV[i + 1]
        local wasAdded = redis.call("SADD", setKey, dedupeValue)
        if wasAdded == 1 then
          redis.call("XADD", streamKey, "*", "value", streamValue)
          total = total + 1
        end
      end
      return total
    `,
    {
      keys: [setKey, streamKey],
      arguments: args
    }
  );
  return Number(added) || 0;
}

module.exports = {
  createRedisClient,
  blpop,
  rpush,
  ensureStreamGroup,
  streamAdd,
  streamAddBatch,
  streamReadGroupBatch,
  streamAutoClaimBatch,
  streamReadGroup,
  streamAutoClaimOne,
  streamAck,
  saddAndQueue,
  saddAndStream,
  saddAndStreamBatch
};

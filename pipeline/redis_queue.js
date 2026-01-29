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

async function saddAndQueue(client, setKey, queueKey, value) {
  const added = await client.sAdd(setKey, value);
  if (added) {
    await client.rPush(queueKey, value);
  }
  return added;
}

module.exports = {
  createRedisClient,
  blpop,
  rpush,
  saddAndQueue
};

const { MongoClient } = require("mongodb");

async function connectMongo(uri, database) {
  const client = new MongoClient(uri, {
    maxPoolSize: 10
  });
  await client.connect();
  return {
    client,
    db: client.db(database)
  };
}

module.exports = {
  connectMongo
};

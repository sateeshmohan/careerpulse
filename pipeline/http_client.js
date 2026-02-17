const got = require("got");
const { randomDesktopUserAgent } = require("./user_agents");

async function fetchHtmlWithGot(url, options = {}) {
  const timeoutMs = options.timeoutMs || 20000;
  const userAgent = options.userAgent || randomDesktopUserAgent();

  const response = await got(url, {
    headers: {
      "user-agent": userAgent,
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    },
    followRedirect: true,
    throwHttpErrors: true,
    timeout: {
      request: timeoutMs
    },
    https: {
      rejectUnauthorized: false
    }
  });

  return {
    html: response.body,
    statusCode: response.statusCode,
    finalUrl: response.url,
    userAgent
  };
}

module.exports = {
  fetchHtmlWithGot
};

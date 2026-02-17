const got = require("got");
const { randomUserAgent } = require("./user_agents");

async function fetchHtmlWithGot(url, options = {}) {
  const timeoutMs = options.timeoutMs || 20000;
  const userAgent =
    options.userAgent ||
    randomUserAgent({
      allowMobile: options.allowMobileUserAgent !== false
    });
  const redirectChain = [];

  const response = await got(url, {
    headers: {
      "user-agent": userAgent,
      "accept-language": "en-US,en;q=0.9",
      "upgrade-insecure-requests": "1",
      accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    },
    followRedirect: true,
    throwHttpErrors: false,
    timeout: {
      request: timeoutMs
    },
    https: {
      rejectUnauthorized: false
    },
    hooks: {
      beforeRedirect: [
        (_options, responseForRedirect) => {
          if (!responseForRedirect) {
            return;
          }
          redirectChain.push({
            url: String(responseForRedirect.url || ""),
            statusCode: Number(responseForRedirect.statusCode || 0),
            location: String(
              (responseForRedirect.headers && responseForRedirect.headers.location) || ""
            )
          });
        }
      ]
    }
  });

  const finalStep = {
    url: String(response.url || ""),
    statusCode: Number(response.statusCode || 0),
    location: ""
  };
  const fullRedirectChain = [...redirectChain, finalStep];
  const redirectStatusCodes = fullRedirectChain
    .map((step) => Number(step.statusCode || 0))
    .filter((code) => Number.isFinite(code) && code > 0);
  const redirected =
    fullRedirectChain.length > 1 ||
    String(response.url || "").toLowerCase() !== String(url || "").toLowerCase();

  return {
    html: response.body,
    statusCode: response.statusCode,
    finalUrl: response.url,
    userAgent,
    redirectChain: fullRedirectChain,
    redirectStatusCodes,
    redirectCount: Math.max(0, fullRedirectChain.length - 1),
    redirected
  };
}

module.exports = {
  fetchHtmlWithGot
};

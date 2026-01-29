const puppeteer = require("puppeteer");
const { randomUserAgent } = require("./user_agents");

let browserPromise = null;

async function getBrowser(launchOptions = {}) {
  if (!browserPromise) {
    browserPromise = puppeteer.launch({
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage"
      ],
      ...launchOptions
    });
  }
  return browserPromise;
}

async function withPage(fn, options = {}) {
  const browser = await getBrowser(options.launchOptions);
  const page = await browser.newPage();
  const userAgent = options.userAgent || randomUserAgent();
  await page.setUserAgent(userAgent);
  await page.setCacheEnabled(false);

  if (options.blockResources !== false) {
    await page.setRequestInterception(true);
    page.on("request", (request) => {
      const type = request.resourceType();
      if (["image", "media", "font"].includes(type)) {
        request.abort();
      } else {
        request.continue();
      }
    });
  }

  try {
    return await fn(page, userAgent);
  } finally {
    await page.close();
  }
}

async function fetchLinksWithPuppeteer(url, options = {}) {
  const timeoutMs = options.timeoutMs || 60000;
  return withPage(
    async (page, userAgent) => {
      await page.goto(url, {
        waitUntil: "networkidle2",
        timeout: timeoutMs
      });
      const links = await page.evaluate(() => {
        const stripSelectors = ["header", "footer", "nav", "aside"];
        stripSelectors.forEach((selector) => {
          document.querySelectorAll(selector).forEach((el) => el.remove());
        });
        return Array.from(document.querySelectorAll("a[href]"))
          .map((anchor) => anchor.href)
          .filter((href) => href);
      });
      return {
        links: Array.from(new Set(links)),
        source: "puppeteer",
        userAgent
      };
    },
    options
  );
}

async function fetchHtmlWithPuppeteer(url, options = {}) {
  const timeoutMs = options.timeoutMs || 60000;
  return withPage(
    async (page, userAgent) => {
      await page.goto(url, {
        waitUntil: "networkidle2",
        timeout: timeoutMs
      });
      const html = await page.content();
      return {
        html,
        source: "puppeteer",
        userAgent
      };
    },
    options
  );
}

async function closeBrowser() {
  if (!browserPromise) {
    return;
  }
  const browser = await browserPromise;
  browserPromise = null;
  await browser.close();
}

module.exports = {
  fetchLinksWithPuppeteer,
  fetchHtmlWithPuppeteer,
  closeBrowser
};

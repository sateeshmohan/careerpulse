const puppeteer = require("puppeteer");
const { randomDesktopUserAgent } = require("./user_agents");

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
  const userAgent = options.userAgent || randomDesktopUserAgent();
  await page.setUserAgent(userAgent);
  await page.setCacheEnabled(false);

  if (options.blockResources !== false) {
    await page.setRequestInterception(true);
    page.on("request", (request) => {
      const type = request.resourceType();
      if (["image", "font"].includes(type)) {
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
      const mainLinks = await page.evaluate(() => {
        const toAbsolute = (value) => {
          if (!value || typeof value !== "string") {
            return "";
          }
          try {
            return new URL(value, document.baseURI).toString();
          } catch (error) {
            return "";
          }
        };
        const extractInlineLinksFromHandler = (handlerText) => {
          if (!handlerText || typeof handlerText !== "string") {
            return [];
          }
          const links = [];
          const regex =
            /(['"])(https?:\/\/[^'"]+|\/\/[^'"]+|\/[^'"]+|\.\.?\/[^'"]+)\1/g;
          let match = null;
          while ((match = regex.exec(handlerText)) !== null) {
            const absolute = toAbsolute(match[2]);
            if (absolute) {
              links.push(absolute);
            }
          }
          return links;
        };

        const anchors = Array.from(document.querySelectorAll("a[href]"))
          .map((anchor) => anchor.href)
          .filter((href) => href);
        const embeddedSources = Array.from(
          document.querySelectorAll("iframe[src],frame[src]")
        )
          .map((element) => element.src)
          .filter((src) => src);
        const dataUrlAttributes = [
          "data-href",
          "data-url",
          "data-link",
          "data-job-url"
        ];
        const dataAttributeLinks = [];
        for (const selector of dataUrlAttributes) {
          document.querySelectorAll(`[${selector}]`).forEach((element) => {
            const raw = element.getAttribute(selector);
            const absolute = toAbsolute(raw);
            if (absolute) {
              dataAttributeLinks.push(absolute);
            }
          });
        }
        const inlineHandlerLinks = [];
        document.querySelectorAll("[onclick],[onkeydown]").forEach((element) => {
          inlineHandlerLinks.push(
            ...extractInlineLinksFromHandler(element.getAttribute("onclick")),
            ...extractInlineLinksFromHandler(element.getAttribute("onkeydown"))
          );
        });
        return anchors.concat(
          embeddedSources,
          dataAttributeLinks,
          inlineHandlerLinks
        );
      });
      const frameLinks = [];
      const frames = page.frames();
      for (const frame of frames) {
        try {
          const frameUrl = frame.url();
          if (frameUrl && frameUrl !== "about:blank") {
            frameLinks.push(frameUrl);
          }
          const links = await frame.$$eval("a[href]", (anchors) =>
            anchors.map((anchor) => anchor.href).filter(Boolean)
          );
          if (Array.isArray(links) && links.length) {
            frameLinks.push(...links);
          }
        } catch (error) {
          // Ignore detached/cross-origin frame failures and continue.
        }
      }
      return {
        links: Array.from(new Set([...(mainLinks || []), ...frameLinks])),
        source: "puppeteer",
        userAgent
      };
    },
    {
      ...options,
      blockResources: false
    }
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

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

function extractAdpExternalJobIds(payload) {
  if (!payload || !Array.isArray(payload.jobRequisitions)) {
    return [];
  }
  const ids = new Set();
  for (const requisition of payload.jobRequisitions) {
    if (!requisition || !requisition.customFieldGroup) {
      continue;
    }
    const stringFields = requisition.customFieldGroup.stringFields;
    if (!Array.isArray(stringFields)) {
      continue;
    }
    for (const field of stringFields) {
      if (!field || !field.nameCode) {
        continue;
      }
      const code = String(field.nameCode.codeValue || "");
      const value = String(field.stringValue || "").trim();
      if (code === "ExternalJobID" && value) {
        ids.add(value);
      }
    }
  }
  return Array.from(ids);
}

function buildAdpJobDetailLink(careerUrl, jobId) {
  if (!jobId) {
    return "";
  }
  try {
    const parsed = new URL(careerUrl);
    parsed.searchParams.set("jobId", String(jobId));
    return parsed.toString();
  } catch (error) {
    return "";
  }
}

async function fetchLinksWithPuppeteer(url, options = {}) {
  const timeoutMs = options.timeoutMs || 60000;
  return withPage(
    async (page, userAgent) => {
      const responseDerivedLinks = new Set();
      const responseTasks = new Set();
      page.on("response", (response) => {
        const task = (async () => {
          try {
            const responseUrl = response.url();
            if (
              !/careercenter\/public\/events\/staffing\/v1\/job-requisitions/i.test(
                responseUrl
              )
            ) {
              return;
            }
            const contentType = String(
              (response.headers() && response.headers()["content-type"]) || ""
            ).toLowerCase();
            if (!contentType.includes("json")) {
              return;
            }
            const payload = await response.json();
            const externalIds = extractAdpExternalJobIds(payload);
            for (const externalId of externalIds) {
              const detailLink = buildAdpJobDetailLink(url, externalId);
              if (detailLink) {
                responseDerivedLinks.add(detailLink);
              }
            }
          } catch (error) {
            // Ignore JSON parsing failures from unrelated responses.
          }
        })();
        responseTasks.add(task);
        task.finally(() => responseTasks.delete(task));
      });

      const collectLinks = async () => {
        const mainLinks = await page.evaluate(() => {
          const toAbsolute = (value) => {
            if (!value || typeof value !== "string") {
              return "";
            }
            try {
              return new URL(value.trim().replace(/&amp;/gi, "&"), document.baseURI).toString();
            } catch (error) {
              return "";
            }
          };
          const extractInlineLinksFromHandler = (handlerText) => {
            if (!handlerText || typeof handlerText !== "string") {
              return [];
            }
            const links = [];
            const addLink = (candidate) => {
              const absolute = toAbsolute(candidate);
              if (absolute) {
                links.push(absolute);
              }
            };
            const quotedRegex =
              /(['"])(https?:\/\/[^'"]+|\/\/[^'"]+|\/[^'"]+|\.\.?\/[^'"]+)\1/g;
            let match = null;
            while ((match = quotedRegex.exec(handlerText)) !== null) {
              addLink(match[2]);
            }
            const absoluteRegex = /https?:\/\/[^\s"'`<>)]+/g;
            while ((match = absoluteRegex.exec(handlerText)) !== null) {
              addLink(match[0]);
            }
            return links;
          };

          const anchors = Array.from(document.querySelectorAll("a[href]"))
            .flatMap((anchor) => {
              const out = [];
              if (
                anchor.href &&
                !String(anchor.href).toLowerCase().startsWith("javascript:")
              ) {
                out.push(anchor.href);
              }
              const rawHref = anchor.getAttribute("href");
              if (
                rawHref &&
                typeof rawHref === "string" &&
                rawHref.trim().toLowerCase().startsWith("javascript:")
              ) {
                out.push(...extractInlineLinksFromHandler(rawHref));
              }
              return out;
            })
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
            const links = await frame.$$eval("a[href]", (anchors) => {
              const toAbsolute = (value) => {
                if (!value || typeof value !== "string") {
                  return "";
                }
                try {
                  return new URL(
                    value.trim().replace(/&amp;/gi, "&"),
                    document.baseURI
                  ).toString();
                } catch (error) {
                  return "";
                }
              };
              const extractInlineLinksFromHandler = (handlerText) => {
                if (!handlerText || typeof handlerText !== "string") {
                  return [];
                }
                const links = [];
                const addLink = (candidate) => {
                  const absolute = toAbsolute(candidate);
                  if (absolute) {
                    links.push(absolute);
                  }
                };
                const quotedRegex =
                  /(['"])(https?:\/\/[^'"]+|\/\/[^'"]+|\/[^'"]+|\.\.?\/[^'"]+)\1/g;
                let match = null;
                while ((match = quotedRegex.exec(handlerText)) !== null) {
                  addLink(match[2]);
                }
                const absoluteRegex = /https?:\/\/[^\s"'`<>)]+/g;
                while ((match = absoluteRegex.exec(handlerText)) !== null) {
                  addLink(match[0]);
                }
                return links;
              };
              return anchors
                .flatMap((anchor) => {
                  const out = [];
                  if (
                    anchor.href &&
                    !String(anchor.href).toLowerCase().startsWith("javascript:")
                  ) {
                    out.push(anchor.href);
                  }
                  const rawHref = anchor.getAttribute("href");
                  if (
                    rawHref &&
                    typeof rawHref === "string" &&
                    rawHref.trim().toLowerCase().startsWith("javascript:")
                  ) {
                    out.push(...extractInlineLinksFromHandler(rawHref));
                  }
                  return out;
                })
                .filter(Boolean);
            });
            if (Array.isArray(links) && links.length) {
              frameLinks.push(...links);
            }
          } catch (error) {
            // Ignore detached/cross-origin frame failures and continue.
          }
        }
        return Array.from(new Set([...(mainLinks || []), ...frameLinks]));
      };

      const waitForDynamicLinksIfSparse = async (currentLinks) => {
        const host = (() => {
          try {
            return new URL(url).hostname.toLowerCase();
          } catch (error) {
            return "";
          }
        })();
        const isWorkdayHost = host.includes("myworkdayjobs.com");
        const hasJobLikeLink = currentLinks.some((link) =>
          /\/job\/|\/jobs\/|jobdetails|requisition|opening|position|apply/i.test(
            String(link || "")
          )
        );
        const shouldRetry =
          currentLinks.length <= 1 ||
          (currentLinks.length <= 10 && !hasJobLikeLink) ||
          (isWorkdayHost && !hasJobLikeLink);
        if (!shouldRetry) {
          return currentLinks;
        }
        const waitMs = isWorkdayHost
          ? Math.min(12000, Math.max(4000, Math.floor(timeoutMs / 6)))
          : Math.min(5000, Math.max(1000, Math.floor(timeoutMs / 10)));
        try {
          await Promise.race([
            page.waitForSelector(
              "a[href*='/job/'],a[href*='/jobs/'],[data-href*='/job/'],[data-url*='/job/'],[data-job-url*='/job/'],[onclick*='/job/'],[onclick*='/jobs/'],[onkeydown*='/job/'],[onkeydown*='/jobs/']",
              { timeout: waitMs }
            ),
            page.waitForFunction(
              () =>
                Boolean(
                  document.querySelector(
                    "a[href*='/job/'],a[href*='/jobs/'],[data-href*='/job/'],[data-url*='/job/'],[data-job-url*='/job/'],[onclick*='/job/'],[onclick*='/jobs/'],[onkeydown*='/job/'],[onkeydown*='/jobs/']"
                  )
                ) || document.querySelectorAll("a[href]").length > 20,
              { timeout: waitMs }
            )
          ]);
          await new Promise((resolve) => setTimeout(resolve, 350));
        } catch (error) {
          // Ignore wait timeout and keep current links.
        }
        const refreshedLinks = await collectLinks();
        if (!isWorkdayHost) {
          return refreshedLinks.length ? refreshedLinks : currentLinks;
        }
        const refreshedHasJobLikeLink = refreshedLinks.some((link) =>
          /\/job\/|\/jobs\/|jobdetails|requisition|opening|position|apply/i.test(
            String(link || "")
          )
        );
        if (refreshedHasJobLikeLink || refreshedLinks.length > 10) {
          return refreshedLinks;
        }
        await new Promise((resolve) => setTimeout(resolve, 1500));
        const finalLinks = await collectLinks();
        return finalLinks.length ? finalLinks : refreshedLinks;
      };

      const waitForResponseTasks = async () => {
        if (!responseTasks.size) {
          return;
        }
        await Promise.allSettled(Array.from(responseTasks));
      };

      await page.goto(url, {
        waitUntil: "networkidle2",
        timeout: timeoutMs
      });
      const initialLinks = await collectLinks();
      const links = await waitForDynamicLinksIfSparse(initialLinks);
      await waitForResponseTasks();
      const mergedLinks = Array.from(
        new Set([...(links || []), ...Array.from(responseDerivedLinks)])
      );
      return {
        links: mergedLinks,
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

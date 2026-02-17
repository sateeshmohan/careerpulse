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
  const pageTextLimit = Number(options.pageTextLimit || 60000);
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

      const dedupeEntries = (entries) => {
        const seen = new Set();
        const out = [];
        for (const entry of entries || []) {
          if (!entry || !entry.url) {
            continue;
          }
          const urlValue = String(entry.url).trim();
          if (!urlValue) {
            continue;
          }
          const textValue = String(entry.text || "")
            .replace(/\s+/g, " ")
            .trim()
            .slice(0, 240);
          const key = `${urlValue}\n${textValue}`;
          if (seen.has(key)) {
            continue;
          }
          seen.add(key);
          out.push({
            url: urlValue,
            text: textValue
          });
        }
        return out;
      };

      const mergeCollections = (base, incoming) => {
        const mergedEntries = dedupeEntries([
          ...((base && base.linkEntries) || []),
          ...((incoming && incoming.linkEntries) || [])
        ]);
        const mergedLinks = Array.from(
          new Set(mergedEntries.map((entry) => entry.url))
        );
        return {
          links: mergedLinks,
          linkEntries: mergedEntries,
          pageTitle:
            (incoming && incoming.pageTitle) ||
            (base && base.pageTitle) ||
            "",
          pageTextSample:
            (incoming && incoming.pageTextSample) ||
            (base && base.pageTextSample) ||
            ""
        };
      };

      const collectLinks = async () => {
        const mainResult = await page.evaluate((maxTextLength) => {
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
          const entries = [];
          const addEntry = (candidateUrl, text) => {
            const absolute = toAbsolute(candidateUrl);
            if (!absolute) {
              return;
            }
            entries.push({
              url: absolute,
              text: String(text || "")
            });
          };
          Array.from(document.querySelectorAll("a[href]")).forEach((anchor) => {
            const anchorText = (
              anchor.textContent ||
              anchor.getAttribute("aria-label") ||
              anchor.getAttribute("title") ||
              ""
            )
              .replace(/\s+/g, " ")
              .trim();
            if (
              anchor.href &&
              !String(anchor.href).toLowerCase().startsWith("javascript:")
            ) {
              addEntry(anchor.href, anchorText);
            }
            const rawHref = anchor.getAttribute("href");
            if (
              rawHref &&
              typeof rawHref === "string" &&
              rawHref.trim().toLowerCase().startsWith("javascript:")
            ) {
              const inlineLinks = extractInlineLinksFromHandler(rawHref);
              for (const inlineLink of inlineLinks) {
                addEntry(inlineLink, anchorText);
              }
            }
          });
          Array.from(document.querySelectorAll("iframe[src],frame[src]")).forEach(
            (element) => {
              addEntry(element.src, "");
            }
          );
          const dataUrlAttributes = [
            "data-href",
            "data-url",
            "data-link",
            "data-job-url"
          ];
          for (const selector of dataUrlAttributes) {
            document.querySelectorAll(`[${selector}]`).forEach((element) => {
              const raw = element.getAttribute(selector);
              const elementText = (
                element.textContent ||
                element.getAttribute("aria-label") ||
                element.getAttribute("title") ||
                ""
              )
                .replace(/\s+/g, " ")
                .trim();
              addEntry(raw, elementText);
            });
          }
          document.querySelectorAll("[onclick],[onkeydown]").forEach((element) => {
            const elementText = (
              element.textContent ||
              element.getAttribute("aria-label") ||
              element.getAttribute("title") ||
              ""
            )
              .replace(/\s+/g, " ")
              .trim();
            const extracted = [
              ...extractInlineLinksFromHandler(element.getAttribute("onclick")),
              ...extractInlineLinksFromHandler(element.getAttribute("onkeydown"))
            ];
            for (const inlineLink of extracted) {
              addEntry(inlineLink, elementText);
            }
          });
          const pageTextSample = String(
            (document.body && document.body.innerText) || ""
          )
            .replace(/\s+/g, " ")
            .trim()
            .slice(0, maxTextLength);
          return {
            entries,
            pageTitle: String(document.title || ""),
            pageTextSample
          };
        }, pageTextLimit);
        const frameEntries = [];
        const frames = page.frames();
        for (const frame of frames) {
          try {
            const frameUrl = frame.url();
            if (frameUrl && frameUrl !== "about:blank") {
              frameEntries.push({ url: frameUrl, text: "" });
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
              return anchors.flatMap((anchor) => {
                const out = [];
                const anchorText = (
                  anchor.textContent ||
                  anchor.getAttribute("aria-label") ||
                  anchor.getAttribute("title") ||
                  ""
                )
                  .replace(/\s+/g, " ")
                  .trim();
                if (
                  anchor.href &&
                  !String(anchor.href).toLowerCase().startsWith("javascript:")
                ) {
                  out.push({ url: anchor.href, text: anchorText });
                }
                const rawHref = anchor.getAttribute("href");
                if (
                  rawHref &&
                  typeof rawHref === "string" &&
                  rawHref.trim().toLowerCase().startsWith("javascript:")
                ) {
                  const inlineLinks = extractInlineLinksFromHandler(rawHref);
                  for (const inlineLink of inlineLinks) {
                    const absolute = toAbsolute(inlineLink);
                    if (absolute) {
                      out.push({ url: absolute, text: anchorText });
                    }
                  }
                }
                return out;
              });
            });
            if (Array.isArray(links) && links.length) {
              frameEntries.push(...links);
            }
          } catch (error) {
            // Ignore detached/cross-origin frame failures and continue.
          }
        }
        const mergedEntries = dedupeEntries([
          ...((mainResult && mainResult.entries) || []),
          ...frameEntries
        ]);
        return {
          links: Array.from(new Set(mergedEntries.map((entry) => entry.url))),
          linkEntries: mergedEntries,
          pageTitle: (mainResult && mainResult.pageTitle) || "",
          pageTextSample: (mainResult && mainResult.pageTextSample) || ""
        };
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
        const lowerUrl = String(url || "").toLowerCase();
        const isOracleCandidateExperience =
          host.includes("oraclecloud.com") &&
          lowerUrl.includes("/hcmui/candidateexperience/");
        const currentUrls = Array.isArray(currentLinks && currentLinks.links)
          ? currentLinks.links
          : [];
        const hasJobLikeLink = currentUrls.some((link) =>
          /\/job\/|\/jobs\/|jobdetails|requisition|opening|position|apply/i.test(
            String(link || "")
          )
        );
        const shouldRetry =
          currentUrls.length <= 1 ||
          (currentUrls.length <= 10 && !hasJobLikeLink) ||
          (isWorkdayHost && !hasJobLikeLink) ||
          (isOracleCandidateExperience && !hasJobLikeLink);
        if (!shouldRetry) {
          return currentLinks;
        }
        const waitMs = isWorkdayHost || isOracleCandidateExperience
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
        if (!isWorkdayHost && !isOracleCandidateExperience) {
          return refreshedLinks.links.length ? refreshedLinks : currentLinks;
        }
        const refreshedHasJobLikeLink = refreshedLinks.links.some((link) =>
          /\/job\/|\/jobs\/|jobdetails|requisition|opening|position|apply/i.test(
            String(link || "")
          )
        );
        if (refreshedHasJobLikeLink || refreshedLinks.links.length > 10) {
          return refreshedLinks;
        }
        await new Promise((resolve) => setTimeout(resolve, 1500));
        const finalLinks = await collectLinks();
        return finalLinks.links.length ? finalLinks : refreshedLinks;
      };

      const waitForResponseTasks = async () => {
        if (!responseTasks.size) {
          return;
        }
        await Promise.allSettled(Array.from(responseTasks));
      };

      const mainResponse = await page.goto(url, {
        waitUntil: "networkidle2",
        timeout: timeoutMs
      });
      const initialLinks = await collectLinks();
      const linksState = await waitForDynamicLinksIfSparse(initialLinks);
      await waitForResponseTasks();
      const mergedState = mergeCollections(linksState, {
        linkEntries: Array.from(responseDerivedLinks).map((link) => ({
          url: link,
          text: ""
        }))
      });
      return {
        links: mergedState.links,
        linkEntries: mergedState.linkEntries,
        source: "puppeteer",
        userAgent,
        statusCode: mainResponse ? mainResponse.status() : 0,
        finalUrl: page.url(),
        pageTitle: mergedState.pageTitle,
        pageTextSample: mergedState.pageTextSample
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

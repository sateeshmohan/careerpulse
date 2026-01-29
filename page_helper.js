/**
 * Waits until the iframe is attached and then returns it to the caller
 *
 * @access public
 * @param {object} page - The Puppeteer page API object
 * @param {string} frame_url - The Frame url
 * @returns {object} The Puppeteer iframe element
 */

async function iframeAttached(page, frame_url) {
  return new Promise(async (resolve) => {
    const pollingInterval = 1000;
    const poll = setInterval(async function waitForIFrameToLoad() {
      const iFrame = page.frames().find((frame) => frame.url() === frame_url);
      if (iFrame) {
        clearInterval(poll);
        resolve(iFrame);
      }
    }, pollingInterval);
  });
}

/**Waits until the iframe is merged into main frame html
 * @access public
 * @param {object} page - The Puppeteer page API object
 *
 */
async function framesMerging(page) {
  return page.evaluate(() => {
    try {
      for (const frame of document.querySelectorAll("iframe")) {
        try {
          frame.sandbox = "allow-same-origin allow-scripts";
          const frameDocument =
            frame.contentDocument ||
            frame.contentWindow.document ||
            frame.contentWindow.document.body.innerHTML;
          const div = document.createElement("div");
          for (const attr of frame.attributes) {
            if (
              attr.name !== "src" &&
              attr.name !== "srcdoc" &&
              attr.name !== "sandbox"
            ) {
              div.setAttribute(attr.name, attr.value);
            }
          }
          div.innerHTML = frameDocument.documentElement.innerHTML;
          frame.parentNode.replaceChild(div, frame);
        } catch (error) {}
      }
    } catch (error) {
      console.error("error in framesLoading:" + error);
    }
  });
}

module.exports = {
  iframeAttached,
  framesMerging,
};

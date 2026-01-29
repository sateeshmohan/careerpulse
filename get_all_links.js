const log = require("debug")("phase_2:get_all_links.js");
const { iframeAttached } = require("./page_helper");
var { getOnlyUrls } = require("./extractOnlyLinks");
const msg = require('debug')('worker:get_all_links')
const lodash = require("lodash");
/**
 *
 * @param {object} page-The Puppeteer page object for extracting all the links
 * @returns {object} Returns the Object all_links which is Array of object of urls
 */
const getAllLinks = async (url, page) => {
  var allLinks = [];
  try {
    // page.on("console", (msg) => console.log(msg.text()));
    // console.log(`LOADED SUCCESSFULLY ${url}`);
    // await framesLoading(page);
    // console.log(`FRAMES WERE SUBMERGED ${url}`);
    const frames = await page.frames();
    //Waiting for e
    await Promise.all(
      frames.map(async (frame) => {
        try {
          // console.log(`CHECKING for ${frame.url()}`);
          if (frame.url() != "about:blank" && frame.url() != "") {
            // console.log(`condition true CHECKING for ${frame.url()}`);
            await iframeAttached(page, frame.url());
            await frame.evaluate(() => {
              console.log("into evaluate function")
              function cleanup(node, type) {
                const scripts = [];
                let els = node.querySelectorAll(type);
                for (let i = els.length - 1; i >= 0; i--) {
                  els[i].parentNode.removeChild(els[i]);
                }
                return scripts;
              }
              cleanup(document.documentElement, "aside");
              cleanup(document.documentElement, 'table[class="evr_events "]');
            });
            var item = await extractAllLinks(frame);
            // console.log(`GOT THE DATA FOR ${frame.url()}`);
            // console.table(item);
            allLinks.push(item);
          }
        } catch (error) {
          log(`error in get_all_links frame urls:${error.toString()}`);
        }
      })
    );
  } catch (error) {
    console.log(`error in get_all_links:${error.toString()}`);
  } finally {
    return lodash.uniqBy(
      getOnlyUrls(
        url,
        lodash.uniqWith([].concat.apply([], allLinks), lodash.isEqual)
      ),
      "link"
    );
  }
};

/**
 * Returns all the links from individual frames
 *
 * @access public
 * @param {object} page - The Puppeteer page API object
 * @returns {object} Returns the Object all_links which is Array of object of urls
 */
const extractAllLinks = async (frame) => {
  try {
    return await frame.evaluate(() => {
      let elementVisible = (element) => {
        const style = getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        return (
          style.visibility !== "hidden" &&
          !!(rect.bottom || rect.top || rect.height || rect.width)
        );
      };
      function validateUrl(value) {
        return /^(?:(?:(?:https?|ftp):)?\/\/)(?:\S+(?::\S*)?@)?(?:(?!(?:10|127)(?:\.\d{1,3}){3})(?!(?:169\.254|192\.168)(?:\.\d{1,3}){2})(?!172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2})(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5])){2}(?:\.(?:[1-9]\d?|1\d\d|2[0-4]\d|25[0-4]))|(?:(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)(?:\.(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)*(?:\.(?:[a-z\u00a1-\uffff]{2,})))(?::\d{2,5})?(?:[/?#]\S*)?$/i.test(value);
      }
      function getLinks(node, selector) {
        let dataset = [];
        try {
          const anchors = node.querySelectorAll(selector) || [];
          // console.log("anchors length", anchors.length)
          for (let element of anchors) {
            let label = element.innerText;
            if (!label && label == "") {
              label =
                element.parentNode.innerText
                  .replace(/\t/g, " ")
                  .replace(/\n/g, " ")
                  .replace(/\s+/g, " ")
                  .trim() || "";
              if (!label && label == "") {
                label =
                  element.parentNode.parentNode.innerText
                    .replace(/\t/g, " ")
                    .replace(/\n/g, " ")
                    .replace(/\s+/g, " ")
                    .trim() || "";
              }
            }
            var obj = {
              label,
              mainparent: node.nodeName,
              parentNode: element.parentNode.nodeName,
              nodeName: element.nodeName,
            };
            var parentNodeAttr = element.parentNode.getAttributeNames();

            var parentNodeDetails = {};
            //Getting the Parent Node Attributes
            for (let ele of parentNodeAttr) {
              parentNodeDetails[ele] = element.parentNode.getAttribute(ele);
            }
            //Getting the Node Attributes
            for (let ele of element.getAttributeNames()) {
              let attrValue = element.getAttribute(ele);
              if (
                ele == "href" || ele == "onclick" ||
                attrValue.startsWith("/") ||
                attrValue.startsWith("www")
              ) {
                try {
                  if (attrValue && (ele == 'href' || attrValue.startsWith('http') || attrValue.startsWith('/') || attrValue.startsWith("www"))) {
                    let link = new window.URL(
                      attrValue,
                      window.document.URL
                    ).toString();
                    if (validateUrl(link)) {
                      console.log("mohan----href------", link);
                      obj[ele] = link;
                    }
                  }
                  if (attrValue && ele == 'onclick' && attrValue.indexOf("http") >= 0) {
                    let link = ""
                    if (attrValue.indexOf('",') > 0) {
                      link = attrValue.substring(attrValue.indexOf("http"), attrValue.indexOf('",'));
                    } else if (attrValue.indexOf("',") > 0) {
                      link = attrValue.substring(attrValue.indexOf("http"), attrValue.indexOf("',"));
                    } else if (attrValue.indexOf("'") > 0) {
                      link = attrValue.substring(attrValue.indexOf("http"), attrValue.lastIndexOf("'"));
                    } else if (attrValue.indexOf('"') > 0) {
                      link = attrValue.substring(attrValue.indexOf("http"), attrValue.lastIndexOf('"'));
                    }
                    link = new window.URL(
                      link,
                      window.document.URL
                    ).toString()
                    if (validateUrl(link)) {
                      // console.log("mohan----onclick------", link);
                      obj[ele] = link;
                    }
                  } else if (ele == 'onclick' && attrValue.indexOf("window.location.href=") >= 0) {
                    attrValue = attrValue.replace("window.location.href=", "").replace(/'/gi, "").replace(/"/gi, "");
                    let link = new window.URL(
                      attrValue,
                      window.document.URL
                    ).toString()
                    if (validateUrl(link)) {
                      obj[ele] = link;
                    }
                  } else if (ele == 'onclick' && attrValue.indexOf("window.location=") >= 0) {
                    attrValue = attrValue.replace("window.location=", "").replace(/'/gi, "").replace(/"/gi, "");
                    let link = new window.URL(
                      attrValue,
                      window.document.URL
                    ).toString()
                    if (validateUrl(link)) {
                      obj[ele] = link;
                    }
                  } else if (ele == 'onclick' && attrValue.indexOf("window.open('") >= 0) {
                    attrValue = attrValue.replace("window.open('", "").replace("'", "").replace(",", "").replace("_self", "").replace(")", "").replace(/'/gi, "").replace(/"/gi, "").replace(/ /g, "").replace(/;/g, "");
                    let link = new window.URL(
                      attrValue,
                      window.document.URL
                    ).toString()
                    if (validateUrl(link)) {
                      obj[ele] = link;
                    }
                  }
                } catch (error) {
                  console.log("error getting attribute ", error.toString())
                  obj[ele] = attrValue;
                }
              } else {
                obj[ele] = attrValue;
              }
            }
            obj["parentNodeDetails"] = parentNodeDetails;
            // console.log("...................................................")
            // console.log(JSON.stringify(obj, null, 3))
            // console.log("..................................................................")

            if (
              elementVisible(element) &&
              obj["label"] != "" &&
              obj.label.length >= 1 &&
              !/Page|Next|opportunities>>/gi.test(obj.label)
            )
              dataset.push(obj);
          }
        } catch (error) {
          console.log(`error in getLinks in extractAllLinks:${error.toString()}`);
        } finally {
          return dataset;
        }
      }
      var links = [];
      var nodeChildren =
        document.documentElement.querySelectorAll("body")[0].children || [];
      // console.log(`GOT THE CHILDREN:${nodeChildren.length}`);
      for (let element of nodeChildren) {
        // console.log(`GOT THE CHILDREN:${element.nodeName}`);
        links = links.concat(getLinks(element, "a"), getLinks(element, "tr"), getLinks(element, "div"));
      }
      return links;
    });
  } catch (error) {
    console.log(`error in extractAllLinks:${error.toString()}`);
    return [];
  }
};
async function framesLoading(page) {
  return page.evaluate(() => {
    try {
      // for (const frame of document.querySelectorAll("iframe")) {
      for (const frame of document.querySelectorAll("iframe,span>iframe")) {
        try {
          const frameDocument =
            frame.contentDocument ||
            frame.contentWindow.document ||
            frame.contentWindow.document.body.innerHTML;
          const div = document.createElement("div");
          for (const attr of frame.attributes) {
            div.setAttribute(attr.name, attr.value);
          }
          div.innerHTML = frameDocument.documentElement.innerHTML;
          console.log("===============================================");
          console.log(frameDocument.documentElement.innerText);
          console.log("===============================================");
          frame.parentNode.replaceChild(div, frame);
        } catch (error) {
          console.log("===================ERROR============================");
          console.log(error.toString());
          console.log("===============================================");
        }
      }
    } catch (error) {
      // log(`ERROR IN framesLoading ${url}`);
      return page;
    }
  });
}
module.exports = {
  getAllLinks,
};

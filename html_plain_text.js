'use strict';

const log = console.log;
const { PendingXHR } = require("pending-xhr-puppeteer");
const pdfToHtml = require('./convertPdfToHtml').pdftohtml;
const { validString } = require("../utilityFunctions/index");
const applyStrings = require('./applyStrings.json');
const { pageSetup } = require('../browser/pageSetup')
module.exports = async (page, url) => {
  try {
    page = await pageSetup(page)
    url = decodeURIComponent(url);
    log(`OPENING ${url}`);
    console.log(`OPENING ${url}`)
    //page.on("console", (msg) => console.log(msg.text()));
    let isPdf = url.indexOf('.pdf') !== -1;
    let response;
    const pendingXHR = new PendingXHR(page);
    if (!isPdf) {
      await page.setCacheEnabled(false);
      response = await page.goto(url, {
        timeout: 150000,
        waitUntil: "networkidle2",
      });
    } else {
      response = await pdfToHtml(url, page)
      return response;
    }
    await Promise.race([
      page.waitForNavigation({ waitUntil: 'networkidle0', timeout: 2400000 }),
      new Promise(resolve => {
        setTimeout(resolve, 5000);
      })
    ])
    // await page.keyboard.press('Escape')
    //   console.table(Object.keys(response))
    //here we have the html loaded in the browser page, now we have to extract data and structure our final response
    if ((!Boolean(response)) || ("_status" in response && response._status < 400) || (!response.hasOwnProperty("_status"))) {
      // await page.waitFor(5000);
      await pendingXHR.waitForAllXhrFinished().then(() => log('DONE WITH XHR')).catch((err) => log(err.toString()));
      log(`LOADED SUCCESSFULLY ${url}`);
      await framesLoading(page);
      log(`FRAMES WERE SUBMERGED ${url}`);
      let { html, jobBody, ldjson, jobType, errorType } = await page.evaluate((applyStrings) => {
        function cleanup(node, type) {
          const scripts = [];
          let els = node.getElementsByTagName(type);
          for (let i = els.length - 1; i >= 0; i--) {
            // if (els[i].type && els[i].type.toLowerCase().indexOf('ld+json') > -1) {
            //   scripts.push(els[i].innerText.replace(/\t/g, ' ').replace(/ /g, ' '));
            // }

            els[i].parentNode.removeChild(els[i]);

          }
          return scripts;
        }

        function removeComments(node) {
          if (node.nodeType === 8) {
            node.parentNode.removeChild(node);
          }
          for (const child of node.childNodes) {
            removeComments(child);
          }
        }
        function isArray(obj) {
          return Object.prototype.toString.call(obj) === "[object Array]";
        }
        function arrayCounter(arr) {
          if (!isArray(arr)) {
            throw new TypeError('Expected an array.');
          }
          var obj = {};
          for (var i = 0; i < arr.length; i++) {
            if (arr[i] in obj) {
              obj[arr[i]]++;
            } else {
              obj[arr[i]] = 1;
            }
          }
          return obj;
        };
        function applyString_function(html) {
          try {
            let html_apply_strings = applyStrings.filter(str => {
              const pattern = /[|&;$%@:!"()+,→,»0-9]/g;
              if (str.match(pattern)) return html.match(RegExp('\\b' + str, 'gi'));
              return html.match(RegExp('\\b' + str + '\\b', 'gi'));
            })
            // if (html.toUpperCase().includes("POSITION:")) html_apply_strings.push('POSITION:')	
            let arrayCount = arrayCounter(html_apply_strings)
            let joblink = true
            for (let i in arrayCount) {
              if (arrayCount[i] > 3) joblink = false;
            }
            if (html_apply_strings.length > 0 && joblink) {
              return {
                job: "job",
                html_apply_strings
              }
            } else {
              return {
                job: "noJob",
                html_apply_strings
              }
            }
          } catch (err) {
            return {
              job: "noJob",
              html_apply_strings: [],
              err: err.toString()
            }
          }
        }
        try {
          var LDJSON = []

          // var LDJSON = cleanup(document.documentElement, "script")
          let ldJsonNodeList = document.documentElement.querySelectorAll('[type="application/ld+json"]')
          for (let ldjson of ldJsonNodeList) {
            LDJSON.push(ldjson.innerText.replace(/\t/g, ' ').replace(/ /g, ' ').replace(/\\'/g, "'"))
          }
          removeComments(document)
          cleanup(document.documentElement, "script")
          cleanup(document.documentElement, "noscript")
          const html = document.documentElement.outerHTML
            .replace(/\s+/g, " ");
          let jobType = applyString_function(html)
          cleanup(document.documentElement, "meta")
          cleanup(document.documentElement, "select")
          // cleanup(document.documentElement, "nav")
          cleanup(document.documentElement, "style")
          cleanup(document.documentElement, "button")
          cleanup(document.documentElement, "link")
          cleanup(document.documentElement, "aside")
          cleanup(document.documentElement, "path");
          cleanup(document.documentElement, "svg");
          // cleanup(document.documentElement, "script")
          const jobbody = document.documentElement.innerText
            .replace(/\t/g, " ")
            .replace(/\s+/g, " ");
          var jobhtml = document.documentElement.outerHTML
            .replace(/\s+/g, " ");
          if (LDJSON.length == 0) {
            var LDJSON = {}
            let itemProps = document.documentElement.querySelectorAll('*[itemprop]')
            for (let index = 0; index < itemProps.length; index++) {
              try {
                const element = itemProps[index];
                let key = element.getAttribute('itemprop')
                let value = element.innerText
                LDJSON[key] = value;
              } catch (error) {
                // throw Error(`Error In if of LDJSON length: ${error}`)
              }
            }
            return {
              'html': jobhtml,
              'jobBody': jobbody,
              'ldjson': LDJSON,
              'errorType': "noError",
              jobType,
            };
          } else {
            let json = {};
            if (LDJSON && LDJSON.length) {
              for (const ldjson of LDJSON) {
                try {
                  // json = Object.assign(json, JSON.parse(ldjson.replace(/\n/g, '')));
                  let validateldjson = {}
                  validateldjson = JSON.parse(ldjson.replace(/\n/g, ''))
                  if (validateldjson.length !== undefined) {
                    for (let index = 0; index < validateldjson.length; index++) {
                      if (validateldjson[index].hasOwnProperty("@type")) {
                        if (validateldjson[index]["@type"] === "JobPosting") {
                          json = Object.assign(json, validateldjson[index]);
                          break
                        }
                      }
                    }
                  }
                  else {
                    if (validateldjson.hasOwnProperty("@type")) {
                      if (validateldjson["@type"] === "JobPosting") {
                        json = Object.assign(json, validateldjson);
                      }
                    }
                  }
                } catch (error) {
                  throw Error(`Error In else of LDJSON length: ${error}`)
                }
              }
            }
            // if (Object.keys(json).length == 0){
            //   json = Object.assign(json, exceptionalJson);
            // }
            if (json.hasOwnProperty("0")) {
              if (json['0'].hasOwnProperty('@type')) {
                try {
                  if (json['0']['@type'] === "JobPosting") {
                    let ldjsondata = json['0']
                    return {
                      'html': jobhtml,
                      'jobBody': jobbody,
                      'ldjson': ldjsondata,
                      'errorType': "noError",
                      jobType,
                    };
                  }
                } catch (error) {
                  // throw Error(`Error In if of LDJSON PROPERTY 0: ${error}`)
                }
              }
            }
            if (json.hasOwnProperty("1")) {
              if (json['1'].hasOwnProperty('@type')) {
                try {
                  if (json['1']['@type'] === "JobPosting") {
                    let ldjsondata = json['1']
                    return {
                      'html': jobhtml,
                      'jobBody': jobbody,
                      'ldjson': ldjsondata,
                      'errorType': "noError",
                      jobType,
                    };
                  }
                } catch (error) {
                  // throw Error(`Error In if of LDJSON PROPERTY 1: ${error}`)
                }
              }
            }
            return {
              'html': jobhtml,
              'jobBody': jobbody,
              'ldjson': json,
              'errorType': "noError",
              jobType,
            };
          }
        } catch (error) {
          // log(`ERROR IN GETTING HTML PLAINTEXT:${error.toString()}`);
          return {
            'html': '',
            'jobBody': '',
            'ldjson': {},
            'errorType': `ERROR IN GETTING HTML PLAINTEXT:${error}`,
            "jobType": {},
          };
        }

      }, applyStrings);

      if (validString(html) && validString(jobBody)) {
        return {
          html,
          jobBody,
          ldjson,
          "jobType": jobType,
          'errorType': "noError"
        }
      }
      else {
        let { jobhtml, jobType } = await getHtml(page);
        let jobbody = await getPlaintext(page);
        if (validString(jobhtml) && validString(jobbody)) {
          return {
            html: jobhtml,
            jobBody: jobbody,
            ldjson,
            "jobType": jobType,
            'errorType': "noError"
          };
        }
        else {
          return {
            'html': '',
            'jobBody': '',
            'ldjson': {},
            jobType,
            'errorType': `Invalid Html PlainText`
          };
        }
      }
    }
    return {
      'html': '',
      'jobBody': '',
      'ldjson': {},
      'jobType': {},
      'errorType': `response._status:${response._status}`
    };
  } catch (error) {
    // log(`error in getHtmlPlaintext:${error.toString()}`);
    return {
      'html': '',
      'jobBody': '',
      'ldjson': {},
      'jobType': {},
      'errorType': `error in main file ${error.toString()}`
    };
  }
};

async function framesLoading(page) {
  return page.evaluate(() => {
    try {
      // for (const frame of document.querySelectorAll("iframe")) {
      for (const frame of document.querySelectorAll("iframe,span>iframe")) {
        try {
          const frameDocument =
            frame.contentDocument || frame.contentWindow.document;
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

const getHtml = async (page) => {
  var frameUrls = [];
  var html = "", jobType = {}
  console.log("into get html funtions")
  try {
    const frames = (await page.frames().length) >= 1 ? await page.frames() : [];
    // log(`Total No of Frames ${frames.length}`)
    let jobTypeStatus = false
    for (const frame of frames) {
      if (frame.url() != "about:blank" && frame.url() != "") {
        try {
          var frameUrl = await frame.url();
          if (frameUrls.indexOf(frameUrl) <= -1) {
            frameUrls.push(frameUrls);
            let { txt, frame_jobType } = await frame.evaluate((applyStrings, jobTypeStatus) => {
              let frame_jobType = {}
              function isArray(obj) {
                return Object.prototype.toString.call(obj) === "[object Array]";
              }
              function arrayCounter(arr) {
                if (!isArray(arr)) {
                  throw new TypeError('Expected an array.');
                }
                var obj = {};
                for (var i = 0; i < arr.length; i++) {
                  if (arr[i] in obj) {
                    obj[arr[i]]++;
                  } else {
                    obj[arr[i]] = 1;
                  }
                }
                return obj;
              };
              function applyString_function(html) {
                try {
                  let html_apply_strings = applyStrings.filter(str => {
                    const pattern = /[|&;$%@:!"()+,→,»0-9]/g;
                    if (str.match(pattern)) return html.match(RegExp('\\b' + str, 'gi'));
                    return html.match(RegExp('\\b' + str + '\\b', 'gi'));
                  })
                  // if (html.toUpperCase().includes("POSITION:")) html_apply_strings.push('POSITION:')	
                  let arrayCount = arrayCounter(html_apply_strings)
                  let joblink = true
                  for (let i in arrayCount) {
                    if (arrayCount[i] > 3) joblink = false;
                  }
                  if (html_apply_strings.length > 0 && joblink) {
                    return { job: "job", html_apply_strings }
                  } else {
                    return { job: "noJob", html_apply_strings }
                  }
                } catch (err) {
                  return { job: "noJob", html_apply_strings: [], err: err.toString() }
                }
              }
              function cleanup(node, type) {
                const scripts = [];
                let els = node.getElementsByTagName(type);
                for (let i = els.length - 1; i >= 0; i--) {
                  if (els[i].type && els[i].type.toLowerCase().indexOf('ld+json') > -1) {
                    scripts.push(els[i].innerText.replace(/\t/g, ' ').replace(/ /g, ' '));
                  }
                  els[i].parentNode.removeChild(els[i]);
                }
                return scripts;
              }
              function removeComments(node) {
                if (node.nodeType === 8) {
                  node.parentNode.removeChild(node);
                }
                for (const child of node.childNodes) {
                  removeComments(child);
                }
              }
              cleanup(document.documentElement, "script")
              removeComments(document)
              cleanup(document.documentElement, "noscript")
              //checking jobTypeStatus for each frame and if any frame matches the applykeywords we will mark it as true
              if (!jobTypeStatus) {
                const frameHtml = document.documentElement.outerHTML
                  .replace(/\s+/g, " ");
                frame_jobType = applyString_function(frameHtml)
                if (frame_jobType.job === "job") {
                  jobTypeStatus = true
                }
              }
              cleanup(document.documentElement, "meta")
              cleanup(document.documentElement, "select")
              // cleanup(document.documentElement, "nav")
              cleanup(document.documentElement, "style")
              cleanup(document.documentElement, "button")
              cleanup(document.documentElement, "link")
              cleanup(document.documentElement, "aside")
              cleanup(document.documentElement, "path");
              cleanup(document.documentElement, "svg");
              var html = "";
              try {
                html = document.documentElement.innerHTML
                  .replace(/\s+/g, " ")
                  .replace(/\n/g, " ")
                  .replace(/\t/g, " ");
              } catch (error) {
              } finally {
                return { "txt": html, frame_jobType };
              }
            }, applyStrings, jobTypeStatus);
            if (validString(txt) && txt.indexOf("function(") <= -1) {
              html = html + " " + txt;
            }
            jobType = frame_jobType;
          }
        } catch (error) {
          console.log(error.toString())
        }
      }
    }
  } catch (error) {
    console.log(error.toString());
  } finally {
    return { "jobhtml": html, jobType };
  }
};

const getPlaintext = async (page) => {
  var frameUrls = [];
  var plaintext = "";
  try {
    const frames = (await page.frames().length) >= 1 ? await page.frames() : [];
    // log(`Total No of Frames ${frames.length}`)
    for (const frame of frames) {
      if (frame.url() != "about:blank" && frame.url() != "") {
        try {
          var frameUrl = await frame.url();
          if (frameUrls.indexOf(frameUrl) <= -1) {
            frameUrls.push(frameUrls);
            var txt = await frame.evaluate(() => {
              var plaintext = "";
              try {
                plaintext = document.documentElement.innerText
                  .replace(/\s+/g, " ")
                  .replace(/\n/g, " ")
                  .replace(/\t/g, " ");
              } catch (error) {
              } finally {
                return plaintext;
              }
            });
            if (validString(txt) && txt.indexOf("function(") <= -1) {
              plaintext = plaintext + " " + txt;
            }
          }
        } catch (error) { }
      }
    }
  } catch (error) {
    log(error);
  } finally {
    return plaintext;
  }
};
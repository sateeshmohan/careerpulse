const msg = require("debug")("worker:deleteSocialMediaUrls");
var socialMediaDomains = "whatsapp.com||instagram.com||twitter.com||pinterest.com||googleads.g.doubleclick.net||glassdoor.com||glassdoor.co.in||tumblr.com||facebook.com||youtube.com".split(
  "||"
);
const validUrl = require("valid-url"); //validation of url
var URL = require("url"); //parsing the url

const deleteSocialMediaUrls = (dataset, domain) => {
  try {
    if (new RegExp(socialMediaDomains.join("|"), "gi").test(domain)) {
      //console.log(domain);

      socialMediaDomains = socialMediaDomains.filter(
        (item) =>
          !(
            new RegExp(domain, "gi").test(item) ||
            new RegExp(item, "gi").test(domain)
          )
      );
    }
    // console.log(socialMediaDomains);

    try {
      return dataset.filter(
        (data) =>
          !socialMediaDomains.some((item) =>
            domainGetter(data.url).includes(item)
          )
      );
    } catch (error) {
      msg(`Sorry got the error in deleteSocialMediaUrls:${error.toString()}`);
      return dataset;
    }
  } catch (error) {
    msg(`Sorry got the error in deleteSocialMediaUrls:${error.toString()}`);
    return dataset;
  }
};

//gives domain of a url
const domainGetter = (joburl) => {
  try {
    if (validString(joburl) && validURL(joburl))
      return URL.parse(joburl).hostname;
    return "";
  } catch (error) {
    msg(`error in domain getter:${error.toString()}`);
    return "";
  }
};

const validString = (str) => {
  try {
    if (
      str &&
      typeof str !== "undefined" &&
      str !== undefined &&
      str !== null &&
      str !== ""
    )
      return true;
    return false;
  } catch (error) {
    console.error(error);
    return false;
  }
};

//validate URL
const validURL = (str) => {
  if (validString(str) && validUrl.isUri(str)) {
    return true;
  } else {
    return false;
  }
};
module.exports = {
  deleteSocialMediaUrls,
  domainGetter,
};

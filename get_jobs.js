const lodash = require("lodash");
const crypto = require("crypto");
const { validString } = require("../../utilityFunctions/index");
const R = require("ramda");
const level1_jobSelectors = [
  "/job-detail.aspx?",
  "/jobs#!/",
  ".org/job/",
  "/position/",
  "/echojobs/",
  "/jobs.jsp&",
  "/JobDescription/",
  "/BYYOURSIDEAutismTherapyServices/",
  "req=",
  "/details.asp?jid=",
  "/x/detail/",
  "/apply/",
  "=showJob&",
  "/jobs-board/",
  "/job_details.php?",
  "/jobs.php?",
  "/job-listings-",
  "/openjob/",
  "reqGK=",
  "/jobopenings/",
  "/us-en/careers/jobdetails?",
  "/job-openings/",
  "/applyjob.php",

  "/details/",
  "ref_id=",
  "/en-us/details/",
  "refId=",
  "/view/",
  "ams-careers.com/",
  "/jobs?keyword=",
  "/indeed-jobs/",
  "/jobs?search",
  "/career-opportunities/",
  "/job-details-page?",
  ".com/o/",
  ".com/apply/",
  "/jb/",
  "/show-job-listing/",
  "/OpportunityDetail?",
  "/search-and-apply/",
  "/jobdetails/",
  "/careers/jobdetails/",
  "/careeropportunities/",
  ".applytojob.com/apply/",
  ".hr/p/",
  "/findjobs-details.php?",
  "/positions/",
  "/epostings/index.cfm?",
  "/j/",
  "/search-jobs/JobDetails/",
  "/epostings/index.cfm?fuseaction=app.jobinfo&jobid=",
  "/hr/ats/Posting/",
  "/careers/v2/viewRequisition?org=",
  "/careers/apply/",
  "/x/detail",
  "/current-positions/",
  "/career-openings/",
  "&ApplyToJob=",
  "/viewjob?jk",
  "?job=",
  "/MainInfoReq.asp?",
  "/ats/careersite/",
  "/JobDetails.aspx?__ID=",
  "/JobDetails.aspx?job=",
  "/MainInfoReq.asp?R_ID=",
  "/directjob.html?",
  "/jobs/ViewJobDetails?job=",
  "/jobs.html?hireology_job_id=",
  "/administration-jobs/",
  "/find-jobs/",
  "/mrcit/",
  "?jobID=",
  "/careersection/rgi_external/jobdetail.ftl?job=",
  "/employment/job-opening.php?req=",
  "/career/JobIntroduction.action?clientId=",
  "/careers/all-openings/",
  "/OpportunityDetail?opportunityId=",
  "/search/jobdetails/",
  "/careers/openings?",
  "/viewRequisition?org=",
  "job_details.cfm&cJobId=",
  "/bullhorn-career-portal/",
  "/careers/discover-openings/vacancy/",
  "/job.aspx?job_id=",
  "/jobdetails.aspx?jid=",
  "/ViewJob.aspx?JobID=",
  "/job.jsp&",
  "/PublicJobs/controller.cfm?jbaction=JobProfile&Job_Id=",
  "/jobs?pos=",
  "/jobs/view.php?id=",
  "/job_postings/",
  "/job_board_form?op=view&JOB_ID=",
  "/hot-jobs/",
  "/career-portal/",
  "/search-jobs/",
  "/careersite/JobDetails.aspx?id=",
  "/pJobDetails.aspx?",
  "/info/ItemID/",
  "/details.aspx?jobnum=",
  "/job-detail/#job_id=",
  "/viewjob?t=",
  "/it-jobs-careers/",
  "/Openings/",
  "/careers/requisition.jsp?org=",
  "/pages/career-opportunities",
  "/careers/PipelineDetail/",
  "/jobs/individual-position/?gh_jid=",
  "/careers/?p=job/",
  "/careers-listing/",
  "/Jobs/Details/",
  "/careers?gh_jid=",
  "/jobSearch.jsp?org=",
  "/jobs/search?",
  "/job-seeker/jobs/",
  "/Jobsbridge1/",
  "/vacancyDetailsView.cfm",
  "careers/vacancies/",
  "/careers/job/?job_id=",
  "/job/",
  "/open-opportunities/job-details/?jobcode=",
  "/jobdetail/?id=",
  "/career-details/?jobid=",
  "/job-description/",
  "/content/about/",
  "/careers?p=job/",
  "/job-seeker/job-details/JobCode/",
  "/jobs.html?gh_jid=",
  "/careers/current-openings/?p=job/",
  "/search-jobs/details/?job_id=",
  "/job-detail/",
  "/careersite/1/home/requisition/",
  "?job_listing=",
  "/opportunity/?id=",
  "/opportunity/",
  "=job_listing&",
  "/job_listings.html?gh_jid=",
  "/jobs?gh_jid=",
  "/available-positions?gh_jid=",
  "/career-details?job=",
  "/apply/jobs/details/",
  "/job-details/",
  "/employment-opportunities/",
  "/index.jsp?POSTING_ID=",
  "/job-description.html?",
  "/careers/positions/",
  "/careers/positions/co/data/",
  "/careers-at-inteleos/",
  "/jobDesc.asp?JobID=",
  "/PostingDetails.aspx?pid=",
  "/job_details_page.php?id=",
  "/current-job-openings&B_ID=",
  "/jobdetail.ftl?job=",
  "/career-detail/",
  "/vacancies/",
  "/careers/FolderDetail/",
  "/ShowJob/Id/",
  "/Job-Postings/",
  "/job-openings#op-",
  "/our-careers/",
  "/employment-ir/",
  "/JobDetails.asp?JO=",
  "/content/employment.asp",
  "/PublicJobs/",
  "/rc/clk",
  "/Portals/Portals/JobBoard/JobDetail.aspx?JobIDs=",
  "/careers.asp",
  "&career_job_req_id=",
  "/job-seekers/",
  "/job_detail/",
  "/JobDescription.asp",
  "&JobNumber=",
  "/job/",
  "/jobs/",
  "/job?",
  "/viewdetail.asp?",
  "jobID=",
  "/opportunities/",
  "/Posting/",
  "/JD/",
  "/GetJob/ViewDetails/",
  "/JobDetail/",
  "/DashJobDetail/",
  "?quickFind=",
  "/job-description-page/",
  "?job_id=",
  "/rc/clk?/jobdetail.ftl",
  "/ViewJobDetails",
  "/careers/opportunity/",
  "/ts2__JobDetails?jobId=",
  "/ts2__JobDetails",
  "/jobs/ViewJobDetails",
  "/ShowJob/",
  "_display&id=",
  "/MainInfoReq.asp",
  "/jobdetail",
  "/myjobs/",
  "/myjobs/openjob",
  "/JobPosting/",
  "/careers/detail/",
  "/Search/Apply/all/",
  "/search/job",
  "/DistrictJobPosting/",
  "/epostings/",
  "/job-board/",
  "/viewRequisition?",
  ".showJob",
  "/position-details/?job_id=",
  "/position-details/",
  "/job_details?",
  "/open-jobs.",
  "?jid=",
  "/posts/",
  "/careers-search/",
  "/career-center/?RequirementId=",
  "/jobboard.aspx?action=detail&recordid=",
  "&recordid=",
  ".viewjobdetail&",
  "/postings/",
  "/Views/Applicant/VirtualStepPositionDetails.aspx",
];
var level2_jobSelectors = [
  "/jobs.",
  "/apd-engineering-architecture/",
  "/careersnew/",
  "/view/",
  "/sources/link/",
  "job-",
  "/p/",
  "/careers.",
  "-jobs-",
  "-jobs",
  "-job-",
  "/project/",
  "/structures/",
  "/about/careers/",
  //"/location-details/",
  "/prostaff/",
  "/2020",
  "/2018",
  "/2017",
  "/062",
  "/012",
  "/022",
  "/032",
  "/042",
  "/career/",
  "/employment/",
  "/careers/",
  "?hsLang=en",
];

const getJobs = (all_links) => {
  var jobs = [];
  ////console.table(all_links);

  try {
    if (all_links.length) {
      ////console.log(all_links);
      Array.from(all_links).forEach((element) => {
        ////console.log(all_links);

        ////console.log({ jobSelector, status, link: element.link });
        if (
          element.label &&
          !isNumber(element.label) &&
          element.label.length >= 5 &&
          !/jobs|Page|Next|>>/gi.test(element.label)
        ) {
          element.PMN_HASH = crypto
            .createHash("md5")
            .update(
              `${element["mainparent"] +
              element["parentNode"] +
              element["nodeName"] +
              jobSelector
              }`
            )
            .digest("hex");
          element.jobSelector = jobSelector;
          jobs.push(element);
        }
      });
      jobs = lodash.uniqBy(jobs, "link");
      return jobs;
      var jobLinks = [];
      var jobLinksBucket = "";
      if (jobs.length) {
        var groupByJobs = PARENT_HASH(jobs);
        if (Object.keys(groupByJobs).length >= 2) {
          Object.keys(groupByJobs).map((item) => {
            if (jobLinks.length <= groupByJobs[item].length) {
              jobLinks = groupByJobs[item];
              jobLinksBucket = item;
            }
          });
          if (jobLinks.length >= 10) {
            var finalJobLinks = [];
            var hashWise = PHash(jobLinks);
            //console.table(Object.keys(hashWise));
            Object.keys(hashWise).map((item) => {
              if ((hashWise[item].length / jobLinks.length) * 100 >= 70) {
                finalJobLinks = hashWise[item];
              }
            });
            if (finalJobLinks.length) return finalJobLinks;
          }
          return jobLinks;
        }
        return jobs;
        // Object.keys(groupByJobs).map((item) => {
        //   if (jobLinks.length < groupByJobs[item].length) {
        //     jobLinks = groupByJobs[item];
        //     jobLinksBucket = item;
        //   }
        // });
      }
    }
    return all_links;
  } catch (error) {
    //console.log(`error while scraping:${error.toString()}`);
    return [];
  }
};

const checklinks = function (link, jobSelectors) {
  try {
    if (link && validString(link)) {
      for (const jobSelector of Array.from(jobSelectors)) {
        const lowerCaseLink = link.toLowerCase();
        const lowerCasejobSelector = jobSelector.toLowerCase();
        if (
          lowerCaseLink.indexOf(lowerCasejobSelector) >= 0 &&
          !lowerCaseLink.endsWith(jobSelector) &&
          !lowerCaseLink.endsWith(jobSelector + "#")
        ) {
          //console.log(`${link} && ${jobSelector}`);
          return { jobSelector, status: true };
        }
      }
    }
    return { jobSelector: "", status: false };
  } catch (error) {
    //console.log(`error in checklinks:${error.toString()} `);

    return { jobSelector: "", status: false };
  }
};

function isNumber(str) {
  if (typeof str != "string") return false; // we only process strings!
  // could also coerce to string: str = ""+str
  return !isNaN(str) && !isNaN(parseFloat(str));
}

const PARENT_HASH = R.groupBy(R.prop("PMN_HASH"));
const PHash = R.groupBy(R.prop("P_HASH"));
// const PARENT_HASH = R.groupBy(R.prop("MP_HASH"));

module.exports = {
  getJobs,
};

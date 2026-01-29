const chalk = require("chalk");
var URL = require("url"); //parsing the url
const validUrl = require("valid-url"); //validation of url
var lodash = require("lodash"); //for array operations
var stringSimilarity = require("string-similarity");
const { getAllLinks } = require("../job/get_all_links");
const fs = require('fs');
const path = require('path')
const { PendingXHR } = require("pending-xhr-puppeteer");
const insertHtml = require("../../processors/insertHtml");
const md5 = require("md5");

const inputBoxSelectors = [
  '//div[@class="pagination-page-count"]/input[@class="pagination-current"]',
  '//input[@class="pagination-current"]',
  '//input[@class="sfPaginatorPageSwitch"]',
  '//input[@id="page-number"]',
  '//input[@id="pageNumber_start"]',
  '//div[@class="page"]/input',
  '//input[@class="x-tbar-page-number"]'
];
const nextButtonSelectors = [
  '//li[@class="paginate_button page-item next"]/a[@class="page-link"]',
  '//span[@class="PageLink"]/a[normalize-space(.) = "Next"]',
  '//div[@class="pagination-paging"]/a[@class="next"]',
  '//a[@class="googlePaging next"]',
  '//span[@id="ctl00_ContentPlaceHolder1_ctl00_DataPager1"]/input[@value="Next"]',
  '//div[@class="careers-row-pagination-nav next"]',
  '//input[@title="Next Page"]',
  '//button[@aria-label="Next Page"]',
  '//span[@id="pageright"]',
  '//a[@class="paginationLink paginationLinkNext"]',
  '//a/i[@class="fa fa-arrow-circle-right"]',
  '//button[@class="paginate paginate--right"]',
  '//a[@class="pageNext pagerNavigationLink"]/span[@class="text"]',
  '//a[@class="b_page__pagination-link b_page__pagination-link--next"]',
  '/button[@class=" x-btn-text x-tbar-page-next"]',
  '//input[@id="__Next"]',
  '//div[@class="tablepager"]/a[3]',
  // '//a[@title="Next"]',
  // '//button[@data-test="pagination_next"]',
  // '//button[@class="page-button-next styles__PageButton-c3uh2t-1 gyNmBo"]',
  '//li[@class="pagination__next"]',
  "//div[@id='jobs-main']/div[1]/div[2]/span[2]/button[3]",
  '//a[@class="paginate_button next"]',
  '//button[@class="page-nav-caret p-icon-right-cursor next p-bg-hv-grey70"]',
  "//input[@id='cphBody_btnPageNext']",
  '//li[@class="next_link arrow_links"]/a',
  '//a/span[@class="css-6s8faq e1wiielh4"]',
  // '//div[@class="pager-container-normal"]//ul/li/a[@aria-label="Go to Page 2"]',
  // '//a[@aria-label="Go to Page 2"]',
  '//div[@class="pager-container-normal"]//li[@class="PagedList-skipToNext"]/a[@aria-label="Go to Next Page"]',
  "//a[@class='jp-next']",
  '//a[@id="next"]',
  '//span[@class="jtable-page-number-next-mobile ui-button ui-state-default"]',
  '//li[@id="company-pages-list_next"]/a',
  "//a[@onclick='listPagination(next)']",
  '//a[@class="next paginate_button"]',
  '//input[@class="pagebuttonnext"]',
  '//a[@class="next page-numbers"]',
  "//div[@id='content']/div/p[1]/a[@class='paginationItem ']",
  "//div[@class='sf-c-pagination']/a[@name='btn-next']",
  "//div[@class='jobs_pagination_main']/div[1]/ul/li[8]/a",
  "//button[@class='mat-paginator-navigation-next mat-icon-button']",
  "//a[@class='next']",
  "//div[@class='right_content']/p[22]//a",
  "//a[@id='showMoreJobs']",
  '//input[@alt="Next Page"]',
  '//input[@class="rdpPageNext"]',
  "//input[@id='__Next']",
  '//input[@name="ctl00$cphBody$btnPageNext"]',
  "//div[@id='search-results']/div/div[1]//a[@class='page-nav next']",
  '//div[@class="row footer"]/div[3]/input',
  '//a[@class="googlePaging next"]',
  '//input[@class="nextbtn"]',
  '//a[@class="js-pagination-link-next pagination-style"]',
  '//a[@title="Go to the next page"]',
  '//input[@id="cphBody_btnPageNext"]',
  '//section[@class="jr-row jr-th jr-sel"]//a[@title="Next"]',
  '//input[@id="__Next" and not(contains(@disabled,"disabled"))]',
  '//input[@id="cphBody_btnPageNext"]',
  '//button[@class="next hireology-button btn hireology-button--medium hireology-button--outline"]',
  "//input[contains(@onclick,'submitNextPage')]",
  '//a[@class="showMoreJobs UnderLineLink ng-binding"]',
  '//a[@class="jv-pagination-next"]',
  '//div[@class="reinvent-pagination-next-container"]/a[@class="next-page-btn"]',
  '//a[@class="sf-c-arrow-btn btn btn-default right-arrow"]',
  '//a[@class="searchresultlist__paging__link searchresultlist__paging__link--right "]',
  '//div[@class="e961-9 x-column x-sm x-1-3"]/a',
  '//a[@id="joblist_next"]',
  "//div[@id='divR1']/div/div/table/tbody/tr[23]//a",
  "//div[@class='pagination paging-btm']/a[2]",
  "//div[@class='jobs-container']/div[1]/div[2]/div/a[@class='jp-next']",
  '//div[@class="ats_pagination_block"]/a[@id="j_id0:j_id1:atsForm:j_id152"]',
  '//button[@class="next hireology-button btn hireology-button--medium hireology-button--outline"]',
  '//button[@id="Jobs_PagedJobList_NextLink"]',
  "//div[@id='layout_content']/div[2]/div[1]/ul/li[@class='pagination_next']",
  '//div[@id="ctl00_ContentPlaceHolder1_pnlResults"]//table//a[@id="ctl00_ContentPlaceHolder1_grdOpenJobs_ctl01_btnNext"]',
  '//div[@class="pageContent"]/div/div[2]/div[2]//a[@id="textLink"]',
  // '//li[@class="next"]', //xpath not working
  '//button[@class="mat-paginator-navigation-next mat-icon-button"]',
  "//html//body//form/table[3]/tbody/tr[13]/td/a",
  '//div[@class="pagination-next"]/a',
  "//a[@id='40:_next']",
  "//span[@class='current next']",
  '//div[@class="bottomPagerbar clearfix"]//div[@class="pages"]/a[@id="JobSearchResults:j_id137:j_id138:enhancedSearch:j_id231"]',
  "//div[@class='pagination-container']/ul/li[1]/a",
  "//div[@class='pagination-paging pagination-paging--next']/a",
  '//a[@class="paginate_disabled_next"]',
  "//li[@class='next']/a",
  "//li[@class='next']",
  '//a[@class="PHPLinkPagination nextbtn"]',
  // '//div[@class="pagination mobile"]/a[@class="next pagination-button "]',
  "//a[@id='next-page']",
  "//li[@class='pager-next last']/a",
  "//a[@class='next tracking-added']",
  "//div[@id='main-content']/div[2]/div/table/tbody/tr[3]/td/table/tbody/tr[3]//input[@title='Next Page']",
  "//td[@id='ATSPageNext']/a",
  // "//a[contains(text(),'Next')]",//////////////////////////////////////
  "//div[@class='pagingPanel']/a[1]",
  // '//a[@class="next pagination-button "]',
  // '//a[@class="page-link"] /span[contains(text(), "›")]',
  // '//a[@class="page-link"] /span[contains(text(), "Next")]', 
  "//a[@id='psjobstable_next']",
  "//a[@id='joblist_next']",
  '//div[@id="jobListSummary"]/div[4]/input',
  '//a[@class="prev-next hidden-phone"]',
  '//a[@aria-label="Next Page"]',
  '//div[@class="pager"]/a[@class="pager-next"]',
  '//li[@class="pagination-next page-item"]/a[@class="page-link"]',
  '//button[@data-page="next"]',
  '//a[@aria-label="Go to Next Page"]',
  '//a[@aria-label="Go to next page"]',
  '//button[@aria-label="go to next page"]',
  '//a[@class="showMoreJobs UnderLineLink ng-binding"]',
  '//div[@id="pager_container"]/a[@class="SMALLFONTBold"]',
  '//input[@id="cphBody_btnPageNext"]',
  '//a[@class="pagination-item-link pagination-item-link--next"]',
  '//button[@class="next hireology-button btn hireology-button--medium hireology-button--outline"]',
  '//td[@class="resultsHeaderPaginator"]//div/ul/li[@class="sfPaginatorArrowContainer paginationArrowContainer next"]/a',
  '//span[@class="current next"]',
  '//li[@class="pagination-next"]/a[@aria-label="Next page"]',
  '//a[@ng-click="setPage(currentPage + 1)"]',
  '//a[@class="next page-numbers"]',
  '//ul[@class="pagination"]/li[@class="next"]/a',
  '//li[@class="next"]/a',
  '//div[@class="bottomPagerbar clearfix"]/div/a[@title="Next"]',
  '//ul[@class="pagination"]//a[@id="pagination1"]',
  '//a[@class="PHPLinkPagination nextbtn"]',
  '//a[@id="nextButton"]',
  '//button[@class="pagination__item pagination__item--arrow"]',
  '//a/i[@rel="next"]',

  // '//div[@id="ctl00_ctl00_ContentContainer_mainContent_ctl00_VacancyPager2"]//a[8]',
  // "//a[contains(text(), 'Next >')]",
  // '//div[@class="pagination pagination--bottom clearfix"]/a[5]',
  '//button[@aria-label="Next Page of Job Search Results"]',
  '//a[@id="next-page"]',
  '//li[@class="paginate_button page-item next"]/a',
  // '//a[contains(text(), "→")]',
  '//*[@aria-label="Next page"]',
  '//a[@title="Go to next page"]',
  '//a[@title="Go To Next Page"]',
  '//a[@title="Next page"]',
  '//button[@title="Next Page"]',

  '//a[@aria-label="Next"]',
  '//a[@id="next"]',
  '//a[@class="next_page"]',
  '//a[@class="paginate_button next"]',
  '//a/i[@class="page-next fa fa-angle-right"]',

  "//div[@class='pag-next']/span",

  // '//a/i[@class="fa fa-angle-right"]',//it is effecting to other buttons in other websites

  '//a[@aria-label="Next »"]',
  '//button[@class="btn btn-outline next"]',
  '//a[@id="psjobstable_next"]',
  '//li[@class="paginate_button next"]/a',
  '//button[@aria-label="Next page"]',
  '//a[@id="NextPageArrow"]',
  '//div[@class="paging__pages"]/button[@class="page"]',
  '//a[@class="next-page-caret"]',
  '//a[@class="arrow arrow--right"]',
  '//a[@class="paginate_enabled_next"]',
  '//li[@class="pager__item pager__item--next"]/a',
  '//li[@class="jrp-desktop-next-page-element"]/a',
  '//a[@class="next tracking-added active"]',
  '//a[@class="paginate_disabled_next"]',
  // '//a[@class="next pagination-button "]',
  //'input[class="pagination-current"]',
  '//a[@class="h-padding-large next-button-link"]',
  '//span[@aria-label="Next page"]',
  '//div[@class="page"]/input[@id="page_"]',
  '//div[@class="k-pager-wrap k-grid-pager k-widget k-floatwrap"]/a[@title="Next"]',
  '//div[@class="jobInfo"]',
  '//a[@class="page-link next"]',
  '//div[@class="careers-welcome"]/ol[@class="H2L2-ol"]/li',
  '//span[@id="direct_moreLessLinks_listingDiv"]',
  '//div[@class="more-button"]/a[@class="button load-more-jobs"]',
  // '//tr[@class="data-row clickable"]',
  '//a[@class="jv-pagination-next"]',
  '//a[@aria-label="Go to the next page of results."]',
  '//a[@class="rh-pager__link--arrow"]',
  '//button[@class="pagination--page pagination--next"]',
  '//section/div/a[@class="next"]',
  '//a[@aria-label="next"]',
  '//a[@ng-click="selectPage(page + 1, $event)"]',
  `//input[@onclick="javascript:__doPostBack('ctl00$gv','Page$Next');return false;"]`,
  '//span[@aria-label="Next page"]',
  '//div[@class="pagination"]//li/a/i[@class="icon-angle-right"]',
  // "//a[contains(text(), 'next')]",
  '//div[@class="pagerpanel"]/span/span/a[@id="next"]',
  '//div[@class="yui-pg-container"]/a[@class="yui-pg-next"]',
  '//a/span[@class="ui-icon ui-icon-circle-arrow-e"]',
  '//div[@class="pagingButtons"]/a[@class="normalanchor ajaxable scroller scroller_movenext buttonEnabled"]',
  '//div[@class="pagination-paging"]/a[@class="next"]',
  '//div[@id="tm_paging"]/span/div/a[@class="next_page"]',
  '//section[@id="content-main"]/div/section/section[3]/div[2]/a',
  '//div[@id="jobListingList"]/nav/ul/li[9]/a',
  // "//span/a[contains(text(), 'Next')]",
  '//li/a[@title="Go to next page"]',
  '//div[@class="pager"]/a[@class="pager-next-arrow"]',
  '//ul[@class="pager"]/li/a[@class="pageSelector"]',
  '//span[@title="Next page of results"]',
  // "//button[contains(text(), 'Next')]",
  // "//a[contains(text(), 'Next >')]",
  '//a[@class="link2 f12 mR5 floatL"]',
  '//span[@class="jtable-page-number-next-mobile ui-button ui-state-default"]',
  // '//a[@class="paginationItemLast"]',
  '//a[@aria-label="View Next page"]',
  '//a[@ng-click="setCurrent(pagination.current + 1)"]',
  '//a[@ng-click="goToPage(pagination.currentPage + 1)"]',
  // "//a[contains(text(), 'Next Page')]",
  // "//span[contains(text(), 'Next Page')]",
  '//a[@class="prev page-numbers"]//i[@class="fa fa-chevron-right"]',
  '//a[@class="next pagination-button"]//i[@class="fa fa-chevron-right"]',

  '//a[@class="paginateNext"]',
  '//span[@class="chevron-right"]',
  '//img[@alt="Next"]',
  '//a/i[@class="fa fa-caret-right"]',
  // '//a[contains(text(), ">>")]',


  // '//a[contains(text(), ">")]',
  '//a[@aria-label="Next Page"]',
  // '//a[contains(text(), "»")]',
  '//div[@class="ats_pagination_block"]//a[3]',
  '//div[@class="jb--pagination"]/ul/li[last()]/a/i',
  // '//span[contains(text(), "NEXT")]',
  // '//a[@data-page="2"]',
  '//a[@class="arrow next"]',
  '//a[contains(@title,"Show next set of Job Postings")]',
  '//a[@class="button alignright"]',
  '//button[@aria-label="next page"]',
  // '//li[contains(text(),"Next")]',
  '//a[@class="HSTableNavigation"]',
  '//a[@class="pagination-pages__icon pagination-pages__icon--next "]',
  '//a[@title="Next Page"]',
  '//button[@class="inforGridPagingButton  nextPage"]',
  // '//*[text()="Next"]',
  '//button[@class="next-page"]',
  // '//*[text()=">>"]',
  '//div[@class="pagination col-md-9 order-md-first col-12 order-last"]/div[last()]',
  '//h6[@class="next"]',
  "//a[span[contains(text(),'Next page')]]",
  "//span[contains(text(),'Next page')]",
  '//span[@class="glyphicon glyphicon glyphicon-menu-right"]',
  '//li[@class="pages-item pages-item__next"]/a',
  '//a[@class="jscroll-next"]',
  '//a[@class="next fa fa-arrow-circle-right"]',
  '//a[@class="mk-pagination-next pagination-arrows js-pagination-next"]',
  '//div[@class="pagination__arrow pagination__arrow--next"]',
  '//a[@class="next jobsearch-page-numbers"]',
  '//a[@class="nextpostslink"]',
  '//a[@class="next jobsearch-page-numbers"]',
  '//a[@class="pagiArrow right"]',
  // '//ul[@class="pagination pull-right"]/li/a[@class="fa fa-angle-right"]',
  '//div[@data-dir="next"]/i[@class="fa fa fa-arrow-circle-left"]',
  '//div[@id="erec-webform"]//td[@class="navigation_buttons"]/a[contains(text(), ">")]',
  '//a[@class="kuma-icon kuma-icon-triangle-right"]',
  '//li[@class="nextButton"]/a',
  '//a[@class="fa fa-angle-double-right"]',
  '//a[@class="oneabb-external-careers-Pagination-next"]',
  '//span[@class="cw-btn--next slider-arrow-right "]',
  '//a[@class="paginate_button btn btn-paginate btn-med next"]',
  '//button[@class="x-btn-text x-tbar-page-next"]',
  '//li[@class="pageitem next"]/a[@class="pageitemLink"]',
  '//div[@class="pagination"]/ul/li[contains(text(), "Next")]',
  '//button[@class="v-pagination__navigation"]/i[@class="v-icon notranslate mdi mdi-chevron-right theme--light"]',
  '//nav[@class="job-manager-pagination pagination"]//a[normalize-space(.) = "Next"]',
  '//div[@class="scroller-next"]',
  '//button[@ng-click="ctrl.nextPage()"]',
  '//li[@class="job-pagination-item visible-nav-button"]/a',
  '//span[@data-page-number="next"]',
  // '/a[@class="content-block-grid--navigation next icon-arrow_thin_right"]',
  '//button[@class=" x-btn-text x-tbar-page-next"]',
  '//li[@data-page="next"]',
  '//*[@id="next-jobs-page"]/span',
  '//a[contains(@class, "pagination__next")]',
  '//ul[contains(@class, "pagination")]/li/a/i[@class="arrow-right "]',
  '//a[@aria-label="Next page of results"]',
  '//div[@class="pagination"]/div[contains(@class, "next")]/a',
  "//div[contains(@class,'pagination-controls--right')]/button[contains(@class, 'next')]",
  '//a[normalize-space(.) = "Next »"]',
  '//li[contains(@class,"pagination__item--right-arrow")]',
  '//a[@class="page-link" and @data-page="next"]',
  '//*[@id="__Next" and @class="ButtonClassNavigation"]',
  '//li[contains(@class, "page")]/a[@title="Next"]',
  "//div[@class='centerbox-results']/form/h3[1]/span[@class='small-results']/a",
  '//a[@title="Next"]',
  '//a[@class="next"]',
  "//li[@class='next']/a",
  '//a[@name="next"]',
  '//a[@rel="next"]',
  '//li[@class="active-page"]/following-sibling::li/a'
];
const loadMoreSelectors = [
  // '//button[@class="btn-default btn deafult"]/descendant::span[1]',
  '//a[@id="showAll"]',
  '//div[@class="ws-load-more"]',
  '//button[@id="jobs-show-more"]',
  '//input[@class="loadmore"]',
  '//button[@class="button button--default button--loadmore"]',
  '//div[@class="wrapper show-more-wrapper"]',
  '//span[@class="jtable-page-number-next ui-button ui-state-default"]',
  '//button[@id="JobSearchResultsLoadMoreButton"]',
  '//span[@class="show_more"]',
  '//div[@class="container"]/span[@id="load_more"]',
  '//div[@class="job_listings"]/a[@class="load_more_jobs"]',
  '//a[@class="load_more_jobs"]',
  '//a[@id="loadMrJob"]',
  '//section[@id="search-results"]//button[@id="get-more-items"]',
  '//button[@class="fwp-load-more"]',
  '//button[@class="h3 load-more"]',
  '//a[@class="load_more_jobs" and not(contains(@style,"display: none"))]',
  '//h5[contains(@data-bind,"visible") and not(contains(@style,"display: none"))]/descendant::a[1]',
  // '//button[@class="btn btn-outline-primary btn-block"]',
  '//p[@class="load-more-data ng-binding" and not(contains(@style,"display: none"))]',
  '//a[@class="awsm-load-more awsm-load-more-btn"]',
  '//div[@id="recent-jobs"]//a[@class="more-link button"]',
  "//a[@id='LoadMoreJobs']",
  "//button[@id='tile-more-results']",
  // "//button[@class='btn btn-outline-primary']",
  "//p[@class='load-more-data ng-binding']",
  "//a[@class='load_more_jobs']",
  '//p[@class="load-more-data ng-binding"]',
  "//a[@id='load-more']",
  "//li[@class='pager__item']/a",
  '//div[@id="recent-jobs"]/p/a[@class="more-link button"]',
  "//a[@id='button_moreJobs']",
  '//button[@class="alm-load-more-btn more"]',
  '//button[@class="overview-container__showmore"]',
  "//button[@class='more']",
  '//a[@class="load_more_jobs"]',
  '//a[@id="button_moreJobs"]',
  '//button[@class="btn btn-default btn-block search-card-container__btn-load-more w-100"]',
  // "//button[@class='btn btn-outline-primary btn-block']",
  "//button[@id='load-more']",
  '//button[@onclick="loadmore();"]',
  '//a[@id="load-more"]',
  '//a[@class="next-posts-link load_more_jobs"]',
  "//a[@id='loadMore']",
  "//div[@id='loadmore']/a",//added on 12aug
  "//button[@class='button-red button-expand js-trigger-expand see-more-insights']",
  '//input[@value="Load More"]',
  "//input[@id='loadMoreButton']",
  '//div[@id="Opportunities"]/div[4]/div/h5/a[@id="LoadMoreJobs"]',
  '//div[@class="blog-pagination1"]/a[@class="next-posts-link load_more_jobs"]',
  '//div[@id="listings"]/span/div/button',
  // '//button[@class="btn btn-outline-primary btn-block"]',
  '//div[@class="center bloc"]/a[@class="btn btn-clear btn-clear-blue b-i"]',
  '//div[@class="jobs-btn-wrap"]/button',
  '//button[@id="loadMore"]',
  // '//div[@id="loadmore"]',//added
  '//div[@class="JobPagerButton"]/div/a[@class="primaryButton"]',
  '//div[@id="careers"]/a[@id="load-more"]',
  '//a[@id="LoadMoreJobs"]',
  '//span[@class="loadMore"]',
  '//a[@class="button-secondary button-secondary--red job-board__job-list__load-more"]',
  '//li[@class="pager-show-more-next first last"]/a',
  '//a[@class="load_more_jobs"]/strong',
  '//div[@id="show-more-button"]/a',
  '//div[@id="load-jobs-btn"]/a/button',
  '//a[@class="c-button js-filter-careers"]',
  '//p[@class="load-more-data ng-binding"]',
  // '//div[@class="jtable-bottom-panel ui-state-default"]//span[@aria-label="Search results pagination"]/span[@class="jtable-page-number-next ui-button ui-state-default"]',
  '//a[@title="More Jobs"]',

  // '//input[@class="button"]',
  '//button[@class="load-more btn btn__white"]',

  "//a[@class='pagination-show-all']",
  "//div[@id='bottomBranding']/div[1]/p/a",
  "//span[@class='button lazy']",
  '//div[@class="searchResults"]//input[@class="button"]',
  '//div[@id="bottomBranding"]/div[1]/p/a[@class="view-all-health"]',
  '//button[@class="button_round_plus_reveal"][@data-cache="job"]',
  '//div[@id="loadmore"]'
];
const level1RegexSelectors = [
  /Showing Jobs (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Showing Job (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Search Results Page (\d{1,5}) of (\d{1,5})/gi,
  // /Page (\d{1,5}) of (\d{1,5})/gi,
  /Showing (\d{1,5}) to (\d{1,5}) of (\d{1,5}) results/gi,
  // /Showing items (\d{1,5}) - (\d{1,5})/gi,
  /Showing (\d{1,5})-(\d{1,5}) of (\d{1,5})/gi,
  /Job Openings (\d{1,5})-(\d{1,5}) of (\d{1,5})/gi,
  /Displaying (\d{1,5})-(\d{1,5}) of (\d{1,5})/gi,
  /Displaying: (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  // /Current Job Listings (\d{1,5}) Total Jobs/gi,
  /Showing (\d{1,5}) of (\d{1,5}) opportunities/gi,
  /Showing (\d{1,5}) of (\d{1,5})/gi,
  /Showing (\d{1,5}) - (\d{1,5}) of (\d{1,5}) total Positions/gi,
  /Showing (\d{1,5}) of (\d{1,5}) jobs/gi,
  /Showing (\d{1,5}) of (\d{1,5}) job/gi,
  /Showing (\d{1,5})-(\d{1,5}) of (\d{1,5})/gi,
  /Showing positions (\d{1,5}) to (\d{1,5}) of (\d{1,5})  matches/gi,
  /Showing (\d{1,5}) to (\d{1,5}) of (\d{1,5}) jobs/gi,
  /Showing (\d{1,5}) to (\d{1,5}) of (\d{1,5})/gi,
  /Showing (\d{1,5})–(\d{1,5}) of (\d{1,5}) results/gi,
  /(\d{1,5}) of (\d{1,5}) Job Opportunities/,
  // /Page (\d{1,5}) of (\d{1,5})/gi,
  /(\d{1,5})-(\d{1,5}) of (\d{1,5}) Next/gi,
  /(\d{1,5}) to (\d{1,5}) of (\d{1,5}) result/gi,
  /Viewing (\d{1,5})-(\d{1,5}) of (\d{1,5}) jobs/gi,
  /viewing (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Viewing (\d{1,5}) – (\d{1,5}) of (\d{1,5}) jobs/gi,
  /Displaying (\d{1,5})-(\d{1,5}) of (\d{1,5})/gi,
  /Displaying (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Showing (\d{1,5}) to (\d{1,5}) of (\d{1,5}) entries/gi,
  /Job Openings (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Displaying (\d{1,5}) - (\d{1,5}) of (\d{1,5}) in total/gi,
  /Showing (\d{1,5}) to (\d{1,5}) of (\d{1,5}) job openings/gi,
  /Results (\d{1,5}) – (\d{1,5}) of (\d{1,5})/gi,
  /Showing (\d{1,5})-(\d{1,5}) of (\d{1,5}) jobs/gi,
  /(\d{1,5}) to (\d{1,5}) of (\d{1,5}) jobs that match your criteria/gi,
  /Search Results	First  (\d{1,5})-(\d{1,5}) of (\d{1,5})   Last/gi,
  /Showing (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Jobs (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Page: (\d{1,5}) of (\d{1,5})/gi,
  /(\d{1,5}) - (\d{1,5}) of (\d{1,5}) Records/gi,
  /(\d{1,5}) of (\d{1,5}) Job Opportunities/gi,
  /(\d{1,5}) - (\d{1,5}) of (\d{1,5}\,\d{1,5})/gi,
  /(\d{1,5}) - (\d{1,5}) of (\d{1,5}) Jobs/gi,
  /(\d{1,5})- (\d{1,5}) of (\d{1,5}) Jobs/gi,
  /(\d{1,5}) - (\d{1,5}) of (\d{1,5}) items/gi,
  // /(\d{1,5}) – (\d{1,5}) of (\d{1,5})/gi, //failing for one url  https://optioncare.com/work-with-us/careersearch/
  /(\d{1,5})-(\d{1,5}) of (\d{1,5})/gi,
  /Jobs (\d{1,5}) to (\d{1,5}) of (\d{1,5})/gi,
  /Record (\d{1,5})-(\d{1,5}) of (\d{1,5})/gi,
  /Record (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Showing (\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
  /Page (\d{1,5}) out of (\d{1,5})/gi,
  /Displaying (\d{1,5}) to (\d{1,5}) of (\d{1,5})/gi,
  /(\d{1,5}) - (\d{1,5}) Results of (\d{1,5})/gi,
  /(\d{1,5}) to (\d{1,5}) of (\d{1,5})/gi,
  /(\d{1,5}) - (\d{1,5}) of (\d{1,5})/gi,
];
const level2RegexSelectors = [
  // /Search Results:(\d{1,5}) jobs/gi,
  // /Jobs in US [|] (\d{1,5}) Jobs/gi,
  // /found (\d{1,5}) jobs matching/gi,
  // // /TOTAL JOBS: (\d{1,5})/gi,
  // /Results: (\d{1,5}) Jobs/gi,
  // /(\d{1,5}) Live Results/,
  // /(\d{1,5}) RESULTS FOR JOBS AT/gi,
  // /(\d{1,5}) active jobs/gi,
  // /(\d{1,5}) records found/gi,
  // /(\d{1,5}) Job Opportunities/gi,
  // /found (\d{1,5}) jobs/gi,
  // /(\d{1,5}) RESULTS FOUND/gi,
  // /(\d{1,5})+ jobs/gi,
  // /(\d{1,5})(.*) results/gi,
  // /(\d{1,5})(.*) jobs/gi,
  // /(\d{1,5})(.*) jobs found/gi,
  // /(\d{1,5})(.*) jobs matched/gi,
  // /Returned (\d{1,5})(.*) results/gi,
  // /(\d{1,5})(.*) Live Results/gi,
  // /view results (\d{1,5})/gi,
  // /job listings (\d{1,5})/gi,
  // /Found (\d{1,5}) matching/gi
];
var jobSelectors = [
  "jobID=",
  "/AroghiaGroup/",
  "/jobdetail.php?jobid=",
  "/jobs#!/",
  "/BYYOURSIDEAutismTherapyServices/",
  "req=",
  "http://bradleyharris.net/",
  "/details.asp?jid=",
  "/x/detail/",
  "/apply/",
  "/job-listings-",
  "reqGK=",
  "/jobopenings/",
  "/us-en/careers/jobdetails?",
  "/job-openings/",
  "job-",
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
  "/employment/",
  "/current-positions/",
  "/JobDetails.aspx?__ID=",
  "/JobDetails.aspx?job=",
  "/MainInfoReq.asp?R_ID=",
  "/jobs/ViewJobDetails?job=",
  "/jobs.html?hireology_job_id=",
  "/administration-jobs/",
  "/find-jobs/",
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
  "careers/vacancies/",
  "/careers/job/?job_id=",
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
  "/?page_id=",
  "/careersite/1/home/requisition/",
  "/job_listings.html?gh_jid=",
  "/jobs?gh_jid=",
  "/available-positions?gh_jid=",
  "/career-details?job=",
  "/apply/jobs/details/",
  "/job-details/",
  "/employment-opportunities/",
  "/index.jsp?POSTING_ID=",
  "/job-description.html?",
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
  "/Portals/Portals/JobBoard/JobDetail.aspx?JobIDs=",
  "/careers.asp",
  "/job-seekers/",
  "/job_detail/",
  "/JobDescription.asp",
  "&JobNumber=",
  "/jobs/",
  "/job/",
  "/job?",
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
  "-jobs-",
  "-job-",
  "/viewRequisition?",
  ".showJob",
  "/position-details/?job_id=",
  "/position-details/",
  "/careers/development/",
  "/careers/partner-co-investor-relations/",
  "/careers/onsite-property-management/",
  "/careers/finance-capital-markets/",
  "/careers/human-resources/",
  "/careers/compliance/",
  "/career-center/?RequirementId=",
  "/jobboard.aspx?action=detail&recordid=",
  "&recordid=",
  "/postings/",
  "/Views/Applicant/VirtualStepPositionDetails.aspx",
];
const popupcloseselectors = [
  "//*[text()='X']",
  "//button[contains(text(), 'Close')]",

  '//a[@id="hs-eu-decline-button"]',
  '//a[@id="band-cookies-ok"]',
  '//a[@id="cn-accept-cookie"]',
  // '//i[@class="fa fa-times"]',
  '//button[@class="leadinModal-close"]',
  '//i[@class="spu-icon spu-icon-close"]',
  '//a[@class="collapse"]',
  '//a[@class="cc_btn cc_btn_accept_all"]',
  '//a[@class="sqs-popup-overlay-close"]',
  '//a[@aria-label="deny cookies"]',
  '//button[@title="Accept Cookies"]',
  '//a[@class="cc-btn cc-dismiss"]',
  '//a[@class="button w-button"]', //may affect other buttons
  '//a[@id="CybotCookiebotDialogBodyButtonDecline"]',
  '//i[@class="eicon-close"]',
  '//a[@title="close"]',
  '//*[text()="ACCEPT"]',
  '//div[@id="closeBTN"]',
  '//button[@class="btn btn-large btn-primary no-restrict-focus accept-use-of-cookies"]',
  '//div[@class="hide"]',
  '//button[@class="secondary"]',//may affect other buttons
  '//span[@class="hustle-icon-close"]',
  '//i[@class="icon-cross"]',
  '//a[@id="hs-eu-confirmation-button"]',
  '//button[@class="mgbutton moove-gdpr-infobar-allow-all"]',
  '//button[@id="closeAlert"]',
  '//div[@id="newsletter__close"]',
  '//*[text()="Got it!"]',
  '//*[text()="No Thanks"]',
  '//button[@id="notification_close"]',
  '//a[text()="Close"]',
  '//a[@class="close-reveal-modal"]',
  '//i[@class="icon icon-check-mark"]',//may affect other buttons
  '//a[@class="cn-close-icon"]',
  '//a[@id="cookie_action_close_header"]',
  '//button[@class="close-button"]',
  '//div[@class="fixedBar-button"]',//may affect other buttons
  '//button[@title="Close"]',
  '//button[@id="catapultCookie"]',
  '//span[@class="ui-icon ui-icon-closethick"]',
  '//div[@id="ct-ultimate-gdpr-cookie-accept"]',
  '//a[@class="cn-set-cookie cn-button bootstrap button"]',
  '//a[@class="cookieConsentCloseButton"]',
  '//div[@class="livechat_close"]',
  '//button[@class="agree-button"]',
  '//div[@class="global-alert-close"]',
  '//a[@id="close-icon"]',
  '//button[@class="cookie-banner-btn"]',
  '//button[@aria-label="Click OK to Agree to our use of cookies"]',
  '//a[@aria-label="dismiss cookie message"]',
  '//button[@class="sqs-cookie-banner-v2-accept"]',
  // '//*[text()="OK"]',  //due to location
  '//button[@id="ccc-reject-settings"]',
  '//*[text()="I Accept"]',
  '//input[@id="cookie-agree"]',
  '//div[@class="xoo-modal__close"]',
  '//a[text()="Allow cookies"]',
  '//input[@id="elc-accept-link"]',
  // '//button[@type="button"]', //due to other tags getting selected
  '//a[@class="ml-popup-close"]',
  '//div[@class="CTA-close-Icon"]',
  '//a[@class="icon-close"]',
  '//button[@id="btn-cookie-allow"]',
  '//a[@class="close-button py-2 px-4"]',
  '//button[@class="js-pushowl-no-button pushowl-optin__button pushowl-optin__no-button"]',
  '//div[@id="bio_ep_close"]',
  '//a[@id="gdpr-dismiss-button"]',
  '//div[@class="x"]',
  '//div[@id="closenotice"]',
  '//a[@id="acceptAllButton"]',
  '//div[@class="cookies show"]',
  '//button[@class="theme-ButtonV1__button--2QNQ_ theme-ButtonV1__foreground__white--3dF9E theme-ButtonV1__border__white--2arEY theme-CookiesBanner__button--1z1nB"]',
  '//button[@class="onetrust-close-btn-handler onetrust-close-btn-ui banner-close-button onetrust-lg ot-close-icon"]',
  '//button[@class="close"]',
  '//div[@class="wpfront-close"]',
  '//button[@id="accept_privacy_sticky"]',
  '//button[@class="bootbox-close-button close"]',
  '//div[@class="DesktopAdhesionAd__Close-sc-1x4l9qb-1 kLewwL"]',
  '//a[@id="cookieClose"]',
  '//button[@class="pum-close popmake-close"]',
  '//a[@aria-label="allow cookies"]',
  '//button[@class="ab-message-button"]',
  '//div[@id="icon-close"]',
  '//*[@id="cn-close-notice"]',
  '//button[@class="pull-right btn btn-link dismiss-link"]',
  '//button[@class="cookies-consent-bar__closer js-cookies-consent-bar__closer"]',

  '//*[text()="I Agree"]',
  '//*[contains(text(),"Yes, I agree")]',
  '//button[@class="btn btn-primary consumer-privacy-banner-button flex-shrink-0 w-auto mt-3 mt-sm-0 ml-sm-3 mb-sm-3"]',
  '//a[@aria-label="Continue to use cookies"]',
  '//button[@class="agree-button eu-cookie-compliance-default-button"]',
  '//button[@id="onesignal-popover-cancel-button"]',
  '//*[text()="Continue"]',
  '//*[text()="Accept All Cookies"]',
  '//a[@class="cookie-close"]',
  '//*[@id="fancybox-close"]',
  '//*[text()="×"]',
  '//*[text()="Accept"]',
  '//*[text()="Accept Cookies"]',
  '//*[text()="x"]',
  '//*[text()="I AGREE TO ALL"]',
]

//Logging
const log = (msg) => {
  const msgColor = chalk.keyword("yellow");
  console.log(
    "\n-----------------------------------------------------------\n"
  );
  console.log(msgColor(msg + "\n"));
  console.log(
    "\n-----------------------------------------------------------\n"
  );
};

//waiting time
const sleep = async (ms) => {
  await new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

//gets the plaintext according to frames
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

//checks whether the xpath is present in which frame
const selectorCheck = async (page, selectors) => {
  try {
    var traverseSelector = await getSelector(page, selectors);
    if (!traverseSelector) {
      const frames = (await page.frames()) || [];
      for (const frame of frames) {
        if (frame.url() != "about:blank" && frame.url() != "") {
          try {
            traverseSelector = await getSelector(frame, selectors);
            if (traverseSelector) {
              return {
                page: frame,
                selector: traverseSelector,
              };
            }
          } catch (error) {
            log(`error in selectorCheck inside frame:${error.toString()}`);
          }
        }
      }
      return {
        page: page,
        selector: "",
      };
    }
    return {
      page: page,
      selector: traverseSelector,
    };
  } catch (error) {
    log(`error in selectorCheck :${error.toString()}`);
    return {
      page: page,
      selector: "",
    };
  }
};

//checks whether the selector is present in the page or not
const getSelector = async (page, selectors) => {
  try {
    for (const selector of selectors) {
      const linkHandlers = await page.$x(selector);
      if (linkHandlers.length > 0) {
        return selector;
      }
    }
    return "";
  } catch (error) {
    console.log("error:getSelector" + error);
    return "";
  }
};

//gives domain of a url
const domainGetter = (joburl) => {
  try {
    if (validString(joburl) && validURL(joburl))
      return URL.parse(joburl).hostname;
    return "";
  } catch (error) {
    log(`error in domain getter:${error.toString()}`);
    return "";
  }
};

//Check for Valid string
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
    log(error);
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

//isJobs Logic
const pageManager = async (page, joburl, pattern = false) => {
  try {
    var jobs = [];
    //added for domain csod static way
    let currentUrl = page.mainFrame().url();
    if (domainGetter(currentUrl).indexOf("workday") >= 0) {
      let required_apply_link = []
      jobs = await page.$$(
        `li[data-automation-id="compositeContainer"] div[data-automation-id="promptOption"]`
      );
      let lab = await page.$(
        `div[aria-label="Search Results"] span`
      );
      console.log(`Total Jobs Loaded:${jobs.length}`);
      // await sleep(5000);
      for (const item of jobs) {
        await item.click({
          button: "right",
        });
        await sleep(300);
        let data = await page.evaluate(() => {
          try {
            return {
              link: document.documentElement
                .querySelector(`div[title="Copy URL"]`)
                .getAttribute("data-clipboard-text"),
              label: document.documentElement
                .querySelector(`div[title = "Copy Text"]`)
                .getAttribute("data-clipboard-text"),
            };
          } catch (error) {
            return "error"
          }
        });
        if (data != "error") {
          required_apply_link.push(data);
        }
        await lab.click();
        await sleep(300);
      }
      return required_apply_link
    }
    if (domainGetter(currentUrl).indexOf('csod.com') >= 0) {
      const jobLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('a[data-tag="linkResult"]');
        return Array.from(links)
          .map((link) => {
            const jsFunc = link.getAttribute('href');
            const data = jsFunc.split(',');
            return data.filter((d) => d.indexOf('JobDetails') >= 0);
          })
          .flat()
          .map((j) => j.replace(/"/g, '').trim());
      });
    }

    let all_links = await getAllLinks(joburl, page);
    /* For testing we used images and htmls saved in db. So don't need to commit for prod */

    // try {
    //   let html = await page.content()
    //   let urlHash = md5(currentUrl+Date.now());
    //   await insertHtml(joburl, currentUrl, html,`${urlHash}.png`)
    //   await page.screenshot({ path: `tmp/${urlHash}.png`, fullPage: true });
    //   // fs.appendFile(`${path.join(__dirname, domainGetter(currentUrl))}_allLinks.json`,
    //   //   JSON.stringify(lodash.map(all_links, 'link'), null, 2), (error, data) => {
    //   //     if (error) console.log(error.stack)
    //   //     else console.log("data append")
    //   //   })
    // } catch (e) { }

    if (pattern === true) {
      let noJobLinks = await extractionOfunnecessaryUrl(page);
      //console.log("----------------------noJobLinks")
      //console.log(noJobLinks.length)
      // fs.appendFile(`${path.join(__dirname, domainGetter(currentUrl))}_nojobs_.json`,
      //   JSON.stringify(noJobLinks, null, 2), (error, data) => {
      //     if (error) console.log(error.stack)
      //     else console.log("data append")
      //   })
      // console.log("----------------------noJobLinks")
      all_links = lodash.differenceBy(lodash.uniqBy(all_links, 'link'), noJobLinks, 'link');
    }
    all_links = all_links.filter(obj => {
      return !endsWithUrl(obj.link, joburl)
    })
    // fs.appendFile(`${path.join(__dirname, domainGetter(currentUrl))}_endsWith.json`,
    //   JSON.stringify(lodash.map(all_links, 'link'), null, 2), (error, data) => {
    //     if (error) console.log(error.stack)
    //     else console.log("data append")
    //   })
    // removeLinks = [{
    //     link: "https://jobs.lever.co/allplants/f204e584-50f3-4f69-9ee7-eac4a5c86f54"
    //   },
    //   // {
    //   //   link: "https://jobs.lever.co/allplants/1d528e21-9486-478c-8afe-fa1089710b33"
    //   // },
    // ]
    // console.log(lodash.map(all_links, 'link'))
    // all_links = lodash.differenceBy(all_links, removeLinks, 'link');
    // console.log(lodash.map(all_links, 'link'))

    return all_links;
  } catch (error) {
    log(`Sorry mistake from pageManager:${error.stack}`);
  }
}

//does click operation on the given selector
const click = async (start, page, selector, tries = 0) => {
  try {
    var onClickSelector = await page.$x(selector);
    if (onClickSelector.length >= 1) {
      // tries=tries+1;
      await onClickSelector[0].click()
        .then(() => log('Click done'));
      await sleep(20000);

    }
    //   await page.click(selector)
  } catch (err) {
    log(`error while clicking:${err.toString()}`);
    // if(tries<3){
    //     await sleep(5000);
    // await click(start, page, selector);
    // }
  }
};

// does click of a popup on given selector
const popupclick = async (page, popupcloseselectors) => {
  try {
    for (const iterator of popupcloseselectors) {
      let onClickSelector = await page.$x(iterator);
      try {
        if (onClickSelector.length >= 1) {
          // console.log("iterator....................", iterator)
          // console.log("onClickSelector....................", onClickSelector.length)
          for (let index = 0; index < onClickSelector.length; index++) {
            try {
              const element = onClickSelector[index];
              let beforeClickURI = await page.mainFrame().url();
              await element
                .click()
                .then(() => log("Popup Click Done"))
                .catch(err => log("error while Popup Click", err.toString()))
              let afterClickURI = await page.mainFrame().url();
              if (beforeClickURI !== afterClickURI) {
                logger.info(`uri: ${uri}
          currentURI: ${beforeClickURI}
          `)
                // console.log("url has changed going beforeClickURI\n", beforeClickURI)
                await page.goto(beforeClickURI, {
                  waitUntil: "networkidle2",
                  timeout: 600000
                });

                try {
                  const pendingXHR = new PendingXHR(page);

                  await Promise.race([
                    pendingXHR.waitForAllXhrFinished(),
                    new Promise(resolve => {
                      setTimeout(resolve, 20000);
                    }),
                  ]);
                  // console.log(`Popup click XHR SUCCESSFULL---`);
                } catch (err) {
                  console.log("error in Popup click waitForAllXhrFinished ", err)
                }

              }
              else {
                console.log("checking next selector")
              }

              //check url changing 
              // let checkSelector = await page
              //   .evaluate((el) => {
              //     try {
              //       return el.innerText;
              //     } catch (error) {
              //       return "error";
              //     }
              //   }, element)
              console.log(`waitig for 4 sec`);
              await sleep(4000);
            }
            catch (errpop) {
              log(`error while popup clicking in loop:${errpop.toString()}`);
              continue
            }
          }
          // return true 
        }
      } catch (error) {
        log(`error while popup clicking in loop_one:${errpop.toString()}`);
        continue
      }
    }
    return true
  }
  catch (err) {
    log(`error while popup clicking:${err.toString()}`);
  }
};

//gives total no of pages in a career site
function getThePageNumber(plainText, allLinks) {
  try {
    var jobSelectorsEsc = jobSelectors.map((item) => escapeRegExp(item));
    var jobLinks = allLinks.filter((data) => {
      return (
        new RegExp(jobSelectorsEsc.join("|"), "gi").test(data.link) &&
        !(
          /\d+$/.test(data.label) ||
          (validString(data.label) &&
            /page|mainUrl|Older posts|Â»|last|next/gi.test(data.label))
        )
      );
    });
    jobLinks = lodash.uniqBy(jobLinks, "link");
    // log.info(`Total Jobs Count:${jobLinks.length}`);
    var closestJobCount = 10;
    if (jobLinks.length) {
      var counts = [5, 10, 15, 20, 25, 50, 100],
        goal = jobLinks.length;
      closestJobCount = counts.reduce(function (prev, curr) {
        return Math.abs(curr - goal) < Math.abs(prev - goal) ? curr : prev;
      });
    }
    closestJobCount = closestJobCount / 2;


    var checkPages = {};

    if (validString(plainText)) {
      checkPages = getTheNoOfPages(plainText, level1RegexSelectors);
      checkPages.plainText = plainText;
      checkPages.status = 1;

      if (checkPages.regexSelector != "") {
        checkPages.regexLevel = 1;
        var totalNumberOfPages = getTotalNumberOfPages(
          checkPages,
          closestJobCount
        );
        checkPages.totalNumberOfPages = totalNumberOfPages;
      } else {
        checkPages = getTheNoOfPages(plainText, level2RegexSelectors);
        checkPages.regexLevel = 2;
        checkPages.totalNumberOfPages = -1;
        checkPages.status = 1;
        if (checkPages.regexSelector != "") {
          var totalNumberOfPages = getTotalNumberOfPages(
            checkPages,
            closestJobCount
          );
          checkPages.totalNumberOfPages = totalNumberOfPages;
        }
      }
    } else {
      checkPages.match = "";
      checkPages.regexSelector = "";
      checkPages.regexLevel = -1;
      checkPages.totalNumberOfPages = 1;
    }
  } catch (error) {
    //   log("Error in getThePageNumber:"+error.toString());
    log(`error in getThePageNumber:${error.toString()}`);
  } finally {
    return checkPages;
  }
}

//checks with the regex and gives the matching string
function getTheNoOfPages(plainText, regexSelectors) {
  try {
    for (const regexSelector of regexSelectors) {
      if (regexSelector.test(plainText)) {
        return {
          regexSelector: regexSelector.toString(),
          match: plainText.match(regexSelector)[0].toString(),
        };
      }
    }
    return {
      regexSelector: "",
      match: "",
    };
  } catch (error) {
    log(`error in getTheNoOfPages:${error.toString()}`);
    return {
      regexSelector: "",
      match: "",
    };
  }
}

//no of pages from the regex match string
function getTotalNumberOfPages(params, closestJobCount) {
  var noOfPages = 1;
  // var totalJobs=0;
  try {
    if (params.regexLevel == 1) {
      var match = params.match.toString().replace(/\,/g, "").toLowerCase();
      var regex = params.regexSelector;
      var numberFromMatch = match.match(/\d{1,5}/gi);
      if (numberFromMatch.length == 3) {
        noOfPages = Math.ceil(
          parseInt(numberFromMatch[2]) / parseInt(numberFromMatch[1])
        );
        // totalJobs=(parseInt(numberFromMatch[2])*parseInt(numberFromMatch[1]))
        //log("Total No Of Pages:" + noOfPages);
      } else if (
        numberFromMatch.length == 2 &&
        (match.startsWith("showing") || match.endsWith("opportunities"))
      ) {
        noOfPages = Math.ceil(
          parseInt(numberFromMatch[1]) / parseInt(numberFromMatch[0])
        );
        //log("Total No Of Pages:" + noOfPages);
      } else if (numberFromMatch.length == 2) {
        noOfPages = parseInt(numberFromMatch[1]);
      } else {
        noOfPages = parseInt(numberFromMatch[1]);
        //log("could not get the total no of pages");
      }
    } else {
      var match = params.match;
      var regex = params.regexSelector.toString().toLowerCase();
      var numberFromMatch = match.match(/\d{1,5}/gi);
      noOfPages = Math.ceil(parseInt(numberFromMatch[0]) / closestJobCount);
      //log({"msg":`Total No Of Pages:${Math.ceil(parseInt(numberFromMatch[0]) / 10)}`,"color":"yellow"})
    }
  } catch (error) {
    log("error in getTotalNumberOfPages:" + error.toString());
  } finally {
    return noOfPages;
  }
}
//escape function for the regex
const escapeRegExp = (str) => {
  try {
    if (validString(str))
      return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").trim();
    return str;
    // $& means the whole matched str
  } catch (error) {
    log(error);
    return str;
  }
};

const extractionOfunnecessaryUrl = async (page) => {
  try {
    let dataset = await page.evaluate(async () => {
      const requiredDataSet = [];
      async function getData(node, selector) {
        const items = node.querySelectorAll(selector);
        for (const item of items) {
          const attributes = item.getAttributeNames() || [];
          for (const attribute of attributes) {
            try {
              var attrValue = item.getAttribute(attribute);
              if (attribute == "href" || attrValue.indexOf("/") >= 0) {
                try {
                  var link = new window.URL(
                    attrValue,
                    window.document.URL
                  ).toString();
                  if (attrValue.indexOf("#") == 0) {
                    link = link + attrValue;
                  }
                  requiredDataSet.push({
                    nodeName: item.nodeName,
                    label: item.innerText,
                    link: link,
                    attribute: attribute,
                    attrValue: attrValue,
                  });
                } catch (error) {
                  console.log(error.toString());
                }
              }
              // if(link!=="")
            } catch (error) { }
          }
        }
      }
      getData(document.documentElement, 'div[id="desktop-sidebar"] a'),
        getData(document.documentElement, "body>header a[href]"),
        getData(document.documentElement, "body>header a"),
        getData(document.documentElement, "body>header tr"),
        getData(document.documentElement, "header a[href]"),
        getData(document.documentElement, "header tr"),
        getData(document.documentElement, "left-sidebar a"),

        getData(
          document.documentElement,
          "div[class='footer-wrapper'] a[href]"
        ),
        getData(document.documentElement, "div[class='footer-wrapper'] a"),
        getData(document.documentElement, "div[class='footer-wrapper'] tr"),
        getData(document.documentElement, "footer a[href]"),
        getData(document.documentElement, "footer a"),
        getData(document.documentElement, "footer tr"),
        getData(document.documentElement, "nav a[href]"),
        getData(document.documentElement, "nav a"),
        getData(document.documentElement, "nav tr"),
        getData(document.documentElement, "aside a[href]"),
        getData(document.documentElement, "aside a"),
        getData(document.documentElement, "aside tr");
      return {
        requiredDataSet,
      };
    });
    return dataset.requiredDataSet;
  } catch (error) {
    log(`Error in extractionOfunnecessaryUrl:${error.toString()}`);
    return [];
  }
};

var endwithData = ['/x/openings', '#menu-toggle', 'main', '#filter', '/vacancy-search-results.aspx#', '/vacancy-search-results.aspx#close-search-refine', 'vacancy-search-results.aspx', 'services.php', 'index.php', 'search-jobs-v8/', 'search-apply1/', 'search-apply1', '/overview', 'overview/', 'hudl-applicant-and-candidate-privacy-policy', 'support', 'signup', 'index.shtml', 'hiring-process', 'benefits', 'subscribe/', '.gov/', 'about/', 'careers/', 'jobs-engineer', 'contact.html', 'search/', 'email-alerts/', 'activate-privacy-policy.html', 'privacy-policy/', 'homepage.htm', 'services.html', '/terms-and-conditions.html', '/careers.html', 'login.xhtml', 'faq', 'terms-of-use', 'privacy-notice', 'news', '/contact/', '?searchphrase=', '/staff', '/services', "/feedback", "search.html", 'ViewFAQ.aspx',
  "savedJobs.html", "MyChart/", "save_job/",
  "terms-and-conditions", "/faqs", 'faqs.html', "/career-areas", '/benefits.html',
  "company.html", "#content", "#culture", "policies/terms", "#cookies", "Contact_Us", "/Newsletters",
  ".com/apply/",
  "prem-offline-form", "/research",
  '/search-and-apply/', 'feed.atom', '/create', '/jobs/#', '/jobs/#go', '=Filter+by+Keywords&', 'job_function=-1', 'employment-opportunities/', 'order=ASC', 'irving',
  "careers/#", "/patients", "/forgot", "Research & Innovation", "Patients & Visitors", "terms/", "/refer", '/responsibility', '/terms-conditions', "/services", ".org/", "our-story/", "/our-story", "/privacy-policy", "forms/", 'save_job/', 'open-positions/', 'topjobs/', '/google-translate', '.edu/', '.org/', '/volunteer', '/join', '/privacy/', '/pay-your-bill', '/terms/', '/pay-a-bill', '/submissions', '/about', '/privacy', '/search', '/your-application', '/job_search#', '.ca//', '/contact-emerald/', '/talk-to-us/', '/connect.html',
  '/contact-us.html', '/contact-us', '/contact', '/contact.html', 'contactus', 'aboutus', '/job-opportunities/',
  "/0/",
  "login",
  "employment/#",
  "careers.html#",
  "jobs#",
  "reviews",
  "jobs/#",
  "job-openings",
  "Careers.aspx",
  "application.html",
  "job-openings#",
  "review/",
  "#menu",
  "career.html#",
  "results/#",
  "privacy-policy",
  "create",
  "Privacy.html",
  "join-us/",
  "results/##",
  "/company/",
  "Career.aspx",
  "create/",
  "login/",
  ".edu/",
  "current-opening",
  "current-opening/",
  "/flowers",
  "/privacy/",
  "#purchasing"
]
/*
here are some urls to don't use for patterns 
1) https://jobs.ubs.com/TGnewUI/Search/home/HomeWithPreLoad?partnerid=25008&siteid=5012&PageType=JobDetails&jobid=213587,
2) https://earthjustice.org/about/jobs/38658/foundations-data-specialist
3) 'http://recruiter.eresumex.com/eresumex/viewJobWithoutLogin.do?id=e1dde8fd-911f-41db-b15d-15d47d2b2feb&isFromDashboard=false'
4)  http://www.choisystechnology.com/?post_type=job&p=1116
5)  https://careers.nagra.com/?page=advertisement_display&id=11492
*/
var Patterns = ['/support-us/', '/ListJobs/ByCustom/Andersen-Job-Category/Keyword-', 'employment/search.php?sort=', '/job-openings.php?sort=', 'jb_my_searches&SAVED_SEARCH_ID=', '/jobtype/', '/jobindustry/', '/jobcategory/', 'jb_search_results', 'results_page=', 'jobs?page=', 'com/job-categories', '?pagesize=', '/page-', 'download', '/downloads/', '/introduction/', '/products', 'terms-of-use', 'contact-us', 'product-category', '/sustainability/', 'meet-our-people', 'site-support', 'why-work-us', 'investors-media', '/media-contacts/', '/media-library', 'our-business', '/professional-areas/', '/terms-use/', '/our-stories', 'webSyncID=', '/products/', 'sort_by=', '/creative/', 'view-all-our-open-jobs', 'maps?q=', '&pageno=', 'aboutus', 'contactus', 'sign-in', '/social-responsibility/',
  '/jobs/job-search/', '/postings/all', '/bookmarks?', 'CareerOpportunities.html', '/fr/', '/search?', '/intro?', 'jobsearch.asp', '/product/', '?pr=', '?ss=', 'maps.google', '/ways-to-give/', '/news/', '/volunteer/', 'page_job=', '/jobs/resume', '&pagenum=', '?pageNum=', 'page_jobs=', '/jobsearch.ftl', '/joblist.rss', 'plus.google.com', 'googleads.g.doubleclick.net', 'page_jobs=', '?facetcategory=', '?facetcategory=', '?facetcountry=', '/listings.html', '&pageNum=', 'pages=', '/jobs/in/', 'jobOffset=', '?folderOffset=', '&paged=', '#page-', '?pg=', 'PGNO=', 'startrow=', 'startRow=', '#||||', '|||||', 'pagenumber=', 'Pagenumber=', 'pageNumber=', "/contact-us/", "/community/", "/research/", '//goo.gl/', '//t.co/', '//buff.ly/', 'http://bit.ly/', '//ow.ly/',
  "#tab-",
  "/property/",
  "/property-search/",
  "request-form",
  "com/idx/",
  "/catalog",
  "BrowseAllJobsby",
  "/member/",
  "reqtemplate",
  "GetAQuote&pageName",
  "apply_now?",
  "blog#",
  "-PriceWatch&pageNam",
  "Click&ZoneID",
  "play.google",
  "/for-sale/",
  "manage/optin",
  "/jobs/in/"
]
//endswith function for removing some career links so that we can send mostly job links 
function endsWithUrl(url, joburl) {
  var socialMedia = ['www.apple.com', 'www.whatsapp.com', 'www.facebook.com', 'www.instagram.com',
    'twitter.com', 'www.passtumblr.com',
    'www.google.com', 'www.instagram.com', 'www.youtube.com'
  ];
  // http://jobs.ourcareerpages.com/job/472005?source=MWHConstructors&jobFeedCode=MWHConstructors&returnURL=https://mwhconstructors.com/
  //   here it is the link to  excludeing from ends with .com
  if (url.includes('URL=http')) return false;
  for (let index = 0; index < socialMedia.length; index++) {
    let ele = socialMedia[index]
    if (joburl) {
      if (url.split('/')[2] == ele) {
        if (joburl.split('/')[2] == ele) {
          return false
        }
        // console.log("social media links", url)
        return true
      }
    }
  }
  for (let index = 0; index < endwithData.length; index++) {
    const element = endwithData[index];
    if ((url.toLowerCase()).endsWith(element.toLowerCase())) {
      // console.log(element + " found");
      return true;
    }
  }
  for (let index = 0; index < Patterns.length; index++) {
    const element = Patterns[index];
    if (url.toLowerCase().indexOf(element.toLowerCase()) >= 0) {
      if (element == '/search') {
        if (url.toLowerCase().indexOf('/search/job/') >= 0 ||
          url.toLowerCase().indexOf('/search-and-apply/') >= 0 ||
          url.toLowerCase().indexOf('/search/apply/all/') >= 0 ||
          url.toLowerCase().indexOf('/search-jobs/jobdetails') >= 0) {
          return false;
        }
      }
      // console.log(element + " found");
      return true;
    }
  }
  if (url.split('/').length < 5) {
    if (url.slice(url.lastIndexOf('/'), -1).match(/\d+/i) != null) return false;
    if (url.slice(url.lastIndexOf('/'), -1).includes('#')) return false;
    if (url.slice(url.lastIndexOf('/'), -1).includes('.')) return false;
    if (url.slice(url.lastIndexOf('/'), -1).includes('_')) return false;
    if (url.slice(url.lastIndexOf('/'), -1).includes('-')) return false;
    if (url.slice(url.lastIndexOf('/'), -1).includes('?')) return false;
    if (url.slice(url.lastIndexOf('/'), -1).includes('=')) return false;
    // console.log(url + " found1");
    return true;
  }
  return false;
}

const frameScrolling = async (page, scrollLimit) => {
  try {
    let frameurls = [];
    // page.on('console', msg => {
    //   for (let i = 0; i < msg.args().length; ++i)
    //     console.log(`${i}: ${msg.args()[i]}`);
    // });
    const frames = page.frames();
    for (const frame of frames) {
      if (frame.url() != "about:blank" && frame.url() != "") {
        await infiniteScroll(frame, scrollLimit);
        if (frame && frameurls.indexOf(frame.url()) <= -1) {
          frameurls.push(frame.url());
          console.log(`SCROLL STARTED FOR ${frame.url()}`);
          await frame.evaluate(async (scrollLimit) => {
            for (const node of document.querySelectorAll("*")) {
              console.log(`node.scrollHeight  ${node.scrollHeight}`);
              if (node.scrollHeight >= 3000) {
                // console.log(node.outerHTML);
                await new Promise((resolve, reject) => {
                  try {
                    let totalHeight = 0;
                    let distance = 100;
                    let timer = setInterval(() => {
                      let prevScrollHeight = node.scrollHeight;
                      let prevScrollbarPosition = node.scrollTop;
                      node.scrollBy(prevScrollbarPosition, distance);
                      totalHeight += distance;
                      let afterScrollHeight = node.scrollHeight;
                      let afterScrollbarPosition = node.scrollTop;
                      console.log(
                        prevScrollHeight,
                        afterScrollHeight,
                        prevScrollbarPosition,
                        afterScrollbarPosition,
                        totalHeight
                      );
                      if (
                        totalHeight >= prevScrollHeight ||
                        prevScrollbarPosition === afterScrollbarPosition || totalHeight >= scrollLimit
                      ) {
                        clearInterval(timer);
                        resolve();
                      }
                    }, 400);
                  } catch (error) {
                    console.log(`ERROR FOR SCROLLING:${error.toString()}`);
                  }
                });
              }
            }
          }, scrollLimit);
          console.log(`SCROLL DONE FOR ${frame.url()}`);
          return
        }
      }
    }
  } catch (error) {
    console.log("error at frameScrolling", error.toString())
    return
  }


}

//check whether the page is scroll type or not
const scrollCheck = async (page, joburl, allLinksCount) => {
  try {
    console.log("into scroll check funtion")
    if (domainGetter(joburl).indexOf("workday") >= 0) {
      return true
    }
    //inside scrolling finding
    await frameScrolling(page, 6000);
    await sleep(2000);

    var afterScrollLinks =
      lodash.uniqBy(await pageManager(page, joburl), "link") || [];
    var afterScrollLength = afterScrollLinks.length;
    // log({"msg":`Before Scrolling, AllLinks Count:${JSON.stringify(allLinksCount)}`});
    // log({"msg":`After Scrolling, AllLinks Count:${afterScrollLength}`});
    if (allLinksCount < afterScrollLength) {
      return true;
    }
    return false;
  } catch (error) {
    log(`error in scrolling:${error.toString()}`);
    return false;
  }
};

//makes the page to scroll automatically
const infiniteScroll = async (page, scrollLimit) => {
  try {
    // page.on("console", (msg) => console.log(msg.text()));
    await page.evaluate(async (scrollLimit) => {
      await new Promise((resolve, reject) => {
        var totalHeight = 0;
        var distance = 100;
        var timer = setInterval(() => {
          var scrollHeight = document.body.scrollHeight;
          window.scrollBy(0, distance);
          totalHeight += distance;
          console.log("infiniteScroll...........totalHeight.", totalHeight)
          if (totalHeight >= scrollHeight || totalHeight >= scrollLimit) {
            clearInterval(timer);
            resolve();
          }
        }, 1400);
      });
    }, scrollLimit);
    return
  } catch (error) {
    console.log("error in infiniteScroll", error.toString())
    return
  }
};

//comparition text
const comparisionText = (plaintexts) => {
  if (plaintexts.length >= 3) {
    const uniqs = plaintexts.reduce((acc, val) => {
      acc[val] = acc[val] === undefined ? 1 : acc[val] += 1;
      return acc;
    }, {});
    for (let props in uniqs) {
      if (uniqs[props] >= 3) {
        return true;
      }
    }
  }
  return false;
};

const comparisionLinks = (dataSet1, dataSet2, pattern) => {

  try {
    dataSet1 = lodash.uniqBy(dataSet1, "link");
    dataSet2 = lodash.uniqBy(dataSet2, "link");
    dataSet1 = dataSet1.filter(function (element) {
      if (element.link.includes(pattern) || element.link.includes('?ss=') || element.link.includes('?pr=')) {
        return false
      } else {
        return true
      }
    })
    dataSet2 = dataSet2.filter(function (element) {
      if (element.link.includes(pattern) || element.link.includes('?ss=') || element.link.includes('?pr=')) {
        return false
      } else {
        return true
      }
    })

    let compareSet = lodash.differenceBy(dataSet1, dataSet2, "link");
    // console.log(`Compare dataset Length:${compareSet.length}`);
    if (compareSet.length == 0) return true;
    return false;


  } catch (error) {
    console.log("error in compareDataSet ==" + error);
    return false;
  }
}

function generatrePageNumbers(startingPoint, endPoint) {
  var array = [];
  try {
    for (let index = startingPoint; index <= endPoint; index++) {
      array.push(index.toString());
    }
  } catch (error) {
    log(`error in generatrePageNumbers:${error.toString()}`);
  } finally {
    return array;
  }
}

module.exports = {
  log,
  sleep,
  getPlaintext,
  selectorCheck,
  getSelector,
  pageManager,
  getThePageNumber,
  click,
  popupclick,
  scrollCheck,
  validString,
  validURL,
  comparisionText,
  comparisionLinks,
  domainGetter,
  escapeRegExp,
  generatrePageNumbers,
  jobSelectors,
  inputBoxSelectors,
  nextButtonSelectors,
  loadMoreSelectors,
  popupcloseselectors,
  level1RegexSelectors,
  level2RegexSelectors,
  frameScrolling, endwithData, Patterns
};
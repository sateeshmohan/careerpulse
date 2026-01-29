'use strict';

const delay = require('delay');
var rp = require("request-promise");
const logger = require('../datasources/logger');
const phaseTwo = require('../src/phaseTwo');
const config = require('config');
const sites_with_patterns = require('../queues/sites_with_patterns');
// const errorHandler = require('./errorHandler');
const AWS_INSTANCE = config.get('aws_instance');

module.exports = async function (cluster, job) {
    const log = logger.child({
        jid: job.id
    });

    log.info(`Looking for urls ${job.data.uri}`);
    let result;

    try {
        let date_ob = new Date();
        let startTime = date_ob.toISOString().replace(/T/, ' ').replace(/\..+/, '');
        let startTimeStamp = date_ob;
        let ip = await tracker_details("http://checkip.amazonaws.com/");
        if (AWS_INSTANCE){
            let instanceid = await tracker_details("http://169.254.169.254/latest/meta-data/instance-id")
            await sites_with_patterns.client.hmset(`${sites_with_patterns.keyPrefix}:url_fetcher_task_tracker`,`${job.data.uri}:${job.id}`, `${instanceid}||${ip}||${startTime}`)   
            await sites_with_patterns.client.hmset(`${sites_with_patterns.keyPrefix}:url_fetcher_hitting_track`,`${job.data.uri}:${job.id}`, `${instanceid}||${ip}`)
        }
        else{
            await sites_with_patterns.client.hmset(`${sites_with_patterns.keyPrefix}:url_fetcher_task_tracker`,`${job.data.uri}:${job.id}`, `${ip}||${startTime}`)
            await sites_with_patterns.client.hmset(`${sites_with_patterns.keyPrefix}:url_fetcher_hitting_track`,`${job.data.uri}:${job.id}`, `${ip}`)
        }
        var error_check = await sites_with_patterns.client.hget(`${sites_with_patterns.keyPrefix}:url_fetcher_error`,`${job.data.uri}:${job.id}`)
       
        
        console.log(error_check)
        if (error_check == null){
            await sites_with_patterns.client.multi()
            .incr(`${sites_with_patterns.keyPrefix}:picked_sites_with_patterns`)
            .decr(`${sites_with_patterns.keyPrefix}:unpicked_sites_with_patterns`)    
            .exec();
        }
        job.data['url'] = job.data['uri'];

        switch (job.data.type) {
            case "next_button":
            case "scrolling":
            case "load_more":
            case "singlepage":
                result = await phaseTwo(log, cluster, job.data);
                break;
            case "input":
                // if (job.data.totalNumberOfPages == -1) {
                    result = await phaseTwo(log, cluster, job.data);
                // } else {
                //     const allLinks = [];
                //     for (let index = 1; index <= job.data.totalNumberOfPages; index++) {
                //         const result = await phaseTwo(
                //             log,
                //             cluster,
                //             Object.assign({ pageNumber: index }, job.data)
                //         );
                //         if (result && result.allLinks) {
                //             allLinks.push(result.allLinks);
                //         }
                //     }
                //     result = { allLinks };
                // }
                break;
            case "pagination":
                // if (job.data.totalNumberOfPages == -1) {
                    const { uri, pagination, paginationLinks } = job.data;
                    result = await phaseTwo(log, cluster, {
                        url: uri,
                        pagination,
                        paginationLinks
                    });
                // } 
                // else {
                //     console.log("Parallel pagination");
                //     const { uri, pagination, paginationLinks, totalNumberOfPages } = job.data;
                //     result = await phaseTwo(log, cluster, {
                //         url: uri,
                //         pagination, paginationLinks, totalNumberOfPages
                //     });
                // }
                break;
        }
        if(result)
        result["start_time"] = startTimeStamp
        return result;
    } catch (err) {
        log.error(err);
        let errObj = {
            error: err.toString(),
            line_no: err.stack.split('\n')[1],
            file_path: "processor/url_fetcher.js",
            function_name: "url fetcher "
        }
        // await errorHandler(job.data.uri, job.data.uri, errObj);
        

        return err;
    }
};
function tracker_details(URL) {
    try{
        var options = {method: 'GET', url: URL};
        return rp(options);
    }
    catch(error){
        return error;
    }
}
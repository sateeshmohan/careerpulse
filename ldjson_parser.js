'use strict';

const keyfinder = require('keyfinder');

module.exports = async function (ldjson) {

    if (!ldjson || !Object.keys(ldjson).length) {
        return null;
    }

    const hiringOrganization = keyfinder(ldjson, 'hiringOrganization')[0];
    const title = keyfinder(ldjson, 'title')[0];
    const description = keyfinder(ldjson, 'description')[0];
    const identifier = keyfinder(ldjson, 'identifier')[0] || {};
    const datePosted = keyfinder(ldjson, 'datePosted')[0];
    const validThrough = keyfinder(ldjson, 'validThrough')[0];
    const employmentType = keyfinder(ldjson, 'employmentType')[0];
    const jobLocation = keyfinder(ldjson, 'jobLocation')[0] || {};
    const address = keyfinder(ldjson, 'address')[0] || {};
    const baseSalary = keyfinder(ldjson, 'baseSalary')[0];
    const estimatedSalary = keyfinder(ldjson, 'estimatedSalary')[0];
    const occupationalCategory = keyfinder(ldjson, 'occupationalCategory')[0];
    
    // const {
    //     title,
    //     description,
    //     identifier = {},
    //     datePosted,
    //     validThrough,
    //     employmentType,
    //     hiringOrganization,
    //     jobLocation = {},
    //     baseSalary,
    //     estimatedSalary,
    //     occupationalCategory
    // } = result['ldjson'];

    const { value: jobID } = identifier;

    const {
        name: company,
        sameAs,
        logo
    } = hiringOrganization || {};


    const bigmlLabels = {
        'job-title': title || null,
        'job-description-html': description || null,
        'job-description': description || null,
        'job-id': jobID || null,
        'posted-date': datePosted || null,
        'expiration-date': validThrough || null,
        'job-type': employmentType || null,
        company: company || null,
        sameAs: sameAs || null,
        logo: logo || null,
        occupationalCategory: occupationalCategory || null
    };

    if (typeof hiringOrganization === 'string') {
        bigmlLabels['company'] = hiringOrganization;
    }

    if (typeof identifier === 'string') {
        bigmlLabels['job-id'] = identifier;
    }

    if (baseSalary || estimatedSalary) {
        const salaryType = baseSalary ? 'base' : 'estimated';

        const {
            currency,
            value
        } = baseSalary || estimatedSalary;

        const {
            value: salary,
            minValue,
            maxValue,
            unitText
        } = value || {};

        if (salary) {
            bigmlLabels['salary'] = salary || null;
        } else if (minValue || maxValue) {
            bigmlLabels['salary'] = [minValue, maxValue].join("-") || null;
        }

        if (typeof value === 'string' || typeof value === 'number') {
            bigmlLabels['salary'] = value;
        }

        if (bigmlLabels['salary']) {
            bigmlLabels['currency'] = currency || null;
            bigmlLabels['salary-type'] = salaryType;
            if (unitText) {
                bigmlLabels['salary'] = [bigmlLabels.salary, unitText].join(' ') || null;
            }
        }
    }
    if ( typeof(jobLocation) == "object" && Object.keys(jobLocation).length != 0){
        var {
            addressLocality,
            addressRegion,
            postalCode,
            addressCountry
        } = address || {};
        
        addressCountry = typeof(addressCountry) == "object" && !(addressCountry == null) ? addressCountry["name"] : addressCountry
        bigmlLabels['location'] = [addressLocality, addressRegion, addressCountry]
        .filter(v => v && v.toString().trim().length)
        .map(v => v.toString().trim())
        .join(",")
        .trim() || null;
        bigmlLabels['postal-code'] = postalCode || null;
    }
    else if(typeof(jobLocation) == "string") {
        bigmlLabels['location'] = jobLocation;
    }
    else{
        bigmlLabels['location'] = "";
    }

    if (keyfinder(ldjson, 'hiringOrganization')[0]) {
        return bigmlLabels;
    }
}

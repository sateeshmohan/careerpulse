URL = require('url')
const lodash = require('lodash');
const msg = require("debug")("*");
const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: 'base' });
async function processManager(all_links) {
    let jobLinks = all_links
        //single slash urls should be in one cluster
    let singleSlash = jobLinks.filter(url => {
            if (!URL.parse(url).search)
                return (url.split('/').length < 5 && URL.parse(url).pathname.length > 1) || (url.split('/').length <= 5 && !url.slice(url.lastIndexOf('/'), -1).includes('/'))
        })
        //number after slash urls should be in one cluster
    const numberLinks = jobLinks.filter(url => {
        return /^\d+$/.test(URL.parse(url).pathname.split('/')[1])
    })

    singleSlash = lodash.difference(singleSlash, numberLinks)
    jobLinks = lodash.difference(lodash.difference(jobLinks, singleSlash), numberLinks)
    msg("jobLinks")
    const pathsLength = groupLinkbyPathLength(jobLinks)
    msg("pathsLength")
    const querystringOne = lodash.flatten(pathsLength.map(query_length => {
        return groupLinkbyQueryLength(query_length.links, 0)
    }))

    const querystringTwo = lodash.flatten(querystringOne.map(query_length => {
        return groupLinkbyQueryLength(query_length.links, 1)
    }))

    const querystringThree = lodash.flatten(querystringTwo.map(query_length => {
        return groupLinkbyQueryLength(query_length.links, 2)
    }))
    msg("querystringThree")
        //processing remaining links after removing number and single slash URLs
    const groupDomain = lodash.flatten(querystringThree.map(slash_length => {
        return groupLinkbydomain(slash_length.links)
    }))
    msg("groupDomain")
    const slashLinksOne = lodash.flatten(groupDomain.map(obj => {
        return groupLinksbySlash(obj.links, 1)
    }))

    const slashLinksTwo = lodash.flatten(slashLinksOne.map(obj => {
        return groupLinksbySlash(obj.links, 2)
    }))
    const slashLinksThree = lodash.flatten(slashLinksTwo.map(obj => {
        return groupLinksbySlash(obj.links, 3)
    }))
    const slashLinksFour = lodash.flatten(slashLinksThree.map(obj => {
        return groupLinksbySlash(obj.links, 4)
    }))
    msg("slashLinksFour")
    const slashLinksFive = lodash.flatten(slashLinksFour.map(obj => {
        return groupLinksbySlash(obj.links, 5)
    }))
    const slashLinksSix = lodash.flatten(slashLinksFive.map(obj => {
        return groupLinksbySlash(obj.links, 6)
    }))
    const slashLinksSeven = lodash.flatten(slashLinksSix.map(obj => {
        return groupLinksbySlash(obj.links, 7)
    }))
    const slashLinksEight = lodash.flatten(slashLinksSeven.map(obj => {
            return groupLinksbySlash(obj.links, 8)
        }))
        // return { slashLinksEight, slashLinksSeven }

    const slashLinksLast = lodash.flatten(slashLinksEight.map(obj => {
        return groupLinksbySlash(obj.links, -1)
    }))
    msg("slashLinksLast")
    const cluster = commonCluster(slashLinksLast, singleSlash, numberLinks)
    msg("cluster")
    return cluster
}

function groupLinkbydomain(all_links) {
    let grouplinksByDomain = [];
    let domains = [];
    all_links = all_links.sort(collator.compare);
    all_links.map(link => {
        grouplinksByDomain.push({ domain: URL.parse(link).hostname, link: link })
    })
    grouplinksByDomain = lodash.groupBy(grouplinksByDomain, 'domain');
    for (let domain in grouplinksByDomain) {
        domains.push({ domain: domain, links: lodash.map(grouplinksByDomain[domain], 'link').sort(collator.compare) })
    }
    return domains
}

function groupLinkbyPathLength(all_links) {
    let pathLengths = [];
    let slashLengths = [];
    all_links = all_links.sort(collator.compare);
    all_links.map(link => {
        let slashLength = !link.slice(link.lastIndexOf('/'), -1).length == 0 ? link.split('/').length : link.split('/').length - 1
        pathLengths.push({ slashLength: slashLength, link: link })
    })
    pathLengths = lodash.groupBy(pathLengths, 'slashLength');
    for (let pathLength in pathLengths) {
        slashLengths.push({ pathLength: pathLength, links: lodash.map(pathLengths[pathLength], 'link').sort(collator.compare) })
    }
    return slashLengths
}

function groupLinkbyQueryLength(all_links, stringIndex) {
    let queryLengths = [];
    let andLengths = [];
    let oldLabels = [];
    all_links = all_links.sort(collator.compare);
    all_links.map(link => {
        // let label = URL.parse(link).search && !/\d/.test(URL.parse(link).search.split('=')[stringIndex]) ? URL.parse(link).search.split('=')[stringIndex] : "oldLabel"
        let label = URL.parse(link).search && URL.parse(link).search.split('=')[stringIndex] != undefined ? URL.parse(link).search.split('=')[stringIndex].split('&')[0] : 0
        if (URL.parse(link).search && URL.parse(link).search.split('=')[stringIndex] && URL.parse(link).search.split('=')[stringIndex].split('&')[1]) {
            label = URL.parse(link).search.split('=')[stringIndex].split('&')[1]
        }
        queryLengths.push({ label: label, link: link })
    })
    queryLengths = lodash.groupBy(queryLengths, 'label');
    for (let label in queryLengths) {
        if (lodash.map(queryLengths[label], 'link').length > 1) {
            andLengths.push({ label: label, links: lodash.map(queryLengths[label], 'link').sort(collator.compare) })
        } else {
            oldLabels = oldLabels.concat(lodash.map(queryLengths[label], 'link'))
        }
    }
    if (oldLabels.length >= 1) {
        andLengths = andLengths.concat([{ label: "oldLabel", links: oldLabels }])
    }
    return andLengths
}
function groupLinkbyHash(all_links, stringIndex) {
    let queryLengths = [];
    let andLengths = [];
    let oldLabels = [];
    all_links = all_links.sort(collator.compare);
    all_links.map(link => {
        // let label = URL.parse(link).search && !/\d/.test(URL.parse(link).search.split('=')[stringIndex]) ? URL.parse(link).search.split('=')[stringIndex] : "oldLabel"
        let label = URL.parse(link).search && URL.parse(link).search.split('=')[stringIndex] != undefined ? URL.parse(link).search.split('=')[stringIndex].split('&')[0] : 0
        if (URL.parse(link).search && URL.parse(link).search.split('=')[stringIndex] && URL.parse(link).search.split('=')[stringIndex].split('&')[1]) {
            label = URL.parse(link).search.split('=')[stringIndex].split('&')[1]
        }
        queryLengths.push({ label: label, link: link })
    })
    queryLengths = lodash.groupBy(queryLengths, 'label');
    for (let label in queryLengths) {
        if (lodash.map(queryLengths[label], 'link').length > 1) {
            andLengths.push({ label: label, links: lodash.map(queryLengths[label], 'link').sort(collator.compare) })
        } else {
            oldLabels = oldLabels.concat(lodash.map(queryLengths[label], 'link'))
        }
    }
    if (oldLabels.length >= 1) {
        andLengths = andLengths.concat([{ label: "oldLabel", links: oldLabels }])
    }
    return andLengths
}
function groupLinksbySlash(all_links, stringIndex) {
    let slashLabels = [];
    let slash_lengths = [];
    let oldLabels = [];
    all_links.map(link => {
        if (stringIndex >= 0) {
            let label = URL.parse(link).pathname && URL.parse(link).pathname.split('/')[stringIndex] != undefined ? URL.parse(link).pathname.split('/')[stringIndex] : URL.parse(link).pathname.split('/')[1] ? URL.parse(link).pathname.split('/')[1] : 0
            slashLabels.push({ label: label, link: link })
        } else {
            let label = "commonString"
            if (URL.parse(link).path.split('/').slice(stringIndex)[0] == "") {
                label = URL.parse(link).path.split('/').slice(stringIndex - 1)[0]
            } else {
                label = URL.parse(link).path.split('/').slice(stringIndex)[0]
            }
            slashLabels.push({ label: label, link: link })
        }
    })
    slashLabels = lodash.groupBy(slashLabels, 'label');
    for (let label in slashLabels) {

        if (lodash.map(slashLabels[label], 'link').length > 1) {
            slash_lengths.push({ label: label, links: lodash.map(slashLabels[label], 'link').sort(collator.compare) })
        } else {
            oldLabels = oldLabels.concat(lodash.map(slashLabels[label], 'link'))
        }
    }
    if (oldLabels.length >= 1) {
        slash_lengths = slash_lengths.concat([{ label: "oldLabel", links: oldLabels }])
    }
    return slash_lengths
}

function commonCluster(slashLinksLast, singleSlash, numberLinks) {
    let unclustered = []
    let clusters = [];
    let clusterData = [];
    let unclusterResult = []
    numberLinks = numberLinks.length >= 1 ? [{ commonString: "numberLinks", links: numberLinks }] : [];
    // singleSlash = singleSlash.length >= 1 ? [{ commonString: "singleSlash", links: singleSlash }] : [];
    clusters = clusters.concat(numberLinks, slashLinksLast)
    clusters.map(obj => {
        if (obj.links.length == 1) {
            unclustered = unclustered.concat(obj.links)
        } else {
            clusterData = clusterData.concat({ links: obj.links.sort(collator.compare) })
        }
    })
    unclustered = unclustered.concat(singleSlash);
    commonData = groupLinksbySlash(unclustered, 1);
    commonData.map(obj => {
        if (obj.label == "oldLabel") unclusterResult.push(obj.links)
        else clusterData = clusterData.concat({ links: obj.links.sort(collator.compare) })

    })

    return { "clusters": clusterData, "unclustered": lodash.flatten(unclusterResult.sort(collator.compare)) }
}
module.exports = {
    processManager
}
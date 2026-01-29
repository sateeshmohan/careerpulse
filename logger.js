
'use strict';

// Logger emits specified level logs to specified stream
// Custom configurable through config, check config/*.json fiels

const fs = require('fs');

const config = require('config');
const bunyan = require('bunyan');

const {
    name,
    log,
} = config.util.toObject();

const stream = log.path.length
    ? fs.createWriteStream(log.path, { flags: 'a' })
    : process.stdout;

module.exports = (function () {
    return bunyan.createLogger({
        name: name,
        serializers: bunyan.stdSerializers,
        src: true,
        stream,
        level: log.level
    });
})();
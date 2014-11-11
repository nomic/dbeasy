'use strict';

var client = require('./client'),
    pool = require('./pool'),
    util = require('./util');

exports.client = function(options) {
    return client(options);
};

exports.pool = function(options) {
    return pool(options);
};

exports.jsifyColumns = util.jsifyColumns;
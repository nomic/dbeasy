'use strict';

var store = require('./store'),
    client = require('./client'),
    pool = require('./pool'),
    util = require('./util');

exports.store = function(options) {
    return store(options);
};

exports.client = function(pool, options) {
    return client(pool, options);
};

exports.pool = function(options) {
    return pool(options);
};


exports.jsifyColumns = util.jsifyColumns;
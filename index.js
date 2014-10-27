'use strict';

var store = require('./store'),
    client = require('./client');

exports.store = function(options) {
    return store.connect(options);
};

exports.client = function(options) {
    return client.connect(options);
};

exports.jsifyColumns = client.jsifyColumns;
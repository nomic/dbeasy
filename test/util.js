'use strict';
var _ = require('lodash'),
    dbeasy = require("../index.js");


var pgconf = {};
_.each(['host', 'database', 'user', 'port', 'password', 'url'], function(key) {
    var envKey = 'POSTGRES_'+key.toUpperCase();
    if (!process.env[envKey]) return;
    pgconf[key] = process.env[envKey];
});

exports.createDb = createDb;
function createDb(testOpts) {
    return dbeasy.client( _.extend(_.clone(pgconf), testOpts) );
}

exports.createSimpleStore = createSimpleStore;
function createSimpleStore(testOpts) {
    return dbeasy.store( _.extend(_.clone(pgconf), testOpts) );
}

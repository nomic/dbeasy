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
function createDb(poolOpts, clientOpts) {
    return dbeasy.client(
      dbeasy.pool(_.extend(_.clone(pgconf), poolOpts) ),
      clientOpts
    );
}

exports.createStore = createStore;
function createStore(testOpts) {
    return dbeasy.store( _.extend(_.clone(pgconf), testOpts) );
}

exports.createStoreFactory = createStoreFactory;
function createStoreFactory(testOpts) {
    return dbeasy.storeFactory( _.extend(_.clone(pgconf), testOpts) );
}

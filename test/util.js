'use strict';
var _ = require('lodash'),
    dbeasy = require("../index.js");


var pgconf = {
  url: process.env.POSTGRES_URL,
  poolSize: 5
};

exports.createDb = createDb;
function createDb(testOpts) {
  return dbeasy.client(_.extend(_.clone(pgconf), testOpts));
}
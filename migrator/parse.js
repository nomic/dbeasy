'use strict';
var _ = require('lodash'),
    assert = require('assert'),
    Promise = require('bluebird'),
    fs = Promise.promisifyAll(require('fs')),
    path = require('path');

function makeMigration(filePath, date, description, sql) {
  assert(_.isString(filePath));
  assert(_.isString(description));
  assert(!_.isEmpty(description));
  assert(_.isDate(date));
  var migration = {
    path: filePath,
    date: date,
    description: description,
    template: sql
  };
  return migration;
}

module.exports = function(filePath) {
  var filename = path.basename(filePath, '.sql');

  return fs.readFileAsync(filePath).then(function(data) {
    return makeMigration(
      filePath,
      new Date(_.first(filename.split('_'))),
      _.rest(filename.split('_')).join('_'),
      data.toString()
    );
  });

};

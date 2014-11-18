'use strict';
var _ = require('lodash'),
    _str = require('underscore.string'),
    assert = require('assert'),
    Promise = require('bluebird'),
    fs = Promise.promisifyAll(require('fs'));

function makeMigration(date, description) {
  assert(_.isString(description));
  assert(!_.isEmpty(description));
  assert(_.isDate(date));
  var migration = {
    date: date,
    description: description,
    template: ''
  };
  return migration;
}

function setSql(migration, sqlLines) {
  assert(_.isArray(sqlLines));

  // Throw away trailing comments, blanks, etc.
  var lines = _.clone(sqlLines);
  while(lines.length) {
    if (!_.isEmpty(_.last(lines)) && !isComment(_.last(lines))) break;
    lines.pop();
  }

  // If there was nothing but comments and blanks, we keep it.
  migration.template = lines.length ? lines.join('\n') : sqlLines.join('\n');

  return migration;
}

function readlines(filePath) {
  var lines;
  var lineNum = 0;
  var onReady = fs.readFileAsync(filePath)
  .then(function(content) {
    lines = content.toString().split('\n');
  });
  onReady.done();

  return {
    next: function() {
      return onReady.then(function() {
        lineNum += 1;
        return [lines[lineNum - 1], lineNum];
      });
    }
  };
}

function isComment(line) {
  return /^--/.test(line);
}
function isMigrationComment(line) {
  return /^-- *## /.test(line);
}

function parseMigrationComment(line) {
  var match = /## +migration +([^ ]*) +(.*)/.exec(line);
  var date = new Date(match[1]);
  var description = match[2];
  return makeMigration(date, description);
}

function error(lineNum, message) {
  var err = new Error('Migration parse error: line ' + lineNum + ': ' + message);
  Error.captureStackTrace(err, error);
  throw err;
}


module.exports = function(filePath) {

  var lineReader = readlines(filePath);

  function parseMigrations(lineReader) {

    var migrations = [];

    // Discard any junk before first migration header
    return (function next() {
      return lineReader.next()
      .spread(function(line, lineNum) {
        if (line === undefined) {
          return [];
        }
        line = _str.trim(line);
        if (isMigrationComment(line)) {
          return _parseMigrations(line, lineNum);
        }
        // Discard comments before migration header
        if (isComment(line) || _.isEmpty(line)) {
          return next();
        }
        error(lineNum, 'Unexpected characters before migration header: ' + line);
      });
    })();

    // Parse migration starting with header line
    function _parseMigrations(startLine, lineNum) {
      var sqlLines = [startLine];

      return Promise.try(parseMigrationComment, startLine)
      .catch(function(err) {
        error(
          lineNum,
          'Unable to parse migration header: ' + err.message);
      })
      .then(function(migration) {
        // Read lines until EOF or next header
        return (function next() {
          return lineReader.next()
          .spread(function(line, lineNum) {
            if (isMigrationComment(line)) {
              setSql(migration, sqlLines);
              migrations.push(migration);
              return _parseMigrations(line, lineNum);
            }
            if (line === undefined) {
              migrations.push(setSql(migration, sqlLines));
              return migrations;
            }
            sqlLines.push(line);
            return next();
          });
        }());
      });
    }
  }

  return parseMigrations(lineReader);

};

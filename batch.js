'use strict';
var csvStringify = require('csv-stringify'),
    copyFrom = require('pg-copy-streams').from,
    Readable = require('stream').Readable,
    csvStringify = require('csv-stringify'),
    _ = require('lodash'),
    Promise = require('bluebird');

var batch = module.exports;

batch.insertRows = insertRows;
function insertRows(conn, tableName, rows) {
  return new Promise(function(resolve, reject) {
    if (!rows.length) {
      resolve();
      return;
    }

    var columns = '(' + _.map(_.keys(rows[0]), _.snakeCase).join(', ') + ')';
    var dbOutStream = conn.driver().query(
      copyFrom('COPY ' + tableName + ' ' + columns + ' FROM STDIN CSV')
    );
    var csvInStream = Readable();

    csvStringify(rows, {}, function(err, csvData) {
      if (err) {
        reject(err);
        return;
      }
      csvInStream.on('error', reject);
      dbOutStream.on('error', reject);
      dbOutStream.on('end', resolve);
      csvInStream._read = function noop() {};
      csvInStream.push(csvData);
      csvInStream.push(null);
      csvInStream.pipe(dbOutStream);
    });
  });
}

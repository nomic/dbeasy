'use strict';
var pg = require('pg'),
    Pool = require('pg').Pool,
    Promise = require('bluebird'),
    _ = require('lodash'),
    parse = require('pg-connection-string').parse;

module.exports = function(options) {
  var pool = {};
  options = options || {};

  var config = buildConfig(options);

  config.max = options.poolSize || 1;
  config.ssl = options.ssl;
  var pgPool = new Pool(config);
  pgPool = Promise.promisifyAll(pgPool);

  function buildConfig(pgconf) {
    if (pgconf.url) {
      return parse(pgconf.url);
    }

    return _.defaults({}, {
      host: "localhost",
      port: 5432,
      database: "postgres"
    });
  }

  function initPool() {
//    return _useConnection(function() {});
  }

  function _useConnection(fn) {
    return pgPool.connectAsync()
    .spread(function(connection, release) {
      connection = {

        query: Promise.promisify(connection.query, connection),
        driverQuery: _.bind(connection.query, connection)
      };
      return Promise.try(fn, connection)
      .finally(release);
    });
  }

  pool.useConnection = useConnection;
  function useConnection(fn) {
    return _useConnection(fn);
  }

  pool.getStatus = getStatus;
  function getStatus() {
    var pool = pg.pools.all[JSON.stringify(conString)];
    return {
      connection: conString(options),
      pool: {
        size: pool.getPoolSize(),
        available: pool.availableObjectsCount(),
        waiting: pool.waitingClientsCount(),
        maxSize: poolSize
      }
    };
  }

  pool.close = close;
  function close() {
    return pgPool.end();
  }
  return pool;
};

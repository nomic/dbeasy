'use strict';
var pg = require('pg'),
    Promise = require('bluebird');

var pgConnect = Promise.promisify(pg.connect, pg);

module.exports = function(options) {
  var pool = {};
  options = options || {};

  var conString = buildConString(options);

  // The postgres driver depends on this globally accessible module
  // variable to set the pool size.  This is problematic if you have
  // multiple pools at different sizes.  We must be sure to call
  // initPool() right away so that the value is not changed before the
  // pool is setup.
  var oldPoolSize = pg.defaults.poolSize;
  var poolSize = pg.defaults.poolSize = options.poolSize || pg.defaults.poolSize;
  var onPool = initPool();
  pg.defaults.poolSize = oldPoolSize;


  function buildConString(pgconf) {
    if (pgconf.url) return pgconf.url;

    var host = pgconf.host || "localhost";
    var port = pgconf.port || 5432;
    var database = pgconf.database || "postgres";
    var userString = pgconf.user
    ? pgconf.user + (
      pgconf.password ? ":" + pgconf.password : ""
      ) + "@"
    : "";
    return "postgres://"+userString+host+":"+port+"/"+database+"?ssl=on";
  }

  function initPool() {
    return _useConnection(function() {});
  }

  function _useConnection(fn) {
    return pgConnect(conString)
    .spread(function(connection, release) {
      connection = {
        query: Promise.promisify(connection.query, connection)
      };
      return Promise.try(fn, connection)
      .finally(release);
    });
  }

  pool.useConnection = useConnection;
  function useConnection(fn) {
    return onPool
    .then(function() {
      return _useConnection(fn);
    });
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
  return pool;
};
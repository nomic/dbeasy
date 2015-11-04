"use strict";

var pg = require('pg').native,
path = require('path'),
assert = require('assert'),
_ = require('lodash'),
_str = require('underscore.string'),
Promise = require('bluebird'),
fs = Promise.promisifyAll(require('fs')),
handlebars = require('handlebars'),
crypto = require('crypto'),
util = require('./util'),
makePool = require('./pool'),
makeStore = require('./store').store;


function compileTemplate(content) {
  return handlebars.compile(content, {noEscape: true});
}

function error(msg, detail, cause) {
  var err = new Error(msg + ' '
    + JSON.stringify(detail)
    + '\n' + (cause ? cause.stack : ''));
  Error.captureStackTrace(err, error);
  err.name = "DBEasyError";
  return err;
}

function getKeyPath(obj, keyPath) {
  var keys = keyPath.split('.');
  _.each(keys, function(key) {
    obj = _.isObject(obj) ? obj[key] : void(0);
  });
  return obj;
}

//http://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript-jquery
//An integer only hash of a string
function hashCode(str) {
  var hash = 0, i, chr, len;
  if (str.length === 0) return hash;
  for (i = 0, len = str.length; i < len; i++) {
    chr   = str.charCodeAt(i);
    hash  = ((hash << 5) - hash) + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}

function md5(str) {
  var md5sum = crypto.createHash('md5');
  md5sum.update(str);
  return md5sum.digest('hex');
}

clientFn.parseNamedParams = parseNamedParams;
function parseNamedParams(text) {
  var paramsRegex = /\$([0-9]+):\ *([a-zA-Z_\.\$]+)/mg,
  matches,
  params = [];
  while (matches = paramsRegex.exec(text)) {
    params[parseInt(matches[1], 10) - 1] = matches[2];
  }
  return params;
}

clientFn.loadStatement = loadStatement;
function loadStatement(filePath) {
  var statement = {
    preparedName: md5(filePath),
    path: filePath
  };
  if (filePath.slice(-8) === '.sql.hbs') {
    return fs.readFileAsync(filePath)
    .then(function(data) {
      statement.template = compileTemplate(data.toString());
      return statement;
    });
  }
  return fs.readFileAsync(filePath)
  .then(function(data) {
    statement.sql = data.toString();
    statement.params = parseNamedParams(statement.sql);
    return statement;
  });

}

clientFn.loadStatements = loadStatements;
function loadStatements(dirPath) {
  return fs.readdirAsync(dirPath)
  .then(function(filePaths) {
    filePaths = _.filter(filePaths, function(file) {
      return (
       file.slice(-4) === '.sql'
       || file.slice(-8) === '.sql.hbs'
       );
    });

    var statements = {};
    _.each(filePaths, function(filePath) {
      var key = (filePath.slice(-8) === '.sql.hbs')
        ? path.basename(filePath, '.sql.hbs')
        : path.basename(filePath, '.sql');
      statements[_str.camelize(key)] = loadStatement(dirPath + '/' + filePath);
    });

    return Promise.props(statements);
  });
}

module.exports = clientFn;
function clientFn(options) {

  options = _.defaults(options || {}, {
    debug: false,
    egress: null,
    pool: null,
    enableStore: false
  });

  var pool = options.pool || makePool(_.pick(options, 'poolSize', 'url'));
  var logger = (options.logger === 'console')
  ? {
    debug: function() {
      console.log.apply(console, arguments);
    },
    info: console.log,
    warn: console.log,
    error: console.error
  }
  : options.logger
  ? options.logger
  : {debug: _.noop, info: _.noop, warm: _.noop, error: _.noop};

  var client = {};
  client._logger = logger;
  client.__prepared = {};
  client.__templates = {};
  client.__loadpath = options.loadpath || "";

  var egress = options.egress
  ? _.compose.apply(null, options.egress)
  : _.identity;

  egress = options.enableStore
  ? _.compose(
    egress,
    util.removeNulls,
    util.camelizeColumns,
    util.handleMetaColumns)
  : egress;

  function egressAll(rows) {
    return _.map(rows, egress);
  }

  pg = Promise.promisifyAll(pg);


  function Connection(pgConnection) {
    this.pgConnection = pgConnection;
    // Bind all the methods to the instance;
    _.bindAll(this, _.keys(Connection.prototype));
  }

  Connection.prototype.begin = function() {
    var self = this;
    return self.pgConnection.query('BEGIN');
  };

  Connection.prototype.end = function() {
    var self = this;
    return self.pgConnection.query('END');
  };

  Connection.prototype.query = function() {
    return this.queryRaw.apply(this, arguments)
    .then(function(results) {
      return results ? egressAll(results) : null;
    });
  };

  Connection.prototype.queryRaw = function(text, vals) {
    var self = this;
    if (vals !== undefined) { vals = _.rest(arguments); }
    var statements = _.isArray(text) ? text : [text];
    return (function next(results) {
      var statement = statements.shift();
      if (!statement) return results.pop();

      logger.debug([
        '>>>> DBEasy query ',
        JSON.stringify({vals: vals || []}),
        text,
        '<<<<'].join('\n'));
      return self.pgConnection.query(text, vals)
      .then(function(result) {
        results.push(result.rows ? result.rows : null);
        return next(results);
      })
      .catch(function(err) {
        throw error('queryRaw failed', {text: text, vals: vals}, err);
      });
    })([]);
  };

  Connection.prototype.execTemplate = function(tplQuery, templateParams, values) {
    var self = this;
    if (_.isString(tplQuery)) {
      tplQuery = {template: tplQuery};
    }

    var query, cacheKey;
    if (tplQuery.name) {
      cacheKey = tplQuery.name + JSON.stringify(templateParams);
      query = client.__cachedTemplates[cacheKey];
    }
    if (! query) {
      var template = _.isString(tplQuery.template)
        ? compileTemplate(tplQuery.template)
        : tplQuery.template;

      var sql = template(templateParams);
      query = {
        sql: sql,
        params: parseNamedParams(sql)
      };

      // We're only caching templates that are also prepared
      if (cacheKey) {
        // Max length of prepared statement is NAMEDATALEN (64)
        query.name = md5(cacheKey);
        client.__cachedTemplates[cacheKey] = query;
      }
    }

    return self.exec(query, values);
  };

  Connection.prototype.exec = function(query, values) {
    var self = this;
    if (_.isString(query)) {
      query = {sql: query};
      query.params = parseNamedParams(query);
    }
    if (values !== undefined) { values = _.rest(arguments); }

    if (values && !_.isArray(values[0]) && _.isObject(values[0])) {
      //then assume we have named params
      if (!query.params) {
        throw error("Statement does not have named params defined: " + query.sql);
      }
      var namedValues = values[0];
      values = _.map(query.params, function(paramName) {
        var val = getKeyPath(namedValues, paramName);
        return val;
      });
    }

    var statements = query.sql.split(/;\s*$/m);
    return (function next(results) {
      var statement = statements.shift();
      if (!statement) return results.pop();

      var opts = {
        text: statement + ';',
        values: values
      };

      // This will cause the statement to be prepared
      if (query.name) opts.name = results.length + '.' + query.name;

      logger.debug([
        '>>>> DBEasy execute ',
        JSON.stringify(_.omit(opts, 'text')),
        opts.text,
        '<<<<'].join('\n'));
      return self.pgConnection.query(opts).then(function(result) {
        results.push(result.rows ? egressAll(result.rows) : null);
        return next(results);
      })
      .catch(function(err) {
        if (err && err.code) {
          if (err.code.search("22") === 0) {
            // Codes in the 22* range (data exceptions) are assumed
            // to be the client's fault (ie, using an id which
            // is beyond the range representable by bigint).
            // - reference: http://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
            throw error(opts.name || "anonymous query", opts, err);
          } else if (err.code.search("23") === 0) {
            // Integrity constraint violation, just rethrow it
            // and allow higher-lever wrappers to take action.
            throw err;
          } else if (err.code.search("40P01") === 0) {
            //TODO:2013-06-28:gsilk: special logging for deadlocks?
            throw err;
          }
        }
        logger.error([
          '>xx> DBEasy error ',
          err,
          '<xx<'].join('\n'));
        throw error('exec failed', {name: opts.name, sql: opts.text, values: values}, err);
      });
    })([]);
  };

  var useConnection = function(fn) {
    return pool.useConnection(function(pgConnection) {
      return Promise.try(fn, new Connection(pgConnection))
      .catch(function(err) {
        return pgConnection.query("ROLLBACK")
        .then(function() {
          throw err;
        });
      });

    });
  };

  // set the load path
  client.loadpath = function(path) {
    client.__loadpath = path;
  };


  // Execute a previously prepared statement
  client.exec = function(/*statement, params*/) {
    var args = _.toArray(arguments);
    return client.useConnection(function(conn) {
      return conn.exec.apply(conn, args);
    });
  };

  client.execTemplate = function(/*statement, templateParams, params*/) {
    var args = _.toArray(arguments);
    return client.useConnection(function(conn) {
      return conn.execTemplate.apply(conn, args);
    });
  };

  // Execute a sql query string
  client.query = function(/*, params*/) {
    var args = _.toArray(arguments);
    return client.useConnection( function(conn) {
      return conn.query.apply(conn, args);
    });
  };

  // Execute a sql query string and don't run
  // the results through egress
  client.queryRaw = function(/*, params*/) {
    var args = _.toArray(arguments);
    return client.useConnection( function(conn) {
      return conn.queryRaw.apply(conn, args);
    });
  };

  // Returns a promise for work completed within the
  // scope of a single transaction.
  //
  // You supply a function which receives a connection and
  // returns a promise.
  //
  // The transaction has already been opened on the connection,
  // and it will automatically be committed once your promise
  // completes.
  //
  // NOTE: You should only use this function if you require
  // multiple statements to be executed within a single transaction.
  // Generally try to avoid this.  You must understand locking
  // (and deadlocking) in postgres before using this.
  client.transaction = function(workFn) {
    return useConnection( function(conn) {
      conn.begin();

      // Should blow up if result not a promise (not thenable) since
      // this most likely means we forgot to return a promise.
      var working = workFn(conn);
      assert(
        _.isFunction(working.then),
        'Transaction function must return a promise');
      return working
      .then(function(result) {
        return conn.end()
        .then(function() {
          return result;
        });
      });
    });
  };

  client.synchronize = function(lockName, workFn) {
    var lockNum = hashCode(lockName);
    return useConnection( function(conn) {
      return conn.query(
        'SELECT pg_advisory_lock(' + lockNum + ');')
      .then(function() {
        return workFn();
      })
      .finally(function() {
        return conn.query(
          'SELECT pg_advisory_unlock(' + lockNum + ');');
      });
    });
  };

  // Returns a promise for work completed against a connection.
  //
  // You supply a function which receives a connection and
  // returns a promise.
  //
  // The connection is automatically returned to the pool
  // once your promise completes.
  //
  // NOTE: You shouldn't really need to use this.  This is
  // only necessary if you need fine grain control over
  // transactions.
  client.useConnection = function(workFn) {
    return useConnection( function(conn) {
      var working = workFn(conn);
      assert(
        _.isFunction(working.then),
        'Connection function must return a promise');
      return working;
    });
  };

  client.close = function() {
    _.each(pg.pools.all, function(pool, key) {
      pool.drain(function() {
        pool.destroyAllNow();
      });
      delete pg.pools.all[key];
    });
  };

  client.cleansedConfig = function() {
    return _.omit(options, "password", "logger", "egress");
  };

  client.store = function(specName, storeOptions) {
    assert(
      options.enableStore,
      'store not availble: create client with {enableStore: true}');
    return makeStore(client, specName, storeOptions);
  };

  var onSqlPrepared = null;
  client.__prepareSql = function() {
    return onSqlPrepared || client.prepareDir(path.join(__dirname, 'sql'));
  };

  //Make available here for convenience;
  client.loadStatement = loadStatement;
  client.loadStatements = loadStatements;

  return client;
}

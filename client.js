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


function loadQuery(loadpath, fileName) {
    return fs.readFileAsync(loadpath+"/"+fileName).then(function(data) {
        return data.toString();
    });
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

exports.encodeArray = function(arr, conformer) {
    return '{' + _.map(arr, conformer).join(',') + '}';
};

module.exports = function client(options) {

    options = _.defaults(options || {}, {
        debug: false,
        egress: null,
        pool: null,
        enableStore: false
    });

    var pool = options.pool || makePool(_.pick(options, 'poolSize', 'url'));
    var logger = options.logger || {
        debug: function() {
            if (options.debug) console.log.apply(console, arguments);
        },
        info: console.log,
        warn: console.log,
        error: console.error,
    };

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
            util.jsifyColumns,
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

    Connection.prototype.query = function(text, vals) {
        return this.queryRaw(text, vals)
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

    Connection.prototype.execTemplate = function(templateKey, templateParams, values) {
        var self = this;
        var key = templateKey + JSON.stringify(templateParams);

        // Max length of prepared statement is NAMEDATALEN (64)
        var md5sum = crypto.createHash('md5');
        md5sum.update(key);
        key = md5sum.digest('hex');

        if (client.__prepared[key]) {
            return this.exec(key, values);
        }

        var template = client.__templates[templateKey];
        if (! template) {
            throw error("template not found: " + templateKey);
        }
        var text = template(templateParams);
        prepare(key, null, text);
        return self.exec(key, values);
    };

    Connection.prototype.exec = function(key, values) {
        var self = this;
        if (values !== undefined) { values = _.rest(arguments); }
        var prepared = client.__prepared[key];
        if (! prepared) {
            throw error("prepared statement not found: " + key);
        }
        if (values && !_.isArray(values[0]) && _.isObject(values[0])) {
            //then assume we have named params
            if (!prepared.namedParams) {
                throw error("prepared statement does not have named params defined: " + key);
            }
            var namedValues = values[0];
            values = _.map(prepared.namedParams, function(paramName) {
                var val = getKeyPath(namedValues, paramName);
                if (!val && _str.endsWith(paramName, 'Id')) {
                    paramName = paramName.slice(0,-2) + '.id';
                    val = getKeyPath(namedValues, paramName.clice);
                }
                return val;
            });
        }

        var statements = prepared.text.split(/;\s*$/m);
        return (function next(results) {
            var statement = statements.shift();
            if (!statement) return results.pop();

            var opts = {
                name: results.length + '.' + key,
                text: statement + ';',
                values: values,
                types: prepared.types
            };
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
                throw error('exec failed', {key: key, values: values}, err);
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
    client.query = function(sql /*, params*/) {
        var args = _.toArray(arguments);
        return client.useConnection( function(conn) {
            return conn.query.apply(sql, args);
        });
    };

    // Execute a sql query string and don't run
    // the results through egress
    client.queryRaw = function(sql /*, params*/) {
        var args = _.toArray(arguments);
        return client.useConnection( function(conn) {
            return conn.queryRaw.apply(sql, args);
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
            var working = workFn(conn);
            return working.then(function() {
                return conn.end().then(function() {
                    return working;
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
            return workFn(conn);
        });
    };

    client.prepareTemplate = function(key, templateFile, context, types) {
        var reading = loadQuery(client.__loadpath, templateFile);

        var templating = reading.then(function(templateBody) {
            var template = handlebars.compile(templateBody);
            var text = template(context);

            return text;
        });

        var stashing = templating.then(function(text) {
            client.__prepared[key] = {
                name: key,
                text: text,
                types: types
            };

            return key;
        });

        return stashing;
    };

    function compileTemplate(key, path, fname) {
        return loadQuery(path, fname)
        .then(function(text) {
            client.__templates[key] = handlebars.compile(text);
        });
    }

    function prepare(key, types, text, path, fname) {
        var stash = function(text, namedParams) {
            client.__prepared[key] = {
                name: key,
                text: text,
                types: types,
                namedParams: namedParams
            };
        };
        if (text) {
            stash(text, util.parseNamedParams(text));
            return Promise.resolve(key);
        }
        var reading = loadQuery(path, fname)
        .then(function(text) {
            stash(text, util.parseNamedParams(text));
        })
        .then(function() {
            return key;
        }, function(err) {
            throw error("Failed to prepare", {key: key}, err);
        });
        return reading;
    }

    client.prepare = function() {
        var key, text, types, path, fname;
        assert(arguments.length <= 3);
        key = arguments[0];
        if (_.isString(arguments[1])) {
            text = arguments[1];
            types = arguments[2];
        } else if (arguments.length === 2) {
            types = arguments[1];
        }
        if (text) return prepare(key, types, text);

        path = client.__loadpath;
        if (key.slice(-8) === '.sql.hbs') {
            fname = key;
            key = key.slice(0, -8);
            return compileTemplate(key, path, fname);
        }

        if (key.slice(-4) === '.sql') {
            fname = key;
            key = key.slice(0, -4);
        } else {
            fname = key + '.sql';
        }
        return prepare(key, types, text, path, fname);
    };

    client.prepareDir = function(path) {
        return fs.readdirAsync(path)
        .then(function(files) {
            var sqlFiles = _.filter(files, function(file) {
                return (
                       file.slice(-4) === '.sql'
                    || file.slice(-8) === '.sql.hbs'
                );
            });
            return Promise.all(
                _.map(sqlFiles, function(file) {
                    if (file.slice(-8) === '.sql.hbs') {
                        key = file.slice(0, -8);
                        return compileTemplate(key, path, file);
                    }
                    var key = file.slice(0, -4);
                    return prepare(key, null, null, path, file);
                }));
        });
    };

    client.prepareAll = function(/* statements */) {
        return Promise.all(
            _.map(arguments, function(arg) {
                if (_.isString(arg)) {
                    return client.prepare(arg);
                }
                //arrays are used to pass through statement name
                //as well as argument types
                return client.prepare.apply(null, arg);
            })
        ).then( function(results) {
            _.each(results, function(r) {
                logger.debug("prepared statement loaded:", r);
            });
            return results;
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

    return client;
};

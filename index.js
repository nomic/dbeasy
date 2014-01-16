"use strict";

var pg = require('pg')
, assert = require('assert')
, nodefn = require('when/node/function')
, _ = require('underscore')
, fs = require('fs')
, when = require('when')
, whenfn = require('when/function');


function conString(pgconf) {
    return "postgres://"+pgconf.user+":"+pgconf.password+"@"+pgconf.host+":"+pgconf.port+"/"+pgconf.database+"?ssl=on";
}

function loadQuery(loadpath, fileName) {
    return nodefn.call(fs.readFile, loadpath+"/"+fileName+".sql").then(function(data) {
        return data.toString();
    });
}

function error(msg, detail, cause) {
    var err = new Error(msg);
    Error.captureStackTrace(err, error);
    err.name = "DBEasyError";
    err.detail = detail || {};
    if (cause) err.cause = cause;
    return err;
}

exports.envToConfig = function(prefix) {
    var pgconf = {};
    _.each(['host', 'user', 'port', 'password', 'database'], function(key) {
        var envKey = prefix + key.toUpperCase();
        if (!process.env[envKey]) throw new Error('missing configuration: '+envKey);
        pgconf[key] = process.env[envKey];
    });
    return pgconf;
};

exports.encodeArray = function(arr, conformer) {
    return '{' + _.map(arr, conformer).join(',') + '}';
};

exports.create = function(options) {
    var logger = options.logger || {
        info: function(){},
        error: function(){},
    };
    options = options || {};
    var db = {};
    db.__prepared = {};
    db.__loadpath = options.loadpath || "";
    var requestedPoolSize = options.poolSize || pg.defaults.poolSize;
    pg.defaults.poolSize = requestedPoolSize;
    var egress = options.egress
        ? function(rows) {
            return _.map(rows, options.egress);
        }
        : function(rows) { return rows; }

    // connect to the postgres db defined in conf
    var onConnection = function(fn) {
        return nodefn.call(_.bind(pg.connect, pg, conString(options)))
            .then(function(args) {
                var ctx = {};
                var conn = args[0];
                var done = args[1];

                ctx.__conn = conn;
                ctx.begin = function() {
                    conn.query('BEGIN');
                };
                ctx.end = function() {
                    return nodefn.call(_.bind(conn.query, conn), 'END');
                };
                ctx.query = function(text, vals) {
                    if (vals !== undefined) { vals = _.rest(arguments); }
                    return nodefn.call(_.bind(conn.query, conn), text, vals)
                        .then(function(result) {
                            return result.rows ? egress(result.rows) : null;
                        })
                        .otherwise(function(err) {
                            throw error('query failed', {text: text, vals: vals}, err);
                        });
                };
                ctx.queryRaw = function(text, vals) {
                    if (vals !== undefined) { vals = _.rest(arguments); }
                    return nodefn.call(_.bind(conn.query, conn), text, vals)
                        .otherwise(function(err) {
                            throw error('queryRaw failed', {text: text, vals: vals}, err);
                        });
                };
                ctx.exec = function(key, values) {
                    if (values !== undefined) { values = _.rest(arguments); }
                    var prepared = db.__prepared[key];
                    if (! prepared) {
                        throw error("prepared statement not found", {name: key});
                    }
                    var opts = {
                        name: key,
                        text: prepared.text,
                        values: values,
                        types: prepared.types
                    };
                    return nodefn.call(_.bind(conn.query, conn), opts).then(function(result) {
                        return result.rows ? egress(result.rows) : null;
                    })
                    .otherwise(function(err) {
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
                        throw error('exec failed', {key: key, values: values}, err);
                    });
                };

                var rollback = function(err) {
                    return nodefn.call(_.bind(conn.query, conn), "ROLLBACK").then(function() {
                        throw err;
                    });
                };

                return whenfn.call(function() { return fn(ctx); })
                .otherwise(rollback)
                .ensure(done);

            }).otherwise(function(err) {
                throw err;
            });
    };

    // set the load path
    db.loadpath = function(path) {
        db.__loadpath = path;
    };


    // Execute a previously prepared statement
    db.exec = function(statement /*, params*/) {
        var args = _.toArray(arguments);
        return db.onConnection(function(conn) {
            return conn.exec.apply(conn, args);
        });
    };

    // Execute a sql query string
    db.query = function(sql /*, params*/) {
        var args = _.toArray(arguments);
        return db.onConnection( function(conn) {
            return conn.query.apply(sql, args);
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
    db.transaction = function(workFn) {
        return onConnection( function(conn) {
            conn.begin();
            var working = workFn(conn);
            return working.then(function() {
                return conn.end().yield(working);
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
    db.onConnection = function(workFn) {
        return onConnection( function(conn) {
            return workFn(conn);
        });
    };

    db.prepare = function() {
        var key, text, types;
        assert(arguments.length <= 3);
        key = arguments[0];
        if (_.isString(arguments[1])) {
            text = arguments[1];
            types = arguments[2];
        } else if (arguments.length === 2) {
            types = arguments[1];
        }
        var stash = function(text) {
            db.__prepared[key] = {
                name: key,
                text: text,
                types: types
            };
        };
        if (text) {
            stash(text);
            return when(key);
        }
        var reading = loadQuery(db.__loadpath, key).then(stash)
        .then(function() {
            return key;
        }, function(err) {
            throw error("Failed to prepare", {key: key}, err);
        });
        return reading;
    };

    db.prepareAll = function(/* statements */) {
        return when.join(
            _.map(arguments, function(arg) {
                if (_.isString(arg)) {
                    return db.prepare(arg);
                }
                return db.prepare.apply(null, arg);
            })
        ).then( function(results) {
            _.each(results, function(r) {
                logger.info("prepared statement loaded:", r);
            });
            return results;
        });
    };

    db.status = function() {
        var pool = pg.pools.all[JSON.stringify(conString(options))];
        return {
            connection: conString(options),
            pool: {
                size: pool.getPoolSize(),
                available: pool.availableObjectsCount(),
                waiting: pool.waitingClientsCount(),
                maxSize: requestedPoolSize
            }
        };
    };

    db.close = function() {
        _.each(pg.pools.all, function(pool, key) {
            pool.drain(function() {
                pool.destroyAllNow();
            });
            delete pg.pools.all[key];
        });
    };

    return db;
};

"use strict";

var pg = require('pg')
, assert = require('assert')
, nodefn = require('when/node/function')
, _ = require('lodash')
, _str = require('underscore.string')
, fs = require('fs')
, when = require('when')
, whenfn = require('when/function')
, handlebars = require('handlebars');


function conString(pgconf) {
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

function loadQuery(loadpath, fileName) {
    return nodefn.call(fs.readFile, loadpath+"/"+fileName).then(function(data) {
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

function parseNamedParams(text) {
    var paramsRegex = /\$([0-9]+):\ *([a-zA-Z_\$]+)/mg,
        matches,
        params = [];
    while (matches = paramsRegex.exec(text)) {
        params[parseInt(matches[1], 10) - 1] = matches[2];
    }
    return params;
}
exports.parseNamedParams = parseNamedParams;

exports.envToConfig = function(prefix) {
    var pgconf = {};
    _.each(['host', 'user', 'port', 'password', 'database'], function(key) {
        var envKey = prefix + key.toUpperCase();
        if (!process.env[envKey]) return;
        pgconf[key] = process.env[envKey];
    });
    return pgconf;
};

exports.encodeArray = function(arr, conformer) {
    return '{' + _.map(arr, conformer).join(',') + '}';
};

exports.camelizeColumnNames = function(row) {
    var r = {};
    _.each(row, function(v, k) {
        var end = k.slice(-3);
        if (end === "_id")
            r[_str.camelize(k.slice(0,-3))] = {id: v};
        else
            r[_str.camelize(k)] = v;
    });
    return r;
};

exports.connect = function(options) {
    var logger = options.logger || {
        debug: function(){},
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
        : function(rows) { return rows; };

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
                    if (values && !_.isArray(values[0]) && _.isObject(values[0])) {
                        //then assume we have named params
                        if (!prepared.namedParams) {
                            throw error("prepared statement does not have named params defined", {name: key});
                        }
                        var namedValues = values[0];
                        values = _.map(prepared.namedParams, function(paramName) {
                            return namedValues[paramName];
                        });
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
                        console.log(err, {key: key, values: values});
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

    db.prepareTemplate = function(key, templateFile, context, types) {
        var reading = loadQuery(db.__loadpath, templateFile);

        var templating = reading.then(function(templateBody) {
            var template = handlebars.compile(templateBody);
            var text = template(context);

            return text;
        });

        var stashing = templating.then(function(text) {
            db.__prepared[key] = {
                name: key,
                text: text,
                types: types
            };

            return key;
        });

        return stashing;
    };

    function prepare(key, types, text, path, fname) {
        var stash = function(text, namedParams) {
            db.__prepared[key] = {
                name: key,
                text: text,
                types: types,
                namedParams: namedParams
            };
        };
        if (text) {
            stash(text);
            return when(key);
        }
        var reading = loadQuery(path, fname)
        .then(function(text) {
            stash(text, parseNamedParams(text));
        })
        .then(function() {
            return key;
        }, function(err) {
            throw error("Failed to prepare", {key: key}, err);
        });
        return reading;
    }

    db.prepare = function() {
        var key, text, types, path, fname;
        assert(arguments.length <= 3);
        key = arguments[0];
        if (_.isString(arguments[1])) {
            text = arguments[1];
            types = arguments[2];
        } else if (arguments.length === 2) {
            types = arguments[1];
        }
        if (!text) {
            path = db.__loadpath;
            fname = key + '.sql';
        }
        return prepare(key, types, text, path, fname);
    };

    db.prepareDir = function(path) {
        return nodefn.call(fs.readdir, path)
        .then(function(files) {
            var sqlFiles = _.filter(files, function(file) {
                return file.slice(-4) === '.sql';
            });
            return when.all(
                _.map(sqlFiles, function(file) {
                    var key = file.slice(0, -4);
                    return prepare(key, null, null, path, file);
                }));
        });
    };

    db.prepareAll = function(/* statements */) {
        console.log(arguments);
        return when.join(
            _.map(arguments, function(arg) {
                if (_.isString(arg)) {
                    return db.prepare(arg);
                }
                //arrays are used to pass through statement name
                //as well as argument types
                return db.prepare.apply(null, arg);
            })
        ).then( function(results) {
            _.each(results, function(r) {
                logger.debug("prepared statement loaded:", r);
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

    db.cleansedConfig = function() {
        return _.omit(options, "password", "logger", "egress");
    };

    return db;
};

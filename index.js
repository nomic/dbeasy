"use strict";

var pg = require('pg').native
, assert = require('assert')
, _ = require('lodash')
, _str = require('underscore.string')
, Promise = require('bluebird')
, fs = Promise.promisifyAll(require('fs'))
, handlebars = require('handlebars')
, path = require('path')
, crypto = require('crypto')
, SYS_COL_PREFIX = '__'
, BAG_COL = SYS_COL_PREFIX + 'bag';



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

function parseNamedParams(text) {
    var paramsRegex = /\$([0-9]+):\ *([a-zA-Z_\.\$]+)/mg,
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



exports.jsifyColumns = jsifyColumns;
function jsifyColumns(row) {
    return _.transform(row, function(result, val, key) {
        if (val === null) return;
        if (key.slice(-3) === '_id')
            result[_str.camelize(key.slice(0,-3))] = {id: val};
        else
            result[_str.camelize(key)] = val;
        return result;
    }, {});
}

function columnifyJsName(name) {
    if (name.slice(-3) === '.id')
        return _str.underscore(name.slice(0,-3)) + '_id';
    else
        return _str.underscore(name);
}

exports.removeSysColumns = removeSysColumns;
function removeSysColumns(row) {
    return _.transform(row, function(result, val, key) {
        if (key === '__bag') {
            _.extend(result, val);
        }
        if (key.slice(0, 2) !== SYS_COL_PREFIX) {
            result[key] = val;
        }
        return result;
    }, {});
}

exports.simpleStore = function(options) {
    options = _.defaults(options, {
        egress: _.compose(
            jsifyColumns,
            removeSysColumns)
    });
    var ss = exports.connect(options);
    ss.prepareDir(path.join(__dirname, 'sql'));
    var onTableInfo = Promise.resolve({});
    var onNamespaces = Promise.resolve({});

    var onSavedSpecs = (function getSavedSpecs() {
        return ss.query([
            'SELECT * FROM information_schema.tables',
            ' WHERE table_name=\'spec\'',
            '   AND table_schema=\'simple_store\';'].join('\n'))
        .then(function(rows) {
            return (rows.length
                ? ss.query('SELECT * FROM simple_store.spec;')
                : ss.exec('__create_spec_table')
                    .then(function() {
                        return [];
                    })
            );
        });
    })();

    function fieldsToDDL(fields, isRefs) {
        return _.transform(fields, function(ddl, spec, name) {
            name = _str.underscored(name);
            if (isRefs) {
                name += '_id';
            }
            ddl.push([name, spec].join(' '));
            return ddl;
        }, []);
    }

    function requireNamespace(namespace) {
        return onNamespaces.then(function(namespaces) {
            if (!namespaces[namespace]) {
                throw new Error('Namespace not found: ' + namespace);
            }
        });
    }

    function tableInfo(specName) {
        return onTableInfo
        .then(function(tableInfo) {
            if (!tableInfo[specName]) throw new Error('Unkown spec: ' + specName);
            return tableInfo[specName];
        });
    }

    function getSavedSpec(fullName) {
        return onSavedSpecs
        .then(function(specs) {
            return _.where(specs, {name: fullName}) || null;
        });
    }

    function saveSpec(fullName, spec) {
        return ss.exec('__save_spec', {name: fullName, spec: spec});
    }

    function getInsertContext(specName, data) {
        return tableInfo(specName)
        .then(function(atableInfo) {
            var cols = atableInfo.columns;
            console.log("COLNAMES", colNames);
            var inputData = {};
            var fieldNames = [];
            _.each(_.pluck(cols, 'columnName'), function(colName) {
                var fieldName = _str.camelize(colName),
                    accessor = fieldName;
                if (fieldName.slice(-2) === 'Id') {
                    accessor = fieldName.slice(0, -2);
                    fieldName = accessor + '.id';
                }
                if (data[accessor]) {
                    inputData[accessor] = data[accessor];
                    fieldNames.push(fieldName);
                }
            });
            console.log("inputdata1", inputData);
            var colNames = [];
            var colVals = [];
            inputData[BAG_COL] = _.omit(data, _.keys(inputData));
            console.log("inputdata2", inputData);
            var valNum = 1;
            _.each(cols, function(col) {
                var colName = col.columnName;
                if (colName === BAG_COL) {
                    colNames.push(colName);
                    colVals.push('$' + valNum);
                    valNum++;
                    return;
                }
                if (_str.startsWith(colName, SYS_COL_PREFIX)
                    && colName !== BAG_COL) {
                    return;
                }
                colNames.push(colName);
                var fieldName = _str.camelize(colName);
                if (fieldName.slice(-2) === 'Id') {
                    fieldName = fieldName.slice(0, -2);
                }
                var val = inputData[fieldName];
                if (val) {
                    colVals.push('$' + valNum);
                    valNum++;
                } else {
                    colVals.push(col.columnDefault ? 'DEFAULT' : 'NULL');
                }
            });

            var bindVars = _.transform(
                fieldNames.concat([BAG_COL]),
                function(result, val, idx) {
                    result[idx+1] = val;
                    return result;
                }, {});

            return {
                inputData: inputData,
                templateVars: {
                    tableName: _str.underscored(specName),
                    bindVars: bindVars,
                    colNamesStr: colNames.join(', '),
                    colValsStr: colVals.join(', ')
                }
            };

        });
    }

    ss.addNamespace = function(namespace) {
        var schema = _str.underscored(namespace);
        onNamespaces = onNamespaces.then(function(namespaces) {
            return ss.query('SELECT * FROM information_schema.schemata WHERE schema_name=$1;', schema)
            .then(function(results) {
                return results.length
                    ? Promise.resolve()
                    : ss.query('CREATE SCHEMA ' + schema);
            })
            .then(function() {
                namespaces[namespace] = namespace;
                return namespaces;
            });
        });
    };

    ss.addSpec = function(fullName, spec) {
        var nameParts = fullName.split('.');
        var name = (nameParts.length > 1) ? nameParts[1] : nameParts[0],
            namespace = (nameParts.length > 1) ? nameParts[0] : null,
            schema = _str.underscored(namespace),
            table = _str.underscored(name);

        spec = _.defaults(spec || {}, {
            fields: {},
            refs: {}
        });
        return Promise.all([
            getSavedSpec(fullName),
            requireNamespace(namespace)
        ])
        .spread(function(savedSpec) {
            if (! _.isEqual(spec, savedSpec)) {
                return ss.execTemplate('__create_entity', {
                    tableName: schema + '.' + table,
                    columnDefinitions: fieldsToDDL(spec.fields),
                    refDefinitions: fieldsToDDL(spec.refs, true)
                })
                .then(function() {
                    return saveSpec(fullName, spec);
                });
            }
        })
        .then(function() {
            return ss.exec('__get_table_info', {
                schemaName: schema,
                tableName: table
            });
        })
        .then(function(info) {
            onTableInfo = onTableInfo.then(function(tableInfo) {
                tableInfo[fullName] = {columns: info};
                return tableInfo;
            });
        });
    };

    ss.upsert = function(name, data) {
        return (
            data.id
                ? getInsertContext(name, data)
                    .then(function(ctx) {
                        console.log("CTX", ctx);
                        return ss.execTemplate(
                            '__update',
                            ctx.templateVars,
                            ctx.inputData
                        );
                    })
                : getInsertContext(name, data)
                    .then(function(ctx) {
                        console.log("CTX", ctx);
                        return ss.execTemplate(
                            '__insert',
                            ctx.templateVars,
                            ctx.inputData
                        );
                    })
        ).then(_.first);
    };

    ss.getById = function(name, id) {
        return ss.getByIds(name, [id])
        .then(function(results) {
            return results[0] || null;
        });
    };

    ss.deleteById = function(name, id) {
        return ss.deleteByIds(name, [id]);
    };

    ss.deleteByIds = function(name, ids) {
        return ss.execTemplate('__delete_by_ids', {
            tableName: _str.underscored(name)
        }, ids);
    };

    ss.getByIds = function(name, ids) {
        return ss.execTemplate('__get_by_ids', {
            tableName: _str.underscored(name)
        }, ids);
    };

    return ss;
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
    db.__templates = {};
    db.__loadpath = options.loadpath || "";
    var requestedPoolSize = options.poolSize || pg.defaults.poolSize;
    pg.defaults.poolSize = requestedPoolSize;
    var egress = options.egress
        ? function(rows) {
            return _.map(rows, options.egress);
        }
        : function(rows) { return rows; };

    pg = Promise.promisifyAll(pg);
    // connect to the postgres db defined in conf
    var onConnection = function(fn) {
        return pg.connectAsync(conString(options))
            .then(function(args) {
                var ctx = {};
                var conn = args[0];
                var connQuery = Promise.promisify(conn.query, conn);
                var done = args[1];

                ctx.__conn = conn;
                ctx.begin = function() {
                    conn.query('BEGIN');
                };
                ctx.end = function() {
                    return connQuery('END');
                };
                ctx.query = function(text, vals) {
                    if (vals !== undefined) { vals = _.rest(arguments); }
                    return ctx.queryRaw(text, vals)
                    .then(function(results) {
                        return results ? egress(results) : null;
                    });
                };
                ctx.queryRaw = function(text, vals) {
                    if (vals !== undefined) { vals = _.rest(arguments); }
                    var statements = _.isArray(text) ? text : [text];
                    return (function next(results) {
                        var statement = statements.shift();
                        if (!statement) return results.pop();

                        return connQuery(text, vals)
                            .then(function(result) {
                                results.push(result.rows ? result.rows : null);
                                return next(results);
                            })
                            .catch(function(err) {
                                throw error('queryRaw failed', {text: text, vals: vals}, err);
                            });
                    })([]);
                };
                ctx.execTemplate = function(templateKey, templateParams, values) {
                    var key = templateKey + JSON.stringify(templateParams);

                    // Max length of prepared statement is NAMEDATALEN (64)
                    var md5sum = crypto.createHash('md5');
                    md5sum.update(key);
                    key = md5sum.digest('hex');

                    if (db.__prepared[key]) {
                        return ctx.exec(key, values);
                    }

                    var template = db.__templates[templateKey];
                    if (! template) {
                        throw error("template not found: " + templateKey);
                    }
                    var text = template(templateParams);
                    console.log("TEMPLATE: ", templateParams, text);
                    prepare(key, null, text);
                    return ctx.exec(key, values);
                };
                ctx.exec = function(key, values) {
                    if (values !== undefined) { values = _.rest(arguments); }
                    var prepared = db.__prepared[key];
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
                            return getKeyPath(namedValues, paramName);
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
                        return connQuery(opts).then(function(result) {
                            results.push(result.rows ? egress(result.rows) : null);
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
                            console.log(err, {key: key, values: values});
                            throw error('exec failed', {key: key, values: values}, err);
                        });
                    })([]);
                };

                var rollback = function(err) {
                    return connQuery("ROLLBACK").then(function() {
                        throw err;
                    });
                };

                return Promise.try(function() { return fn(ctx); })
                .catch(rollback)
                .finally(done);

            }).catch(function(err) {
                throw err;
            });
    };

    // set the load path
    db.loadpath = function(path) {
        db.__loadpath = path;
    };


    // Execute a previously prepared statement
    db.exec = function(/*statement, params*/) {
        var args = _.toArray(arguments);
        return db.onConnection(function(conn) {
            return conn.exec.apply(conn, args);
        });
    };

    db.execTemplate = function(/*statement, templateParams, params*/) {
        var args = _.toArray(arguments);
        return db.onConnection(function(conn) {
            return conn.execTemplate.apply(conn, args);
        });
    };

    // Execute a sql query string
    db.query = function(sql /*, params*/) {
        var args = _.toArray(arguments);
        return db.onConnection( function(conn) {
            return conn.query.apply(sql, args);
        });
    };

    // Execute a sql query string and don't run
    // the results through egress
    db.queryRaw = function(sql /*, params*/) {
        var args = _.toArray(arguments);
        return db.onConnection( function(conn) {
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
    db.transaction = function(workFn) {
        return onConnection( function(conn) {
            conn.begin();
            var working = workFn(conn);
            return working.then(function() {
                return conn.end().then(function() {
                    return working;
                });
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

    function compileTemplate(key, path, fname) {
        return loadQuery(path, fname)
        .then(function(text) {
            db.__templates[key] = handlebars.compile(text);
        });
    }

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
            stash(text, parseNamedParams(text));
            return Promise.resolve(key);
        }
        var reading = loadQuery(path, fname)
        .then(function(text) {
            stash(text, parseNamedParams(text));
        })
        .then(function() {
            return key;
        }, function(err) {
            console.log(err);
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
        if (text) return prepare(key, types, text);

        path = db.__loadpath;
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

    db.prepareDir = function(path) {
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

    db.prepareAll = function(/* statements */) {
        return Promise.all(
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

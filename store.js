'use strict';
var path = require('path'),
    _ = require('lodash'),
    _str = require('underscore.string'),
    Promise = require('bluebird'),
    client = require('./client'),
    SYS_COL_PREFIX = '__',
    BAG_COL = SYS_COL_PREFIX + 'bag';

exports.BAG_COL = BAG_COL;

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

exports.factory = factory;
function factory(options) {

    return function() {
        var singleSchema = false;
        var specs = {};
        if (_.isString(arguments[0])) {
            singleSchema = true;
            specs[arguments[0]] = arguments[1];
        } else {
            specs = arguments[0];
        }
        var store = connect(options);
        _.each(specs, function(spec, name) {
            store.addSpec(name, spec);
        });
        return singleSchema
            ? store(arguments[0])
            : store;
    };

}

exports.connect = connect;
function connect(options) {
    options = _.defaults(options, {
        egress: _.compose(
            client.jsifyColumns,
            removeSysColumns)
    });

    var defaultFields = {
        id: 'bigint NOT NULL',
        created: 'timestamp without time zone DEFAULT now() NOT NULL',
        updated: 'timestamp without time zone DEFAULT now() NOT NULL',
    };
    var neverUpdated = ['id', 'created'];

    var db = client.connect(options);
    db.prepareDir(path.join(__dirname, 'sql'));

    var onSpecs = ensureSpecTableExists();
    var derivations = {};

    // Expose db client functions, but only run them after
    // specs have been processed
    function addDbFns(store) {
    _.extend(
        store,
        _.mapValues(
            db,
            function(val) {
                if (!_.isFunction(val)) return val;
                return function() {
                    var args = arguments;
                    return onSpecs.then(function() {
                        return val.apply(db, args);
                    });
                };
            }));
    }

    function specTableExists() {
        return db.query([
            'SELECT 1 FROM information_schema.tables',
            ' WHERE table_name=\'spec\'',
            '   AND table_schema=\'dbeasy_store\';'].join('\n'));
    }

    function ensureSpecTableExists() {
        return specTableExists()
        .then(function(results) {
            if (results.length) return;
            return db.synchronize('specTable', function() {
                return specTableExists()
                .then(function(results) {
                    if (results.length) return;
                    return db.exec('__create_spec_table');
                });
            });
        });
    }

   function getSpec(specName) {
        return db.query(
            'SELECT spec FROM dbeasy_store.spec WHERE name = $1',
            specName)
        .then(function(specRows) {
            return specRows[0] ? specRows[0].spec : null;
        });
    }

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

    function ensureNamespace(namespace) {
        return addNamespace(namespace);
    }

    function getColumnInfo(tableName) {
        var parts = tableName.split('.');
        return db.exec('__get_table_info', {
            schemaName: parts[0],
            tableName: parts[1]
        })
        .then(function(cols) {
            if (!cols[0]) throw new Error('Unkown store table: ' + tableName);
            return cols;
        });
    }

    function saveSpec(specName, spec) {
        return db.exec('__save_spec', {name: specName, spec: spec});
    }

    function getInsertContext(specName, data) {
        var tableName = _str.underscored(specName);
        // Do not save derived values.
        data = _.omit(data, _.keys(derivations[specName]));

        return getColumnInfo(tableName)
        .then(function(cols) {
            cols = _.reject(cols, function(col) {
                return _.contains(neverUpdated, col.columnName);
            });
            var inputData = {};
            inputData[BAG_COL] = {};


            var fieldNames = [];
            // Fields that have a matching column
            var placed = [];
            _.each(_.pluck(cols, 'columnName'), function(colName) {
                var fieldName = _str.camelize(colName),
                    val = data[fieldName];
                placed.push(fieldName);
                if (!val && fieldName.slice(-2) === 'Id') {
                    //check for nested id
                    var objField = fieldName.slice(0,-2);
                    placed.push(objField);
                    val = data[objField];
                    val = val && val.id;
                }
                if (val) {
                    inputData[fieldName] = val;
                    fieldNames.push(fieldName);
                }
            });
            var colNames = [];
            var colVals = [];
            inputData[BAG_COL] = _.omit(data, placed);
            var valNum = 1;
            _.each(cols, function(col) {
                var colName = col.columnName;
                if (colName === BAG_COL) {
                    colNames.push(colName);
                    colVals.push('$' + valNum);
                    valNum++;
                    return;
                }
                if (_str.startsWith(colName, SYS_COL_PREFIX)) {
                    return;
                }
                var fieldName = _str.camelize(colName);
                if (inputData[fieldName] === undefined) {
                    return;
                }
                colNames.push(colName);

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
                    tableName: _str.underscored(tableName),
                    bindVars: bindVars,
                    colNamesStr: colNames.join(', '),
                    colValsStr: colVals.join(', ')
                }
            };

        });
    }

    function addUpdateContext(onInsertContext, whereProps) {
        return onInsertContext.then(function(ctx) {
            ctx.templateVars.whereColBinds = {};
            var nextBindVar = _.keys(ctx.templateVars.bindVars).length + 1;
            _.each(whereProps, function(val, name) {
                ctx.inputData[name] = val;
                ctx.templateVars.bindVars[nextBindVar] = name;
                ctx.templateVars.whereColBinds[nextBindVar] = name;
                nextBindVar++;
            });
            return ctx;
        });
    }

    function addNamespace(namespace) {
        var schema = _str.underscored(namespace);
        return db.query('SELECT * FROM information_schema.schemata WHERE schema_name = $1;', schema)
        .then(function(results) {
            return results.length
                ? Promise.resolve()
                : db.query('CREATE SCHEMA ' + schema);
        });
    }

    // A falsey can be passed in for a default field
    // so that it does not get added, or an alternate
    // definition will override it.
    function addDefaultFields(fields, defaults) {
        var result = _.extend({}, defaults, fields);

        var omit = [];
        _.each(result, function(def, name) {
            if (! result[name]) {
                omit.push(name);
            }
        });

        return _.omit(result, omit);
    }

    function first(rows) {
        return _.first(rows) || null;
    }

    function derive(specName) {
        var derivers = derivations[specName];
        if (_.isEmpty(derivers)) return _.identity;
        return function(rows) {
            if (! rows) return;
            rows = _.isArray(rows) ? rows : [rows];
            return _.map(rows, function(row) {
                row = _.clone(row);
                _.each(derivers, function(deriverFn, derivedName) {
                    row[derivedName] = deriverFn(row);
                });
                return row;
            });
        };
    }

    function doSpecUpdate(specName, spec) {
        var nameParts = specName.split('.');
        var namespace = (nameParts.length > 1) ? nameParts[0] : null,
            table = _str.underscored(specName);
        return db.synchronize(specName, function() {
            return getSpec(specName)
            .then(function(savedSpec) {
                // Even if we checked this before entering this
                // critical section, do it again to avoid wasted work
                // when several processes are coming up at the same time.
                if (_.isEqual(spec, savedSpec)) return;

                return ensureNamespace(namespace)
                .then(function() {
                    return db.transaction(function(conn) {
                        return Promise.all([
                            conn.execTemplate('__create_entity', {
                                tableName: table,
                                columnDefinitions: fieldsToDDL(spec.fields),
                                refDefinitions: fieldsToDDL(spec.refs, true),
                                hasId: !!spec.fields.id
                            }),
                            saveSpec(specName, spec)
                        ]);
                    });
                });
            });
        });
    }

    var ss = function(specName) {
        var store = _.mapValues(
            _.pick(
                ss,
                'getByIds', 'getById', 'deleteById', 'deleteByIds',
                'insert', 'update', 'upsert', 'find', 'findOne'),
            function(fn) { return _.partial(fn, specName); });
        addDbFns(store);
        return store;
    };

    addDbFns(ss, db);

    ss.dropNamespace = function(namespace) {
        return onSpecs
        .then(function() {
            return Promise.all([
                // Drop the remembered specs
                ss.query([
                    'DELETE FROM dbeasy_store.spec',
                    'WHERE name LIKE \'' + namespace + '.%\';'
                ].join('\n')),

                // Erase the data
                ensureNamespace(namespace)
                .then(function() {
                    return ss.query([
                        'DROP SCHEMA ',
                        _str.underscored(namespace) + ' CASCADE;'
                    ].join('\n'));
                })
            ]);
        });
    };

    ss.addSpec = function(specName, spec) {
        spec = _.defaults(spec || {}, {
            fields: {},
            refs: {},
            derived: {}
        });

        var unknownKeys = _.keys(_.omit(spec, 'fields', 'refs', 'derived'));
        if (unknownKeys.length !== 0) {
            throw new Error('Malformed spec; unknown keys: ' + unknownKeys);
        }
        derivations[specName] = spec.derived;
        delete spec.derived;

        spec.fields = addDefaultFields(spec.fields, defaultFields);

        onSpecs = onSpecs
        .then(function() {
            return getSpec(specName);
        })
        .then(function(savedSpec) {
            if (! _.isEqual(spec, savedSpec)) {
                return doSpecUpdate(specName, spec);
            }
        });
    };

    ss.upsert = function(name, data) {
        return (data.id
            ? ss.update(name, _.pick(data, 'id'), data)
            : ss.insert(name, data)
        );
    };

    ss.insert = function(name, data) {
        return onSpecs
        .then(function() {
            return getInsertContext(name, data);
        })
        .then(function(ctx) {
            return ss.execTemplate(
                '__insert',
                ctx.templateVars,
                ctx.inputData
            );
        })
        .then(_.compose(first, derive(name)));
    };

    ss.update = function(name, whereProps, data) {
        data = _.omit(data, _.keys(defaultFields));
        return onSpecs
        .then(function() {
            return addUpdateContext(getInsertContext(name, data), whereProps);
        })
        .then(function(ctx) {
            return ss.execTemplate(
                '__update',
                ctx.templateVars,
                ctx.inputData
            );
        })
        .then(_.compose(first, derive(name)));
    };

    ss.getById = function(name, id) {
        return onSpecs
        .then(function() {
            return ss.getByIds(name, [id])
            .then(first);
        });
    };

    ss.deleteById = function(name, id) {
        return onSpecs
        .then(function() {
            return ss.deleteByIds(name, [id]);
        });
    };

    ss.getByIds = function(name, ids) {
        return onSpecs
        .then(function() {
            return ss.execTemplate('__get_by_ids', {
                tableName: _str.underscored(name)
            }, ids)
            .then(derive(name));
        });
    };

    ss.deleteByIds = function(name, ids) {
        return onSpecs
        .then(function() {
            return ss.execTemplate('__delete_by_ids', {
                tableName: _str.underscored(name)
            }, ids);
        });
    };

    ss.find = function(name, opts) {
        var idx = 1;
        var templateVars = {
            bindVars: {},
            columns: {},
            tableName: _str.underscored(name)
        };
        templateVars = _.transform(opts, function(result, val, key) {
            result.bindVars[idx] = key;
            result.columns[idx] = _str.underscored(key);
            return result;
        }, templateVars);
        return onSpecs
        .then(function() {
            return ss.execTemplate('__find', templateVars, opts)
            .then(derive(name));
        });
    };

    ss.findOne = function(name, opts) {
        return ss.find(name, opts)
        .then(function(rows) {
            return rows[0] || null;
        });
    };

    return ss;
}


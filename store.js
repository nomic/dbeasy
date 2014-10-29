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
        var schemas = {};
        if (_.isString(arguments[0])) {
            singleSchema = true;
            schemas[arguments[0]] = arguments[1];
        } else {
            schemas = arguments[0];
        }
        var store = connect(options);
        _.each(schemas, function(schema, name) {
            store.addSpec(name, schema);
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

    var db = client.connect(options);
    db.prepareDir(path.join(__dirname, 'sql'));

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

    function ensureSpecTableExists() {
        return db.query([
            'SELECT 1 FROM information_schema.tables',
            ' WHERE table_name=\'spec\'',
            '   AND table_schema=\'dbeasy_store\';'].join('\n'))
        .then(function(results) {
            if (! results.length) return db.exec('__create_spec_table');
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

    function saveSpec(fullName, spec) {
        return db.exec('__save_spec', {name: fullName, spec: spec});
    }

    function getInsertContext(specName, data) {
        var tableName = _str.underscored(specName);
        return getColumnInfo(tableName)
        .then(function(cols) {
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
            var colNames = [];
            var colVals = [];
            inputData[BAG_COL] = _.omit(data, _.keys(inputData));
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
                    tableName: _str.underscored(tableName),
                    bindVars: bindVars,
                    colNamesStr: colNames.join(', '),
                    colValsStr: colVals.join(', ')
                }
            };

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
    var ss = function(schema) {
        var store = _.mapValues(
            _.pick(
                ss,
                'getByIds', 'getById', 'deleteById', 'deleteByIds',
                'insert', 'update', 'upsert'),
            function(fn) { return _.partial(fn, schema); });
        addDbFns(store);
        return store;
    };

    var onSpecs = ensureSpecTableExists(db);

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

    ss.addSpec = function(fullName, spec) {
        spec = _.defaults(spec || {}, {
            fields: {},
            refs: {}
        });

        var unknownKeys = _.keys(_.omit(spec, 'fields', 'refs'));
        if (unknownKeys.length !== 0) {
            throw new Error('Malformed spec; unknown keys: ' + unknownKeys);
        }

        var nameParts = fullName.split('.');
        var name = (nameParts.length > 1) ? nameParts[1] : nameParts[0],
            namespace = (nameParts.length > 1) ? nameParts[0] : null,
            schema = _str.underscored(namespace),
            table = _str.underscored(name);

        onSpecs = onSpecs
        .then(function() {
            return Promise.all([
                getSpec(fullName),
                ensureNamespace(namespace)
            ]);
        })
        .spread(function(savedSpec) {
            if (! _.isEqual(spec, savedSpec)) {
                return db.execTemplate('__create_entity', {
                    tableName: schema + '.' + table,
                    columnDefinitions: fieldsToDDL(spec.fields),
                    refDefinitions: fieldsToDDL(spec.refs, true)
                })
                .then(function() {
                    return saveSpec(fullName, spec);
                });
            }
        });
    };

    ss.upsert = function(name, data) {
        return (
            data.id
                ? ss.update(name, data)
                : ss.insert(name, data)
        ).then(_.first);
    };

    ss.insert = function(name, data) {
        return onSpecs
        .then(function() {
            return getInsertContext(name, data)
            .then(function(ctx) {
                return ss.execTemplate(
                    '__insert',
                    ctx.templateVars,
                    ctx.inputData
                );
            });
        });
    };

    ss.update = function(name, data) {
        return onSpecs
        .then(function() {
            return getInsertContext(name, data)
            .then(function(ctx) {
                return ss.execTemplate(
                    '__update',
                    ctx.templateVars,
                    ctx.inputData
                );
            });
        });
    };

    ss.getById = function(name, id) {
        return onSpecs
        .then(function() {
            return ss.getByIds(name, [id])
            .then(function(results) {
                return results[0] || null;
            });
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
            }, ids);
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

    return ss;
}


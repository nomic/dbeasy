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

exports.connect = connect;
function connect(options) {
    options = _.defaults(options, {
        egress: _.compose(
            client.jsifyColumns,
            removeSysColumns)
    });

    var ss = client.connect(options);
    ss.prepareDir(path.join(__dirname, 'sql'));
    var onTableInfo = Promise.resolve({});
    var onNamespaces = Promise.resolve({});

   function loadSpecs() {
        return ss.query([
            'SELECT 1 FROM information_schema.tables',
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
        return onNamespaces.then(function(namespaces) {
            if (!namespaces[namespace]) {
                return ss.addNamespace(namespace);
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
        return loadSpecs()
        .then(function(specRows) {
            var specRow = _.find(specRows, {name: fullName});
            return specRow ? specRow.spec : null;
        });
    }

    function saveSpec(fullName, spec) {
        console.log("SAVING", fullName, spec);
        return ss.exec('__save_spec', {name: fullName, spec: spec});
    }

    function clearTableInfo(specName) {
        var tableName = _str.Camelize(specName);
        return onTableInfo.then(function(tableInfo) {
            delete tableInfo[tableName];
        });
    }

    function deleteSpec(specName) {
        return ss.query(
            'DELETE FROM simple_store.spec WHERE name = ' + specName);
    }

    function getInsertContext(specName, data) {
        return tableInfo(specName)
        .then(function(atableInfo) {
            var cols = atableInfo.columns;
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
        console.log("ADD NAMESPACE", namespace);
        var schema = _str.underscored(namespace);
        onNamespaces = onNamespaces.then(function(namespaces) {
            return ss.query('SELECT * FROM information_schema.schemata WHERE schema_name=$1;', schema)
            .then(function(results) {
                console.log("CREATING SCHEMA", schema);
                return results.length
                    ? Promise.resolve()
                    : ss.query('CREATE SCHEMA ' + schema);
            })
            .then(function() {
                namespaces[namespace] = namespace;
                return namespaces;
            });
        });
        return onNamespaces;
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

        return Promise.all([
            getSavedSpec(fullName),
            ensureNamespace(namespace)
        ])
        .spread(function(savedSpec) {
            if (! _.isEqual(spec, savedSpec)) {
                console.log("NOT EQUAL", spec, savedSpec);
                return ss.execTemplate('__create_entity', {
                    tableName: schema + '.' + table,
                    columnDefinitions: fieldsToDDL(spec.fields),
                    refDefinitions: fieldsToDDL(spec.refs, true)
                })
                .then(function() {
                    return saveSpec(fullName, spec);
                });
            }
            console.log("EQUAL", spec, savedSpec);
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
            return onTableInfo;
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

    ss.dropNamespace = function(namespace) {
        return onNamespaces.then(function(namespaces) {
            delete namespaces[namespace];
        })
        .then(function() {
            return onTableInfo.then(function(tableInfo) {
                return Promise.map(
                    _.filter(
                        _.map(_.keys(tableInfo), _str.camelize),
                        function(specName) {
                            _str.startsWith(specName, namespace);
                        }),
                    function(specName) {
                        return _.map(specName, clearTableInfo)
                            .concat(_.map(specName, deleteSpec));
                    });
            });
        })
        .then(function() {
            ss.query(
                'DROP SCHEMA '
                + _str.underscored(namespace) + ' CASCADE;')
            .catch(_.noop);
        });
    };

    ss.dropNamespace = function(namespace) {
        return onNamespaces.then(function(namespaces) {
            delete namespaces[namespace];
        })
        .then(function() {
            // Clear cached table info
            onTableInfo = onTableInfo
            .then(function(tableInfo) {
                return _.reject(
                    _.map(_.keys(tableInfo), _str.camelize),
                    function(specName) {
                        _str.startsWith(specName, namespace);
                    });
            });

            // Drop the remembered specs
            return ss.query([
                'DELETE FROM simple_store.spec',
                'WHERE name LIKE \'' + namespace + '.%\';'
            ].join('\n'));
        })
        .then(function() {
            // Erase the data
            ss.query(
                'DROP SCHEMA '
                + _str.underscored(namespace) + ' CASCADE;')
            .catch(_.noop);
        });
    };

    return ss;
}


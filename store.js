'use strict';
var _ = require('lodash'),
    _str = require('underscore.string'),
    assert = require('assert'),
    util = require('./util'),
    SYS_COL_PREFIX = util.SYS_COL_PREFIX,
    BAG_COL = util.BAG_COL;

var defaultFields = {
    id: 'bigint NOT NULL',
    created: 'timestamp without time zone DEFAULT now() NOT NULL',
    updated: 'timestamp without time zone DEFAULT now() NOT NULL',
};

var neverUpdated = _.keys(defaultFields);
exports.defaultFields = defaultFields;


exports.store = function(client, storeName, options) {
    options = _.defaults(options || {}, {
        derived: {}
    });

    var onReady = client.__prepareSql();
    var store = {};

    function getColumnInfo(tableName) {
        var parts = tableName.split('.');
        return client.exec('__get_table_info', {
            schemaName: parts[0],
            tableName: parts[1]
        })
        .then(function(cols) {
            if (!cols[0]) throw new Error('Unkown store table: ' + tableName);
            return cols;
        });
    }

    function getWriteContext(data, opts) {
        var isPartialUpdate = opts.partial || false;

        assert(opts.partial !== undefined);
        var tableName = _str.underscored(storeName);
        // Do not save derived values.
        data = _.omit(data, _.keys(options.derived));

        return getColumnInfo(tableName)
        .then(function(cols) {
            cols = _.reject(cols, function(col) {
                return (
                    _.contains(neverUpdated, col.columnName)
                    || (isPartialUpdate && (col.columnName === BAG_COL)));
            });
            var inputData = {};
            var colNames = [];
            var colVals = [];
            var bindNum = 1;
            var bindVars = {};

            // Fields that have a matching column
            var placed = [];
            _.each(cols, function(col) {
                var colName = col.columnName;
                if (_str.startsWith(colName, SYS_COL_PREFIX)) {
                    return;
                }

                var fieldName = _str.camelize(colName),
                    val = data[fieldName];
                if (!val && fieldName.slice(-2) === 'Id') {
                    //check for nested id
                    var objField = fieldName.slice(0,-2);
                    placed.push(objField);
                    val = data[objField];
                    val = val && val.id;
                }

                if (!val && isPartialUpdate) {
                    return;
                }

                colNames.push(colName);

                if (!val) {
                    colVals.push(col.columnDefault ? 'DEFAULT' : 'NULL');
                    return;
                }

                inputData[fieldName] = val;
                colVals.push('$' + bindNum);
                bindVars[bindNum] = fieldName;
                bindNum++;
                placed.push(fieldName);

            });

            var bagData = _.omit(data, placed);

            if (!_.isEmpty(bagData)) {
                if (isPartialUpdate) {
                    client._logger.warn(
                        '__bag vals ignored; partial update on __bag unsupported', {
                        storeName: storeName,
                        values: bagData
                    });
                } else {
                    inputData[BAG_COL] = bagData;
                    bindVars[bindNum] = BAG_COL;
                    colNames.push(BAG_COL);
                    colVals.push('$' + bindNum);
                }
            }

            colNames.push('updated');
            colVals.push('DEFAULT');

            return {
                inputData: inputData,
                templateVars: {
                    tableName: tableName,
                    bindVars: bindVars,
                    colNamesStr: colNames.join(', '),
                    colValsStr: colVals.join(', ')
                }
            };

        });
    }

    function addWhereContext(onInsertContext, whereProps) {
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

    function first(rows) {
        return _.first(rows) || null;
    }

    function derive() {
        if (_.isEmpty(options.derived)) return _.identity;
        return function(rows) {
            if (! rows) return;
            rows = _.isArray(rows) ? rows : [rows];
            return _.map(rows, function(row) {
                row = _.clone(row);
                _.each(options.derived, function(deriverFn, derivedName) {
                    row[derivedName] = deriverFn(row);
                });
                return row;
            });
        };
    }

    store.insert = function(data) {
        return onReady
        .then(function() {
            return getWriteContext(data, {partial: false});
        })
        .then(function(ctx) {
            return client.execTemplate(
                '__insert',
                ctx.templateVars,
                ctx.inputData
            );
        })
        .then(_.compose(first, derive()));
    };

    store.replace = function(whereProps, data) {
        return store.update(whereProps, data, {partial: false});
    };

    store.update = function(whereProps, data, opts) {
        opts = _.defaults(opts || {}, {
            partial: true
        });
        data = _.omit(data, _.keys(defaultFields));
        return onReady
        .then(function() {
            return addWhereContext(
                getWriteContext(data, opts),
                whereProps);
        })
        .then(function(ctx) {
            return client.execTemplate(
                '__update',
                ctx.templateVars,
                ctx.inputData
            );
        })
        .then(_.compose(first, derive()));
    };

    store.getById = function(id) {
        return onReady
        .then(function() {
            return store.getByIds([id])
            .then(first);
        });
    };

    store.deleteById = function(id) {
        return onReady
        .then(function() {
            return store.deleteByIds([id]);
        });
    };

    store.getByIds = function(ids) {
        return onReady
        .then(function() {
            return client.execTemplate('__get_by_ids', {
                tableName: _str.underscored(storeName)
            }, ids)
            .then(derive());
        });
    };

    store.deleteByIds = function(ids) {
        return onReady
        .then(function() {
            return client.execTemplate('__delete_by_ids', {
                tableName: _str.underscored(storeName)
            }, ids);
        });
    };

    store.find = function(opts) {
        var idx = 1;
        var templateVars = {
            bindVars: {},
            columns: {},
            tableName: _str.underscored(storeName)
        };
        templateVars = _.transform(opts, function(result, val, key) {
            result.bindVars[idx] = key;
            result.columns[idx] = _str.underscored(key);
            return result;
        }, templateVars);
        return onReady
        .then(function() {
            return client.execTemplate('__find', templateVars, opts)
            .then(derive());
        });
    };

    store.findOne = function(opts) {
        return store.find(opts)
        .then(function(rows) {
            return rows[0] || null;
        });
    };

    return store;
};


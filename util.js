'use strict';

var _ = require('lodash'),
    _str = require('underscore.string'),
    SYS_COL_PREFIX = '__',
    BAG_COL = SYS_COL_PREFIX + 'bag';

exports.SYS_COL_PREFIX = SYS_COL_PREFIX;
exports.BAG_COL = BAG_COL;

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

exports.handleMetaColumns = handleMetaColumns;
function handleMetaColumns(row) {
    return _.transform(row, function(result, val, key) {
        if (key === BAG_COL) {
            _.extend(result, val);
        }
        if (key.slice(0, 2) !== SYS_COL_PREFIX) {
            result[key] = val;
        }
        return result;
    }, {});
}
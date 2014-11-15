'use strict';

var _ = require('lodash'),
    _str = require('underscore.string'),
    SYS_COL_PREFIX = '__',
    BAG_COL = SYS_COL_PREFIX + 'bag';

exports.SYS_COL_PREFIX = SYS_COL_PREFIX;
exports.BAG_COL = BAG_COL;


exports.removeNulls = removeNulls;
function removeNulls(row) {
  return _.reduce(row, function(result, val, key) {
    if (val !== null) result[key] = val;
    return result;
  }, {});
}

exports.camelizeColumns = camelizeColumns;
function camelizeColumns(row) {
  return _.reduce(row, function(result, val, key) {
    result[_str.camelize(key)] = val;
    return result;
  }, {});
}

exports.handleMetaColumns = handleMetaColumns;
function handleMetaColumns(row) {
  return _.reduce(row, function(result, val, key) {
    if (key === BAG_COL) {
      _.extend(result, val);
    }
    if (key.slice(0, 2) !== SYS_COL_PREFIX) {
      result[key] = val;
    }
    return result;
  }, {});
}
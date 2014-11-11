'use strict';

var _ = require('lodash'),
    _str = require('underscore.string');

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

exports.parseNamedParams = parseNamedParams;
function parseNamedParams(text) {
    var paramsRegex = /\$([0-9]+):\ *([a-zA-Z_\.\$]+)/mg,
        matches,
        params = [];
    while (matches = paramsRegex.exec(text)) {
        params[parseInt(matches[1], 10) - 1] = matches[2];
    }
    return params;
}

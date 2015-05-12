'use strict';
var _ = require('lodash'),
_str = require('underscore.string'),
assert = require('assert'),
util = require('../util'),
Promise = require('bluebird'),
SYS_COL_PREFIX = util.SYS_COL_PREFIX,
BAG_COL = util.BAG_COL;

var defaultFields = {
  id:      'bigserial PRIMARY KEY',
  created: 'timestamp with time zone DEFAULT now() NOT NULL',
  updated: 'timestamp with time zone DEFAULT now() NOT NULL',
};

var metaFields = {
  __bag:     'json NOT NULL DEFAULT \'{}\'',
  __deleted: 'timestamp with time zone'
};

var neverUpdated = _.keys(defaultFields);
exports.defaultFields = defaultFields;
exports.metaFields = metaFields;

exports.store = function(client, storeName, options) {
  options = _.defaults(options || {}, {
    derived: {}
  });
  var layout = require('../layout')(client);

  var statements;
  var onReady = client.loadStatements(__dirname + '/sql')
  .then(function(stmts) {
    statements = stmts;
  });


  var store = {};

  function initContext() {
    return {
      inputData: {},
      templateVars: {
        tableName: _str.underscored(storeName),
        bindVars: {},
        colNamesStr: null,
        colValsStr: null
      }
    };
  }

  function addWriteContext(onCtx, data, opts) {
    var isPartialUpdate = opts.partial || false;

    assert(opts.partial !== undefined);
    var tableName = _str.underscored(storeName);
    // Do not save derived values.
    data = _.omit(data, _.keys(options.derived));

    return Promise.all([
      layout.getColumnInfo(tableName),
      onCtx
    ])
    .spread(function(cols, ctx) {

      var hasUpdated = false;
      cols = _.reject(cols, function(col) {
        hasUpdated = hasUpdated || col.columnName === 'updated';
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
      var fieldNames = [];
      _.each(cols, function(col) {
        var colName = col.columnName;
        var fieldName = _str.camelize(colName);
        fieldNames.push(fieldName);

        if (_str.startsWith(colName, SYS_COL_PREFIX)) {
          return;
        }

        var val = data[fieldName];

        if (_.isUndefined(val) && isPartialUpdate) {
          return;
        }

        colNames.push(colName);

        if (_.isUndefined(val)) {
          colVals.push(col.columnDefault ? 'DEFAULT' : 'NULL');
          return;
        }

        inputData[fieldName] = val;
        colVals.push('$' + bindNum);
        bindVars[bindNum] = fieldName;
        bindNum++;

      });

      var bagData = _.omit(data, fieldNames);

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

      if (hasUpdated) {
        colNames.push('updated');
        colVals.push('DEFAULT');
      }

      _.extend(ctx.inputData, inputData);
      _.extend(ctx.templateVars, {
        bindVars: bindVars,
        colNamesStr: colNames.join(', '),
        colValsStr: colVals.join(', ')
      });
      return ctx;

    });
  }

  function addWhereContext(context, whereProps) {
    return Promise.resolve(context)
    .then(function(ctx) {
      ctx.templateVars.whereColBinds = {};
      ctx.templateVars.whereColAnyBinds = {};
      var nextBindVar = _.keys(ctx.templateVars.bindVars).length + 1;
      _.each(whereProps, function(val, name) {
        ctx.inputData[name] = val;
        ctx.templateVars.bindVars[nextBindVar] = name;
        if (_.isArray(val)) {
          ctx.templateVars.whereColAnyBinds[nextBindVar] =
            _str.underscored(name);
        } else {
          ctx.templateVars.whereColBinds[nextBindVar] =
            _str.underscored(name);
        }
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

  store.insert = function(values, conn) {
    var handler = conn || client;
    return onReady
    .then(function() {
      return addWriteContext(
        initContext(),
        values,
        {partial: false}
        );
    })
    .then(function(ctx) {
      return handler.execTemplate(
        statements.insert,
        ctx.templateVars,
        ctx.inputData
        );
    })
    .then(_.compose(first, derive()));
  };


  store.delsert = function(whereProps, values) {
    // The where properties will also be values of the resulting
    // row because a this operation replaces the entire row.
    values = _.extend({}, values, whereProps);
    return client.transaction(function(conn) {
      return store.delete(whereProps, conn)
      .then(function() {
        return store.insert(_.extend({}, whereProps, values), conn);
      });
    });
  };

  store.replace = function(whereProps, values, conn) {
    // The where properties will also be values of the resulting
    // row because a this operation replaces the entire row.
    values = _.extend({}, values, whereProps);
    return _update(whereProps, values, {partial: false}, conn);
  };

  function _update(whereProps, values, opts, conn) {
    var handler = conn || client;
    opts = _.defaults(opts || {}, {
      partial: true
    });
    values = _.omit(values, _.keys(defaultFields));
    return onReady
    .then(function() {
      return addWhereContext(
        addWriteContext(
          initContext(),
          values,
          opts),
        whereProps);
    })
    .then(function(ctx) {
      return handler.execTemplate(
        statements.update,
        ctx.templateVars,
        ctx.inputData);
    })
    .then(_.compose(first, derive()));
  }

  store.update = function(whereProps, values, conn) {
    return _update(whereProps, values, {partial: true}, conn);
  };

  store.getById = function(id, conn) {
    return onReady
    .then(function() {
      return store.getByIds([id], conn)
      .then(first);
    });
  };

  store.deleteById = function(id, conn) {
    return onReady
    .then(function() {
      return store.deleteByIds([id], conn);
    });
  };

  store.getByIds = function(ids, conn) {
    var handler = conn || client;
    return onReady
    .then(function() {
      return handler.execTemplate(
        statements.getByIds,
        initContext().templateVars,
        ids)
      .then(derive());
    });
  };

  store.deleteByIds = function(ids, conn) {
    var handler = conn || client;
    return onReady
    .then(function() {
      return handler.execTemplate(
        statements.deleteByIds,
        initContext().templateVars,
        ids);
    });
  };

  store.delete = function(whereProps, conn) {
    var handler = conn || client;
    return onReady
    .then(function() {
      return addWhereContext(initContext(), whereProps);
    })
    .then(function(ctx) {
      return handler.execTemplate(
        statements.delete,
        ctx.templateVars,
        ctx.inputData);
    });
  };

  store.erase = function(conn) {
    var handler = conn || client;
    return onReady
    .then(function() {
      return handler.query('DELETE FROM ' + _str.underscored(storeName));
    });
  };

  store.find = function(whereProps, conn) {
    var handler = conn || client;
    return onReady
    .then(function() {
      return addWhereContext(initContext(), whereProps);
    }).then(function(ctx) {
      return handler.execTemplate(
        statements.find,
        ctx.templateVars,
        ctx.inputData)
      .then(derive());
    });
  };

  store.findOne = function(whereProps, conn) {
    return store.find(whereProps, conn)
    .then(function(rows) {
      return rows[0] || null;
    });
  };

  store.synchronizeOnRow = function(whereProps, workFn, conn) {
    var handler = conn || client;
    return onReady
    .then(function() {
      return handler.transaction(function(txConn) {
        return addWhereContext(initContext(), whereProps)
        .then(function(ctx) {
          return txConn.execTemplate(
            statements.selectForUpdate,
            ctx.templateVars,
            ctx.inputData);
        })
        .then(function() {
          return workFn(txConn);
        });
      });
    });
  };

  return store;
};

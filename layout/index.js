'use strict';
var _str = require('underscore.string'),
    _ = require('lodash'),
    store = require('../store');

function fieldsToDDL(fields) {
  return _.transform(fields, function(ddl, spec, name) {
    name = _str.underscored(name);
    ddl.push([name, spec].join(' '));
    return ddl;
  }, []);
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

module.exports = function(client) {
  var exposed = {};


  var statements;
  var onReady = client.loadStatements(__dirname + '/sql')
  .then(function(stmts) {
    statements = stmts;
  });

  var expose = function(fn) {
    exposed[fn.name] = function() {
      var args = arguments;
      return onReady
      .then(function() {
        return fn.apply(null, args);
      });
    };
  };

  expose(ensureNamespace);
  function ensureNamespace(namespace) {
    var schema = _str.underscored(namespace);
    return namespaceExists(namespace)
    .then(function(result) {
      if (! result) {
        return client.query('CREATE SCHEMA ' + schema);
      }
    });
  }

  expose(dropNamespace);
  function dropNamespace(namespace) {
    var schema = _str.underscored(namespace);
    return namespaceExists(namespace)
    .then(function(result) {
      if (result) {
        return client.query('DROP SCHEMA ' + schema + ' CASCADE;');
      }
    });
  }

  expose(namespaceExists);
  function namespaceExists(namespace) {
    var schema = _str.underscored(namespace);
    return client.query(
      'SELECT * FROM information_schema.schemata' +
      ' WHERE schema_name = $1;', schema)
    .then(function(results) {
      return !!(results.length);
    });
  }

  expose(tableExists);
  function tableExists(specName, spec) {
    var tableName = _str.underscored(specName);
    var parts = tableName.split('.');
    return client.exec(statements.getTableInfo, {
      schema: parts[0],
      table: parts[1]
    })
    .then(function(results) {
      return !!results.length;
    });
  }

  expose(ensureTable);
  function ensureTable(specName, spec) {
    return tableExists(specName, spec)
    .then(function(exists) {
      if (!exists) {
        return addTable(specName, spec);
      }
    });
  }

  expose(getColumnInfo);
  function getColumnInfo(tableName) {
    var parts = tableName.split('.');
    return client.exec(statements.getColumnInfo, {
      schema: parts[0],
      table: parts[1]
    })
    .then(function(cols) {
      if (!cols[0]) throw new Error('Unkown store table: ' + tableName);
      return cols;
    });
  }

  expose(addTable);
  function addTable(specName, spec) {
    spec = _.defaults(spec || {}, {
        columns: {}
    });

    if (!specName) {
        throw new Error('Name of table required');
    }
    var unknownKeys = _.keys(_.omit(spec, 'columns'));
    if (unknownKeys.length !== 0) {
        throw new Error('Malformed spec; unknown keys: ' + unknownKeys);
    }

    var nameParts = specName.split('.'),
        namespace = (nameParts.length > 1) ? nameParts[0] : null,
        table = _str.underscored(specName);

    return ensureNamespace(namespace)
    .then(function() {
      return client.execTemplate(statements.createTable, {
        tableName: table,
        columnDefinitions: fieldsToDDL(spec.columns).join(',\n  ')
      });
    });

  }

  expose(addStore);
  function addStore(specName, spec) {
    spec = _.defaults(spec || {}, {
        columns: {}
    });

    spec.columns = addDefaultFields(
      spec.columns,
      _.extend(
        {},
        store.defaultFields,
        store.metaFields
      )
    );
    return addTable(specName, spec);
  }

  return exposed;

};

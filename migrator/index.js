'use strict';
var _ = require('lodash'),
    Promise = require('bluebird'),
    layoutModule = require('../layout'),
    parseMigrations = require('./parse'),
    storeModule = require('../store');

module.exports = function(client) {

  var migrator = {};
  var layout = layoutModule(client);
  var loadedMigrations = {};
  var templateVars = {};

  var statements;
  var onReady = client.loadStatements(__dirname + '/sql')
  .then(function(stmts) {
    statements = stmts;
  });

  var col = {};
  _.each(
    _.extend({}, storeModule.defaultFields, storeModule.metaFields),
    function(def, name) {
      col[name] = name + ' ' + def;
    });

  col.timestamps = col.created + ',\n  ' + col.updated;

  templateVars.col = col;

  function ensureMigrationTable(schema) {
    return layout.ensureTable(schema + '.migration', {
      columns: {
        date: 'timestamp without time zone NOT NULL',
        description: 'text NOT NULL',
        completed: 'timestamp without time zone DEFAULT now() NOT NULL',
      }});
  }

  function loadLastMigrationDate(schema) {
    return ensureMigrationTable(schema)
    .then(function() {
      return client.execTemplate(
        statements.getLastMigration,
        {schema: schema});
    })
    .then(function(result) {
      return result.length && result[0].date;
    });
  }

  function recordMigration(migration) {
    return client.execTemplate(
      statements.recordMigration,
      {schema: migration.schema},
      migration);
  }

  function migrationTableExists(schema) {
    return layout.tableExists(schema + '.migration');
  }

  migrator.clearMigrations = clearMigrations;
  function clearMigrations(schema) {
    return onReady
    .then(function() { return migrationTableExists(schema); })
    .then(function(exists) {
      if (exists) {
        return client.execTemplate(statements.clearMigrations, {schema: schema});
      }
    });
  }

  migrator.loadMigrations = loadMigrations;
  function loadMigrations(filePath, opts) {
    return parseMigrations(filePath)
    .then(function(migrations) {
      if (!migrations.length) {
        throw new Error('No migrations found: ' + filePath);
      }
      return addMigrations(migrations, opts);
    });
  }

  migrator.templateVars = templateVars;

  migrator.addMigrations = addMigrations;
  function addMigrations(migrations, opts) {
    opts = _.defaults(opts || {}, {
      schema: 'public'
    });

    var prevMigrationDate = new Date('1970-01-01');
    migrations = _.map(migrations, function(migration) {
      var date = migration.date;

      if (isNaN(date.getTime()) || !_.isDate(date)) {
        throw new TypeError(
          'Invalid date object: ' + date);
      }

      if (prevMigrationDate.getTime() === date.getTime()) {
        throw new Error(
          'Migration has identical time stamp; bad: ' + date);
      }
      if (prevMigrationDate > date) {
        throw new Error(
          'Migrations must remain ordered by date; bad: ' + date);
      }
      prevMigrationDate = date;

      return _.extend({}, {schema: opts.schema}, migration);
    });

    loadedMigrations[opts.schema] = (
      (loadedMigrations[opts.schema] || []).concat(migrations));
  }

  function getPendingMigrations() {
    return Promise.all(_.transform(
      loadedMigrations,
      function(results, setMigrations, schema) {
        results.push(
          loadLastMigrationDate(schema)
          .then(function(lastDate) {
            return _.filter(setMigrations, function(migration) {
              return (!lastDate || (migration.date > lastDate));
            });
          }));
        return results;
      }, []))
    .then(function(resultsBySet) {
      return _.sortBy(_.flatten(resultsBySet), 'date');
    });
  }

  migrator.up = migrator.runPending = runPending;
  function runPending() {
    return onReady.then(function() {
      return getPendingMigrations();
    })
    .then(function(pendingMigrations) {
      if (!pendingMigrations.length) {
        client._logger.info('No pending migrations');
        return;
      }
      return Promise.reduce(pendingMigrations, function(__, migration) {
        client._logger.info(
          'Running migration',
          _.pick(migration, 'date', 'description'));
        return (migration.template
          ? client.execTemplate(migration.template, templateVars)
          : client.exec(migration.sql))
        .then(function() {
          return recordMigration(migration);
        });
      }, null);
    });
  }

  return migrator;

};


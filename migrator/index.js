'use strict';
var _ = require('lodash'),
    Promise = require('bluebird'),
    layoutModule = require('../layout');

module.exports = function(client) {

  var migrator = {};
  var layout = layoutModule(client);
  var migrations = [];
  var lastRunMigrationDate = null;

  function loadLastMigrationDate() {
    return layout.ensureTable('dbeasy.migration', {
      columns: {
        date: 'timestamp without time zone NOT NULL',
        description: 'text NOT NULL'
      }
    })
    .then(function() {
      return client.exec('__get_last_migration');
    })
    .then(function(result) {
      lastRunMigrationDate = result.length && result[0].date;
    });
  }

  var onReady = client.prepareDir(__dirname + '/sql')
  .then(function() {
    return loadLastMigrationDate();
  });

  function recordMigration(migration) {
    return client.exec('__record_migration', migration);
  }

  migrator.clearMigrations = clearMigrations;
  function clearMigrations() {
    lastRunMigrationDate = null;
    return client.exec('__clear_migrations');
  }

  migrator.collector = collector;
  function collector() {
    var prevMigrationDate;
    return function migration(isoDateStr, description) {
      var date = new Date(isoDateStr);
      if (isNaN(date.getTime())) {
        throw new Error(
          'Invalid ISO date string: ' + isoDateStr);
      }

      if (prevMigrationDate && (prevMigrationDate.getTime() === date.getTime())) {
        throw new Error(
          'Migration has identical time stamp; bad: ' + date);
      }
      if (prevMigrationDate && (prevMigrationDate > date)) {
        throw new Error(
          'Migrations must remain ordered by date; bad: ' + date);
      }

      prevMigrationDate = date;

      var api = {};
      var migration_ = {
        date: date,
        description: description || 'unnamed',
        tasks: []
      };
      api.addStore = function(specName, spec) {
        migration_.tasks.push(function() {
          return layout.addStore(specName, spec);
        });
        return api;
      };
      if (lastRunMigrationDate >= date) {
        return api;
      }

      migrations.push(migration_);

      return api;
    };
  }

  migrator.runPending = runPending;
  function runPending() {
    return Promise.reduce(migrations, function(__, migration) {
      return Promise.reduce(migration.tasks, function(__, task) {
        return task();
      }, null)
      .then(function() {
        return recordMigration(migration);
      });
    }, null)
    .then(function() {
      migrations = [];
      return loadLastMigrationDate();
    });
  }

  return onReady.then(function() {
    return migrator;
  });

};


'use strict';
var _ = require('lodash'),
    Promise = require('bluebird'),
    layoutModule = require('../layout');

module.exports = function(client) {

  var migrator = {};
  var layout = layoutModule(client);
  var migrations = {};
  var onReady = client.prepareDir(__dirname + '/sql');


  function ensureMigrationTable(setName) {
    return layout.ensureTable(setName + '.migration', {
      columns: {
        date: 'timestamp without time zone NOT NULL',
        description: 'text NOT NULL',
        completed: 'timestamp without time zone DEFAULT now() NOT NULL',
      }});
  }

  function loadLastMigrationDate(setName) {
    return ensureMigrationTable(setName)
    .then(function() {
      return client.execTemplate(
        '__get_last_migration',
        {setName: setName});
    })
    .then(function(result) {
      return result.length && result[0].date;
    });
  }

  function recordMigration(migration) {
    return client.execTemplate(
      '__record_migration',
      {setName: migration.setName},
      migration);
  }

  function migrationTableExists(setName) {
    return layout.tableExists(setName + '.migration');
  }

  migrator.clearMigrations = clearMigrations;
  function clearMigrations(setName) {
    return onReady
    .then(function() { return migrationTableExists(setName); })
    .then(function(exists) {
      if (exists) {
        return client.execTemplate('__clear_migrations', {setName: setName});
      }
    });
  }

  migrator.collector = collector;
  function collector(setName) {
    if (! _.isString(setName)) {
      throw new TypeError('Expected a string name identifying the migration set');
    }
    var setMigrations = [];
    migrations[setName] = setMigrations;
    var prevMigrationDate;

    function migration(isoDateStr, description) {
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
        setName: setName,
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

      setMigrations.push(migration_);

      return api;
    }

    return migration;
  }

  function getPendingMigrations() {
    return Promise.all(_.transform(
      migrations,
      function(results, setMigrations, setName) {
        results.push(
          loadLastMigrationDate(setName)
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
        return Promise.reduce(migration.tasks, function(__, task) {
          return task();
        }, null)
        .then(function() {
          return recordMigration(migration);
        });
      }, null);
    });
  }

  return migrator;

};


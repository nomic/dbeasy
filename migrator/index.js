'use strict';
var _ = require('lodash'),
    Promise = require('bluebird'),
    layoutModule = require('../layout'),
    parseMigrations = require('./parse'),
    storeModule = require('../store');

var CANDIDATE_STATUS = {
  PENDING: 'PENDING',
  MISSING: 'MISSING'
};

module.exports = function(client) {

  var migrator = {};
  var layout = layoutModule(client);
  var candidateMigrations = [];
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
        date: 'timestamp with time zone NOT NULL',
        description: 'text NOT NULL',
        completed: 'timestamp with time zone DEFAULT now() NOT NULL',
      }});
  }

  function getCommittedMigrations(schema) {
    return ensureMigrationTable(schema)
      .then(function() {
        return client.execTemplate(
          statements.getMigrations,
          {schema: schema});
      });
  }

  function recordMigration(migration, schema) {
    return client.execTemplate(
      statements.recordMigration,
      {schema: schema},
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

      return _.clone(migration);
    });

    candidateMigrations = candidateMigrations.concat(migrations);
  }

  migrator.getStatus = getStatus;
  function getStatus(schema) {
    return onReady.then(function() {
      return getCommittedMigrations(schema);
    })
      .then(function(committedMigrations) {
        var candidate = _.map(candidateMigrations, function(m) {
          return _.extend({}, m, {isCommitted: false});
        });
        var committed = _.map(committedMigrations, function(m) {
          return _.extend({}, m, {isCommitted: true});
        });
        var committedByDate = _.indexBy(committed, 'date');
        var candidateByDate = _.indexBy(candidate, 'date');
        var allByDate = _.extend({}, candidateByDate, committedByDate);
        var allMigrations = _.sortBy(_.values(allByDate), 'date');

        var hasMissing = false;
        var hasPending = false;
        var hasComitted = false;

        allMigrations = _.map(allMigrations.reverse(), function(m) {
          if (!hasComitted && !m.isCommitted) {
            m.candidateStatus = CANDIDATE_STATUS.PENDING;
            hasPending = true;
          }
          if (hasComitted && !m.isCommitted) {
            m.candidateStatus = CANDIDATE_STATUS.MISSING;
            hasMissing = true;
          }
          if (!hasComitted && m.isCommitted) {
            hasComitted = true;
          }
          return m;
        }).reverse();

        return [allMigrations, hasPending, hasMissing];
      });
  }

  migrator.up = migrator.runPending = runPending;
  function runPending(schema) {
    return onReady.then(function() {
      return getStatus(schema);
    })
      .then(_.spread(function(migrations, hasPending, hasMissing) {
        if (!hasPending && !hasMissing) {
          client._logger.info('No pending migrations');
          return;
        }
        if (hasMissing) {
          throw new Error('One ore more migrations were missed');
        }

        return Promise.reduce(
          _.filter(migrations, {candidateStatus: CANDIDATE_STATUS.PENDING}),
          function(__, migration) {
            client._logger.info(
              'Running migration',
              _.pick(migration, 'date', 'description'));
            return (migration.template
                    ? client.execTemplate(migration.template, templateVars)
                    : client.exec(migration.sql))
              .then(function() {
                return recordMigration(migration, schema);
              });
          }, null);
      }));
  }

  return migrator;

};

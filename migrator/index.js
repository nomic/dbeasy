'use strict';
var _ = require('lodash'),
    Promise = require('bluebird'),
    layoutModule = require('../layout'),
    storeModule = require('../store'),
    fs = Promise.promisifyAll(require('fs')),
    path = require('path'),
    assert = require('assert'),
    moment = require('moment-timezone');

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

  function makeMigration(filePath, date, description, sql) {
    assert(_.isString(filePath));
    assert(_.isString(description));
    assert(!_.isEmpty(description));
    assert(_.isDate(date));
    var migration = {
      path: filePath,
      date: date,
      description: description,
      template: sql
    };
    return migration;
  }

  function readMigration(filePath) {
    var filename = path.basename(filePath, '.sql');

    return fs.readFileAsync(filePath).then(function(data) {
      return makeMigration(
        filePath,
        new Date(_.first(filename.split('_'))),
        _.rest(filename.split('_')).join('_'),
        data.toString()
      );
    });

  }

  function ensureMigrationTable(table) {
    return layout.ensureTable(table, {
      columns: {
        date: 'timestamp with time zone NOT NULL',
        description: 'text NOT NULL',
        completed: 'timestamp with time zone DEFAULT now() NOT NULL',
      }});
  }

  function getCommittedMigrations(table) {
    return ensureMigrationTable(table)
      .then(function() {
        return client.execTemplate(
          statements.getMigrations,
          {table: table});
      });
  }

  function recordMigration(migration, table) {
    return client.execTemplate(
      statements.recordMigration,
      {table: table},
      migration);
  }

  function migrationTableExists(table) {
    return layout.tableExists(table + '.migration');
  }

  migrator.clearMigrations = clearMigrations;
  function clearMigrations(table) {
    return onReady
    .then(function() { return migrationTableExists(table); })
    .then(function(exists) {
      if (exists) {
        return client.execTemplate(statements.clearMigrations, {table: table});
      }
    });
  }

  migrator.loadMigration = loadMigration;
  function loadMigration(filePath, opts) {
    return readMigration(filePath)
    .then(function(migration) {
      return addMigration(migration, opts);
    });
  }

  migrator.templateVars = templateVars;

  migrator.addMigrations = addMigrations;
  function addMigrations(migrations) {
    _.each(migrations, addMigration);
  }

  migrator.addMigration = addMigration;
  function addMigration(migration) {

    var prevMigrationDate =
          (_.last(candidateMigrations) || {date : new Date('1970-01-01')}).date;
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

    candidateMigrations.push(_.cloneDeep(migration));
  }

  migrator.createMigration = createMigration;
  function createMigration(name, dir) {
    assert(name, 'migration name required');
    assert(dir, 'migration directory required');
    var timestamp =
          moment().tz('America/Los_Angeles').format('YYYY-MM-DDTHH:mm:ss');

    var filename =
          timestamp + '_' + _.snakeCase(name) + '.sql';
    var contents = [
      '-- ',
      '-- ' + _.snakeCase(name),
      '-- ',
      '',
      ''
    ].join('\n');

    var path = dir + '/' + filename;
    return fs.writeFileAsync(path, contents)
      .then(function() {
        return path;
      });
  }

  migrator.redate = redate;
  function redate(filePath) {
    assert(filePath, 'a path is required');
    var namePart = _.rest(path.basename(filePath).split('_')).join('_');
    var dir = path.dirname(filePath);
    var timestamp =
          moment().tz('America/Los_Angeles').format('YYYY-MM-DDTHH:mm:ss');

    var newPath = dir + '/' + timestamp + '_' + namePart;

    return fs.renameAsync(filePath, newPath)
      .then(function() {
        return newPath;
      });
  }

  migrator.getStatus = getStatus;
  function getStatus(table) {
    return onReady.then(function() {
      return getCommittedMigrations(table);
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
  function runPending(table) {
    return onReady.then(function() {
      return getStatus(table);
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
                return recordMigration(migration, table);
              });
          }, null);
      }));
  }

  return migrator;

};

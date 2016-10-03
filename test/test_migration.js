"use strict";
/*global suite: false, test: false, setup: false*/

//allow chai syntax like expect(foo).to.be.empty
/*jshint expr: true*/

var Promise = require('bluebird'),
    _ = require('lodash'),
    expect = require('chai').expect,
    util = require('./util'),
    makeMigrator = require('../migrator'),
    layout = require('../layout');

Promise.longStackTraces();

suite('Migration', function() {

  var testSqlPath = __dirname + '/test_migration_sql/';

  suite('Running', function() {
    var client;
    var migrator;
    var SCHEMA = 'school';
    var migrationTable = 'migration.migration';

    setup(function() {
      if (client) client.close();
      client = util.createDb({poolSize: 3, enableStore: true});
      migrator = makeMigrator(client);
      return migrator.clearMigrations(migrationTable)
      .then(function() {
        return layout(client).dropNamespace('school');
      })
      .then(function() {
        return layout(client).ensureNamespace('school');
      });
    });

    test('Create some tables', function() {
      migrator.addMigrations([{
        date: new Date('2014-11-11T01:24'),
        description: 'create rooms',
        sql: 'CREATE TABLE school.classroom(); CREATE TABLE school.cafeteria();'
      }]);

      return migrator.runPending(migrationTable)
      .then(function() {
        return Promise.all([
          client.query('SELECT * FROM school.classroom;'),
          client.query('SELECT * FROM school.cafeteria;')
        ]);
      })
      .spread(function(classrooms, cafeterias) {
        expect(classrooms).to.be.empty;
        expect(cafeterias).to.be.empty;
      });
    });

    test('Run multiple migrations', function() {
      migrator.addMigrations([{
        date: new Date('2014-11-11T01:24'),
        description: 'create rooms',
        sql: 'CREATE TABLE school.classroom();'
      },{
        date: new Date('2014-11-12T01:24'),
        description: 'create rooms',
        sql: 'CREATE TABLE school.cafeteria();'
      }]);

      return migrator.runPending(migrationTable)
      .then(function() {
        return Promise.all([
          client.query('SELECT * FROM school.classroom;'),
          client.query('SELECT * FROM school.cafeteria;')
        ]);
      })
      .spread(function(classrooms, cafeterias) {
        expect(classrooms).to.be.empty;
        expect(cafeterias).to.be.empty;
      });
    });

    test('Identify missed migrations', function() {
      migrator.addMigrations([{
        date: new Date('2014-11-12T01:24'),
        description: 'noop',
        sql: 'SELECT;'
      },{
        date: new Date('2014-12-12T01:24'),
        description: 'noop',
        sql: 'SELECT;'
      }]);

      return Promise.resolve()
        .then(function() {
          return migrator
            .getStatus(migrationTable)
            .then(_.spread(function(items, hasPending, hasMissing) {
              expect(hasPending).to.equal(true);
              expect(hasMissing).to.equal(false);
            }));
        })
        .then(function() {
          return migrator
            .runPending(migrationTable)
            .then(function() {
              return migrator.getStatus(migrationTable);
            })
            .then(_.spread(function(items, hasPending, hasMissing) {
              expect(hasPending).to.equal(false);
              expect(hasMissing).to.equal(false);
            }));
        })
        .then(function() {
          migrator = makeMigrator(client);
          migrator.addMigrations([{
            date: new Date('2014-11-13T01:24'),
            description: 'noop',
            sql: 'SELECT;'
          }]);
          return migrator
            .getStatus(migrationTable)
            .then(_.spread(function(items, hasPending, hasMissing) {
              expect(hasPending).to.equal(false);
              expect(hasMissing).to.equal(true);
              expect(items[1]).to.eql({
                date: new Date('2014-11-13T01:24'),
                description: 'noop',
                sql: 'SELECT;',
                isCommitted: false,
                candidateStatus: 'MISSING'
              });
            }));
        });

    });


    test('Do not allow misordered migrations', function() {
      return Promise.try(function() {
        migrator.addMigrations([{
          date: new Date('2014-11-12T01:24'),
          description: 'create rooms',
          sql: ''
        },{
          date: new Date('2014-11-12T01:23'),
          description: 'create rooms',
          sql: ''
        }], {schema: 'school'});
      })
      .then(function() {
        throw new Error('Should not have allowed misordered migrations');
      }, _.noop);
    });

    test('Do not allow identical timestamps', function() {
      return Promise.try(function() {
        migrator.addMigrations([{
          date: new Date('2014-11-12T01:24'),
          description: 'create rooms',
          sql: ''
        },{
          date: new Date('2014-11-12T01:24'),
          description: 'create rooms',
          sql: ''
        }], {schema: 'school'});
      })
      .then(function() {
        throw new Error('Should not have allowed identical timestamps');
      }, _.noop);
    });

    test('Do not run the same migration twice', function() {
      migrator.addMigrations([{
        date: new Date('2014-11-11T01:24'),
        description: 'create rooms',
        sql: 'CREATE TABLE school.classroom();'
      }]);
      return migrator.runPending(migrationTable)
        .then(function() {
          return migrator.runPending(migrationTable);
        })
        .then(function() {
          var migrator = makeMigrator(client);
          migrator.addMigrations([{
            date: new Date('2014-11-11T01:24'),
            description: 'create rooms',
            sql: 'CREATE TABLE school.classroom();'
          }], {schema: 'school'});
          return migrator.runPending(migrationTable);
        });

    });

  });

});

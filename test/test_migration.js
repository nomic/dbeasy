"use strict";
/*global suite: false, test: false, setup: false*/

//allow chai syntax like expect(foo).to.be.empty
/*jshint expr: true*/

var Promise = require('bluebird'),
    _ = require('lodash'),
    expect = require('chai').expect,
    assert = require('assert'),
    util = require('./util'),
    makeMigrator = require('../migrator'),
    parse = require('../migrator/parse'),
    layout = require('../layout');

Promise.longStackTraces();

suite('Migration', function() {

  var testSqlPath = __dirname + '/test_migration_sql/';
  suite('Parsing', function() {


    test('Empty file yields emtpy array', function() {
      return parse(testSqlPath + '00_empty.sql')
      .then(function(migrations) {
        expect(migrations).to.have.length(0);
      });
    });

    test('Blank migration parses', function() {
      return parse(testSqlPath + '01_comment_only.sql')
      .then(function(migrations) {
        var migration = migrations[0];
        expect(migration.date).to.eql(new Date('2014-11-10T20:00'));
        expect(migration.description).to.equal('Description goes here');
      });
    });

    test('File with single migration parses', function() {
      return parse(testSqlPath + '02_single_statement.sql')
      .then(function(migrations) {
        var migration = migrations[0];
        expect(migration.template).to.contain('SELECT 1 FROM emp;');
      });
    });

    test('File with multiple migrations parses', function() {
      return parse(testSqlPath + '04_multiple_migrations.sql')
      .then(function(migrations) {
        expect(migrations).to.have.length(2);
        expect(migrations[0].description).to.equal('Description 1 goes here');
        expect(migrations[1].description).to.equal('Description 2 goes here');
      });
    });

  });

  suite('Running', function() {
    var client;
    var migrator;
    var migration;

    setup(function() {
      if (client) client.close();
      client = util.createDb({poolSize: 3, enableStore: true});
      migrator = makeMigrator(client);
      return migrator.clearMigrations('school')
      .then(function() {
        return layout(client).dropNamespace('school');
      });
    });

    test('Create some tables', function() {
      migrator.addMigrations([{
        date: new Date('2014-11-11T01:24'),
        description: 'create rooms',
        sql: 'CREATE TABLE school.classroom();\nCREATE TABLE school.cafeteria();'
      }], {schema: 'school'});

      return migrator.runPending()
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
      }], {schema: 'school'});

      return migrator.runPending()
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

    test('Do not allow misordered migrations', function() {
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
      }], {schema: 'school'});
      return migrator.runPending()
      .then(function() {
        return migrator.runPending();
      })
      .then(function() {
        migrator.addMigrations([{
          date: new Date('2014-11-11T01:24'),
          description: 'create rooms',
          sql: 'CREATE TABLE school.classroom();'
        }], {schema: 'school'});
        return migrator.runPending();
      });

    });

    test('Catch error on empty migration file', function() {
      return migrator.loadMigrations(
        testSqlPath + '00_empty.sql',
        {schema: 'school'})
      .then(function() {
        throw new Error('Expected exception');
      }, function(err) {
        expect(err.message).to.match(/00_empty.sql/);
      });

    });

    test('Load and run migrations from file', function() {
      return migrator.loadMigrations(
        testSqlPath + '10_create_classroom_table.sql',
        {schema: 'school'})
      .then(function() {
        return migrator.runPending();
      })
      .then(function() {
        return Promise.all([
          client.query('SELECT * FROM school.classroom;'),
        ]);
      })
      .spread(function(classrooms) {
        expect(classrooms).to.be.empty;
      });
    });

    test('Load and run migration with template', function() {
      migrator.templateVars['table'] = 'school.classroom';
      return migrator.loadMigrations(
        testSqlPath + '11_create_table.sql.hbs',
        {schema: 'school'})
      .then(function() {
        return migrator.runPending();
      })
      .then(function() {
        return client.query('SELECT * FROM school.classroom;');
      })
      .then(function(classrooms) {
        expect(classrooms).to.be.empty;
      });
    });

  });

});
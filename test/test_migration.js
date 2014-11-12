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
    layout = require('../layout');

Promise.longStackTraces();

suite('Migration', function() {

  var client;
  var migrator;
  var migration;

  setup(function() {
    if (client) client.close();
    client = util.createDb({poolSize: 3, enableStore: true});
    return makeMigrator(client)
    .then(function(migrator_) {
      migrator = migrator_;
      return migrator.clearMigrations()
      .then(function() {
        migration = migrator.collector();
        return layout(client).dropNamespace('school');
      });
    });
  });

  test('Create some stores', function() {
    migration('2014-11-11T01:24', 'Create rooms')
    .addStore('school.classroom')
    .addStore('school.cafeteria');

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
    migration('2014-11-11T01:24', 'Create classroom')
    .addStore('school.classroom');

    migration('2014-11-11T01:25', 'Create cafeteria')
    .addStore('school.cafeteria');

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
      migration('2014-11-11T01:24', 'Create classroom')
      .addStore('school.classroom');

      migration('2014-11-11T01:23', 'Create cafeteria')
      .addStore('school.cafeteria');
    })
    .then(function() {
      throw new Error('Should not have allowed misordered migrations');
    }, _.noop);
  });

  test('Do not allow identical timestamps', function() {
    return Promise.try(function() {
      migration('2014-11-11T01:24', 'Create classroom')
      .addStore('school.classroom');

      migration('2014-11-11T01:24', 'Create cafeteria')
      .addStore('school.cafeteria');
    })
    .then(function() {
      throw new Error('Should not have allowed identical timestamps');
    }, _.noop);
  });

  test('Do not run the same migration twice', function() {
    migration('2014-11-11T01:24', 'Create classroom')
    .addStore('school.classroom');
    return migrator.runPending()
    .then(function() {
      return migrator.runPending();
    })
    .then(function() {
      migration = migrator.collector();
      migration('2014-11-11T01:24', 'Create classroom')
      .addStore('school.classroom');
      return migrator.runPending();
    });

  });

});
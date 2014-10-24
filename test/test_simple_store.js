"use strict";
/*global suite: false, test: false, setup: false*/

//allow chai syntax like expect(foo).to.be.empty
/*jshint expr: true*/

var Promise = require("bluebird"),
    _ = require("lodash"),
    dbeasy = require("../index.js"),
    expect = require("chai").expect,
    ss = require("./util").createSimpleStore;

suite("Simple Store", function() {

    test("Create a default table", function() {
      var store = ss({poolSize: 1});
      return store.query('DROP SCHEMA foo CASCADE;')
      .finally(function() {
        return store.defineSchema('foo')
        .then(function() {
          return store.defineEntity('foo.fooBar');
        })
        .then(function() {
          return store.query('SELECT * FROM foo.foo_bar;')
            .then(function(results) {
              expect(results).to.be.empty;
            });
        })
        .then(function() {
          return store.upsert('foo.fooBar', {creator: {id: '9'}});
        })
        .then(function() {
          return store.getById('foo.fooBar', '1');
        })
        .then(function(results) {
          expect(results).to.have.property('id', '1');
          expect(results.creator).to.eql({id: '9'});
          expect(results.created).to.be.a('Date');
          expect(results.updated).to.be.a('Date');
          expect(results).to.not.have.property('deleted');
        })
        .then(function() {
          return store.deleteById('foo.fooBar', '1');
        })
        .then(function() {
          return Promise.all([
            store.getById('foo.fooBar', '1'),
            store.queryRaw('SELECT __deleted FROM foo.foo_bar WHERE id = \'1\'')
          ]);
        })
        .spread(function(getterResults, queryResults) {
          expect(getterResults).to.be.null;
          expect(queryResults).to.have.length(1);
          expect(queryResults[0].__deleted).to.be.a('Date');
        });
      });
    });

});

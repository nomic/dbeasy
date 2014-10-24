"use strict";
/*global suite: false, test: false, setup: false*/
var Promise = require("bluebird"),
    _ = require("lodash"),
    dbeasy = require("../index.js"),
    expect = require("chai").expect,
    ss = require("./util").createSimpleStore;

suite("Simple Store", function() {

    var db;

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
              expect(results.length).to.equal(0);
            });
        })
        .then(function() {
          return store.upsert('foo.fooBar', {})
          .then(function() {
            return store.getById('foo.fooBar', '1')
            .then(function(results) {
              expect(results).to.have.property('id', '1');
              expect(results).to.have.property('created');
              expect(results).to.have.property('updated');
            });
          });
        });
      });
    });

});

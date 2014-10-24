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

    test("Operations on a default entity", function() {
      var store = ss({poolSize: 2});
      return store.query('DROP SCHEMA foo CASCADE;')
      .catch(_.noop)
      .then(function() {
        return store.addNamespace('foo');
      })
      .then(function() {
        return store.addSpec('foo.fooBar');
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
        return Promise.all([
          store.getById('foo.fooBar', '1'),
          store.queryRaw('SELECT __deleted, __bag FROM foo.foo_bar WHERE id = \'1\'')
        ]);
      })
      .spread(function(getterResult, queryResult) {
        expect(getterResult).to.have.property('id', '1');
        expect(getterResult.creator).to.eql({id: '9'});
        expect(getterResult.created).to.be.a('Date');
        expect(getterResult.updated).to.be.a('Date');
        expect(getterResult).to.not.have.property('__deleted');
        expect(getterResult).to.not.have.property('__bag');
        expect(queryResult).to.have.length(1);
        expect(queryResult[0].__deleted).to.be.null;
        expect(queryResult[0].__bag).to.eql({});
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

    test("Custom fields and data bag", function() {
      var store = ss({poolSize: 2});
      return store.query('DROP SCHEMA biz CASCADE;')
      .catch(_.noop)
      .then(function() {
        return store.addNamespace('biz');
      })
      .then(function() {
        return store.addSpec('biz.emp', {
          fields: {
            firstName: 'text NOT NULL'
          },
          refs: {
            dept: 'bigint'
          }
        });
      })
      .then(function() {
        return store.upsert('biz.emp', {
          creator: {id: '3'},
          firstName: 'Mel',
        });
      })
      .then(function(result) {
        expect(result).to.have.property('id', '1');
        expect(result).to.have.property('firstName', 'Mel');
        expect(result).to.not.have.property('dept');
      })
      .then(function() {
        return store.upsert('biz.emp', {
          id: '1',
          creator: {id: '3'},
          firstName: 'Melly',
          interests: {favoriteSandwich: 'Falafel'},
        });
      })
      .then(function(result) {
        console.log(result);
        expect(result).to.have.property('id', '1');
        expect(result).to.have.property('firstName', 'Melly');
        expect(result).to.have.property('interests');
        expect(result.interests).to.eql({favoriteSandwich: 'Falafel'});
        expect(result).to.not.have.property('dept');
      });

    });

    test("Update a spec", function() {
      var store = ss({poolSize: 2});
      return store.query('DROP SCHEMA biz CASCADE;')
      .catch(_.noop)
      .then(function() {
        return store.addNamespace('biz');
      })
      .then(function() {
        return store.addSpec('biz.emp', {
          fields: {
            firstName: 'text NOT NULL'
          },
        });
      })
      .then(function() {
        return store.addSpec('biz.emp', {
          fields: {
            firstName: 'text'
          },
          ref: {
            dept: 'bigint'
          }
        });
      })
      .then(function() {
        return store.upsert('biz.emp', {
          creator: {id: '3'},
          dept: {id: '2'}
        }).then(function(emp) {
          return store.getById('biz.emp', emp.id);
        });
      })
      .then(function(result) {
        expect(result).to.not.have.property('firstName');
        expect(result).to.have.property('dept');
        expect(result.dept).to.eql({id: '2'});
      });

    });

});

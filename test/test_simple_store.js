"use strict";
/*global suite: false, test: false, setup: false*/

//allow chai syntax like expect(foo).to.be.empty
/*jshint expr: true*/

var Promise = require('bluebird'),
    _ = require('lodash'),
    expect = require('chai').expect,
    assert = require('assert'),
    BAG_COL = require('../store').BAG_COL,
    ss = require('./util').createSimpleStore;

suite('Store', function() {

  var store;


  setup(function() {
    store = ss({poolSize: 1});
    return store.dropNamespace('biz')
    .catch(_.noop);
  });

  test('Operations on a default entity', function() {
    return store.dropNamespace('foo')
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

  test('Custom fields and data bag', function() {
    return store.addSpec('biz.emp', {
      fields: {
        firstName: 'text NOT NULL'
      },
      refs: {
        dept: 'bigint'
      }
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
      expect(result).to.have.property('id', '1');
      expect(result).to.have.property('firstName', 'Melly');
      expect(result).to.have.property('interests');
      expect(result.interests).to.eql({favoriteSandwich: 'Falafel'});
      expect(result).to.not.have.property('dept');
    });

  });

  test('Update a spec', function() {
    return store.addSpec('biz.emp', {
      fields: {
        firstName: 'text NOT NULL'
      },
    })
    .then(function() {
      return store.addSpec('biz.emp', {
        fields: {
          firstName: 'text'
        },
        refs: {
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

  test('Catch invalid spec', function() {
    return Promise.try(function() {
      return store.addSpec('biz.emp', {
        garbage: {
          icky: 'mess'
        },
      });
    })
    .then(function() {
      assert(0, 'Should have thrown error on bad spec');
    })
    .catch(_.noop);

  });

  test('Data not lost when spec is equivalent', function() {
    return store.addSpec('biz.emp', {
      fields: {
        firstName: 'text'
      },
      refs: {
        dept: 'bigint'
      }
    })
    .then(function() {
      return store.upsert('biz.emp', {
        creator: {id: 1},
        firstName: 'Joe'
      });
    })
    .then(function() {
      return store.addSpec('biz.emp', {
        refs: {
          dept: 'bigint'
        },
        fields: {
          firstName: 'text'
        },
      });
    })
    .then(function() {
      return store.query('SELECT * FROM biz.emp;');
    })
    .then(function(result) {
      expect(result[0]).to.have.property('firstName', 'Joe');
    });

  });

});

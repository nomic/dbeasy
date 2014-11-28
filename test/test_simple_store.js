"use strict";
/*global suite: false, test: false, setup: false*/

//allow chai syntax like expect(foo).to.be.empty
/*jshint expr: true*/

var Promise = require('bluebird'),
    _ = require('lodash'),
    expect = require('chai').expect,
    assert = require('assert'),
    util = require('./util'),
    layoutModule = require('../layout');

Promise.longStackTraces();

suite('Store', function() {

  var client;
  var layout;


  setup(function() {
    if (client) client.close();
    client = util.createDb({poolSize: 3, enableStore: true});
    layout = layoutModule(client);
    return layout.dropNamespace('bigBiz');
  });

  test('Operations on a default entity', function() {
    var store = null;
    return layout.dropNamespace('foo')
    .then(function() {
      return layout.addStore('foo.fooBar')
      .then(function() {
        store = client.store('foo.fooBar');
        return;
      });
    })
    .then(function() {
      return client.query('SELECT * FROM foo.foo_bar;')
        .then(function(results) {
          expect(results).to.be.empty;
        });
    })
    .then(function() {
      return store.insert({});
    })
    .then(function() {
      return Promise.all([
        store.getById('1'),
        client.queryRaw('SELECT __deleted, __bag FROM foo.foo_bar WHERE id = \'1\'')
      ]);
    })
    .spread(function(getterResult, queryResult) {
      expect(getterResult).to.have.property('id', '1');
      expect(getterResult.created).to.be.a('Date');
      expect(getterResult.updated).to.be.a('Date');
      expect(getterResult).to.not.have.property('__deleted');
      expect(getterResult).to.not.have.property('__bag');
      expect(queryResult).to.have.length(1);
      expect(queryResult[0].__deleted).to.be.null;
      expect(queryResult[0].__bag).to.eql({});
    })
    .then(function() {
      return store.deleteById('1');
    })
    .then(function() {
      return Promise.all([
        store.getById('1'),
        client.queryRaw('SELECT __deleted FROM foo.foo_bar WHERE id = \'1\'')
      ]);
    })
    .spread(function(getterResults, queryResults) {
      expect(getterResults).to.be.null;
      expect(queryResults).to.have.length(1);
      expect(queryResults[0].__deleted).to.be.a('Date');
    });
  });

  test('Custom fields and data bag', function() {
    var store;
    return layout.addStore('bigBiz.emp', {
      columns: {
        firstName: 'text NOT NULL',
        deptId: 'bigint'
      }
    }).then(function() {
      store = client.store('bigBiz.emp');
      return store.insert({
        firstName: 'Mel',
      });
    })
    .then(function(result) {
      expect(result).to.have.property('id', '1');
      expect(result).to.have.property('firstName', 'Mel');
      expect(result).to.not.have.property('dept');
    })
    .then(function() {
      return store.replace(
        { id: '1' },
        {
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
    })
    .then(function() {
      return store.update(
        { id: '1' },
        {
          deptId: '4',
        });
    })
    .then(function(result) {
      expect(result).to.have.property('firstName', 'Melly');
      expect(result).to.have.property('deptId', '4');
      assert(result.updated > result.created,
        'expected updated to be greater than created');
    });

  });

  test('Falsey updates are correctly applied', function() {
    var store;
    return layout.addStore('bigBiz.emp', {
      columns: {
        isHappy: 'boolean NOT NULL DEFAULT true',
        footnote: 'text',
      }
    })
    .then(function() {
      store = client.store('bigBiz.emp');
      return store.insert({
        isHappy: false,
        footnote: ''
      });
    })
    .then(function(result) {
      expect(result).to.have.property('isHappy', false);
      expect(result).to.have.property('footnote', '');
    });
  });

  test('delsert', function() {
    var store;
    return layout.addStore('bigBiz.emp', {
      columns: {
        topic: 'text',
        rant: 'text'
      }
    })
    .then(function() {
      store = client.store('bigBiz.emp');
      return store.delsert({topic: 'politics'}, {rant: 'blarg!'});
    })
    .then(function(result) {
      expect(result).to.have.property('topic', 'politics');
      expect(result).to.have.property('id', '1');
      expect(result).to.have.property('rant', 'blarg!');
    })
    .then(function() {
      store = client.store('bigBiz.emp');
      return store.delsert({topic: 'politics'}, {rant: 'roar!'});
    })
    .then(function(result) {
      expect(result).to.have.property('topic', 'politics');
      expect(result).to.have.property('id', '2');
      expect(result).to.have.property('rant', 'roar!');
    });
  });

  test('Adding a store twice ends in error', function() {
    return layout.addStore('bigBiz.emp', {
      columns: {
        firstName: 'text NOT NULL'
      },
    })
    .then(function() {
      return layout.addStore('bigBiz.emp', {
        columns: {
          firstName: 'text',
          deptId: 'bigint'
        }
      });
    })
    .then(function() {
      throw new Error("Expected failure on second addStore()");
    }, _.noop);
  });

  test('Catch invalid spec', function() {
    return Promise.try(function() {
      return layout.addStore('bigBiz.emp', {
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

  test('Omit default fields', function() {
    return layout.addStore('bigBiz.emp', {
      columns: {
        id: false,
      }
    })
    .then(function() {
      return client.store('bigBiz.emp').insert({});
    })
    .then(function(result) {
      expect(result).to.not.have.property('id');
    });
  });

  test('findOne()', function() {
    var store;
    return layout.addStore('bigBiz.emp')
    .then(function() {
      store = client.store('bigBiz.emp');
      return store.insert({});
    })
    .then(function() {
      return store.findOne({id: '1'});
    })
    .then(function(result) {
      expect(result).to.have.property('id', '1');
    });
  });

  test('Derived field', function() {
    var store;
    return layout.addStore('bigBiz.emp', {
      columns: {
        firstName: 'text',
        lastName: 'text',
      }
    })
    .then(function() {
      store = client.store('bigBiz.emp', {
        derived: {
          name: function(record) {
            return record.firstName + ' ' + record.lastName;
          }
        }
      });
      return store.insert({
        // http://en.wikipedia.org/wiki/Mel_Blanc
        firstName: 'Mel',
        lastName: 'Blank',
        name: 'ignored'
      });
    })
    .then(function(result) {
      expect(result).to.have.property('name', 'Mel Blank');
      return client.queryRaw('SELECT * FROM big_biz.emp WHERE id=$1', result.id)
      .then(_.first);
    })
    .then(function(result) {
      expect(result).to.have.property('first_name');
      expect(result.__bag).to.not.have.property('name');
    });
  });

});


"use strict";
/*global suite: false, test: false, setup: false*/

//allow chai syntax like expect(foo).to.be.empty
/*jshint expr: true*/

var Promise = require('bluebird'),
    _ = require('lodash'),
    expect = require('chai').expect,
    assert = require('assert'),
    util = require('./util'),
    makeMigrator = require('../migrator');

Promise.longStackTraces();

suite('Store', function() {

  var client;
  var migrator;


  setup(function() {
    if (client) client.close();
    client = util.createDb({poolSize: 3, enableStore: true});
    migrator = makeMigrator(client);
    return migrator.dropNamespace('bigBiz');
  });

  test('Operations on a default entity', function() {
    var store = null;
    return migrator.dropNamespace('foo')
    .then(function() {
      return migrator.ensureStore('foo.fooBar')
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
    return migrator.ensureStore('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL',
        deptId: 'bigint'
      }
    }).then(function() {
      store = client.store('bigBiz.emp');
      return store.insert({
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
      return store.replace(
        { id: '1' },
        {
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
      expect(result.dept).to.have.property('id', '4');
      assert(result.updated > result.created,
        'expected updated to be greater than created');
    });

  });

  test('Update a spec', function() {
    var store;
    return migrator.ensureStore('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL'
      },
    })
    .then(function() {
      return migrator.ensureStore('bigBiz.emp', {
        fields: {
          firstName: 'text',
          deptId: 'bigint'
        }
      });
    })
    .then(function() {
      store = client.store('bigBiz.emp');
      return store.insert({
        creator: {id: '3'},
        dept: {id: '2'}
      });
    })
    .then(function(emp) {
      return store.getById(emp.id);
    })
    .then(function(result) {
      expect(result).to.not.have.property('firstName');
      expect(result).to.have.property('dept');
      expect(result.dept).to.eql({id: '2'});
    });

  });

  test('Update a spec -- remove field', function() {
    var store;
    return migrator.ensureStore('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL'
      },
    })
    .then(function() {
      return migrator.ensureStore('bigBiz.emp', {
        fields: {
          firstName: 'text',
          created: false
        }
      });
    })
    .then(function() {
      store = client.store('bigBiz.emp');
      return store.insert({
        dept: {id: '2'}
      });
    }).then(function(emp) {
      return store.getById(emp.id);
    })
    .then(function(result) {
      expect(result).to.not.have.property('created');
    });

  });

  test('Catch invalid spec', function() {
    return Promise.try(function() {
      return migrator.ensureStore('bigBiz.emp', {
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
    var store;
    return migrator.ensureStore('bigBiz.emp', {
      fields: {
        firstName: 'text',
        deptId: 'bigint'
      }
    })
    .then(function() {
      store = client.store('bigBiz.emp');
      return store.insert({
        creator: {id: 1},
        firstName: 'Joe'
      });
    })
    .then(function() {
      return migrator.ensureStore('bigBiz.emp', {
        fields: {
          deptId: 'bigint',
          firstName: 'text'
        },
      });
    })
    .then(function() {
      return client.query('SELECT * FROM big_biz.emp;');
    })
    .then(function(result) {
      expect(result[0]).to.have.property('firstName', 'Joe');
    });

  });

  test('Use flat references', function() {
    return migrator.ensureStore('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL',
        deptId: 'bigint'
      }
    })
    .then(function() {
      return client.store('bigBiz.emp').insert({
        firstName: 'Mel',
        deptId: '1',
      });
    })
    .then(function(result) {
      expect(result.dept).to.have.property('id', '1');
    });
  });

  test('Omit default fields', function() {
    return migrator.ensureStore('bigBiz.emp', {
      fields: {
        id: false,
        creatorId: false
      }
    })
    .then(function() {
      return client.store('bigBiz.emp').insert({});
    })
    .then(function(result) {
      expect(result).to.not.have.property('id');
      expect(result).to.not.have.property('creator');
    });
  });

  test('findOne()', function() {
    var store;
    return migrator.ensureStore('bigBiz.emp')
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
    return migrator.ensureStore('bigBiz.emp', {
      fields: {
        firstName: 'text',
        lastName: 'text',
        creatorId: false
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


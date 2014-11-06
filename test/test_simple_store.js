"use strict";
/*global suite: false, test: false, setup: false*/

//allow chai syntax like expect(foo).to.be.empty
/*jshint expr: true*/

var Promise = require('bluebird'),
    _ = require('lodash'),
    expect = require('chai').expect,
    assert = require('assert'),
    util = require('./util');

Promise.longStackTraces();

suite('Store', function() {

  var store;


  setup(function() {
    return (store
      ? store.close()
      : Promise.resolve())
    .then(function() {
      store = util.createStore({poolSize: 3});
      return store.dropNamespace('bigBiz')
      .catch(_.noop);
    });
  });

  test('Operations on a default entity', function() {
    return store.dropNamespace('foo')
    .then(function() {
      store.addSpec('foo.fooBar');
      return store.query('SELECT * FROM foo.foo_bar;')
        .then(function(results) {
          expect(results).to.be.empty;
        });
    })
    .then(function() {
      return store.insert('foo.fooBar', {});
    })
    .then(function() {
      return Promise.all([
        store.getById('foo.fooBar', '1'),
        store.queryRaw('SELECT __deleted, __bag FROM foo.foo_bar WHERE id = \'1\'')
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
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL',
        deptId: 'bigint'
      }
    });
    return store.insert('bigBiz.emp', {
      creator: {id: '3'},
      firstName: 'Mel',
    })
    .then(function(result) {
      expect(result).to.have.property('id', '1');
      expect(result).to.have.property('firstName', 'Mel');
      expect(result).to.not.have.property('dept');
    })
    .then(function() {
      return store.replace('bigBiz.emp',
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
      return store.update('bigBiz.emp',
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
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL'
      },
    });
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text',
        deptId: 'bigint'
      }
    });
    return store.insert('bigBiz.emp', {
      creator: {id: '3'},
      dept: {id: '2'}
    }).then(function(emp) {
      return store.getById('bigBiz.emp', emp.id);
    })
    .then(function(result) {
      expect(result).to.not.have.property('firstName');
      expect(result).to.have.property('dept');
      expect(result.dept).to.eql({id: '2'});
    });

  });

  test('Update a spec -- remove field', function() {
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL'
      },
    });
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text',
        creatorId: false
      }
    });
    return store.insert('bigBiz.emp', {
      dept: {id: '2'}
    }).then(function(emp) {
      return store.getById('bigBiz.emp', emp.id);
    })
    .then(function(result) {
      expect(result).to.not.have.property('creator');
    });

  });

  test('Catch invalid spec', function() {
    return Promise.try(function() {
      return store.addSpec('bigBiz.emp', {
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
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text',
        deptId: 'bigint'
      }
    });
    return store.insert('bigBiz.emp', {
      creator: {id: 1},
      firstName: 'Joe'
    })
    .then(function() {
      store.addSpec('bigBiz.emp', {
        fields: {
          deptId: 'bigint',
          firstName: 'text'
        },
      });
      return store.query('SELECT * FROM big_biz.emp;');
    })
    .then(function(result) {
      expect(result[0]).to.have.property('firstName', 'Joe');
    });

  });

  test('Create store via factory', function() {
    var storeFactory = util.createStoreFactory({poolSize: 3});
    var store = storeFactory('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL'
      }
    });
    return store.insert({
      creator: {id: '3'},
      firstName: 'Mel',
    })
    .then(function(result) {
      expect(result).to.have.property('firstName', 'Mel');
    });

  });

  test('Use flat references', function() {
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text NOT NULL',
        deptId: 'bigint'
      }
    });
    return store.insert('bigBiz.emp', {
      firstName: 'Mel',
      deptId: '1',
    })
    .then(function(result) {
      expect(result.dept).to.have.property('id', '1');
    });
  });

  test('Omit default fields', function() {
    store.addSpec('bigBiz.emp', {
      fields: {
        id: false,
        creatorId: false
      }
    });
    return store.insert('bigBiz.emp', {})
    .then(function(result) {
      expect(result).to.not.have.property('id');
      expect(result).to.not.have.property('creator');
    });
  });

  test('findOne()', function() {
    store.addSpec('bigBiz.emp', {});
    return store.insert('bigBiz.emp', {})
    .then(function() {
      return store.findOne('bigBiz.emp', {id: '1'});
    })
    .then(function(result) {
      expect(result).to.have.property('id', '1');
    });
  });

  test('Derived field', function() {
    store.addSpec('bigBiz.emp', {
      fields: {
        firstName: 'text',
        lastName: 'text',
        creatorId: false
      },
      derived: {
        name: function(record) {
          return record.firstName + ' ' + record.lastName;
        }
      }
    });
    return store.insert('bigBiz.emp', {
      // http://en.wikipedia.org/wiki/Mel_Blanc
      firstName: 'Mel',
      lastName: 'Blank',
      name: 'ignored'
    })
    .then(function(result) {
      expect(result).to.have.property('name', 'Mel Blank');
      return store.queryRaw('SELECT * FROM big_biz.emp WHERE id=$1', result.id)
      .then(_.first);
    })
    .then(function(result) {
      expect(result).to.have.property('first_name');
      expect(result.__bag).to.not.have.property('name');
    });
  });

});


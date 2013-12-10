"use strict";
/*global suite: false, test: false, setup: false*/
var when = require("when")
, delay = require("when/delay")
, _ = require("underscore")
, dbeasy = require("./index.js")
, assert = require("assert");

var pgconf = {};
_.each(['host', 'database', 'user', 'port', 'password'], function(key) {
    var envKey = 'POSTGRES_'+key.toUpperCase();
    if (!process.env[envKey]) throw new Error('missing configuration: '+envKey);
    pgconf[key] = process.env[envKey];
});

suite("db easy", function() {

    var db;

    var createTestTable = function(db) {
        return db.onConnection( function(conn) {
            conn.query("CREATE TABLE foo (bar int);");
        }).yield();
    };

    var dropTestTable = function(db) {
        return db.onConnection( function(conn) {
            return conn.query("DROP TABLE IF EXISTS foo;");
        }).yield();
    };

    var createDb = function(testOpts) {
        return dbeasy.create( _.extend(_.clone(pgconf), testOpts) );
    };

    setup(function(done) {
        if (db) {
            db.close();
            db = createDb({poolSize: 1});
            dropTestTable(db).then(function() {
                return createTestTable(db).then(function() {
                    db.close();
                    db = null;
                    done();
                });
            }).otherwise(done);
        } else {
            done();
        }
    });

    test("create a db", function(done) {
        db = createDb();
        db.query("select 1").yield()
        .then(function(){done();}).otherwise(console.log);

    });

    test("deadlock on nested connection requests", function(done) {
        db = createDb({poolSize: 1});
        var gotFirstConnection = false;
        var gotSecondConnection = false;

        var grab2Conns = function() {
            return db.onConnection( function() {
                gotFirstConnection = true;
                return db.onConnection( function() {
                    gotSecondConnection = false;
                    return;
                });
            });
        };

        grab2Conns().then( function() {
            done(new Error("Did not deadlock as expected"));
        }, done);

        // give it a little time before reporting success
        setTimeout(function() {
            assert.equal(gotFirstConnection, true);
            assert.equal(gotSecondConnection, false);
            done();
        }, 100);

    });

    test("don't deadlock on parallel connection requests", function(done) {
        db = createDb({poolSize: 1});
        var gotConnection = _.map(_.range(10), function() { return false; });

        var connecting = when.all(
            _.map(_.range(10), function(i) {
                return db.onConnection(function() {
                    gotConnection[i] = true;
                    return delay(20);
                });
            })
        ).yield();

        connecting.then( function() {
            assert(_.all(gotConnection), "Got all connections");
            done();
        }).otherwise(done);
    });

    test("don't deadlock on sequential connection requests", function(done) {
        db = createDb({poolSize: 1});
        var gotConnection = _.map(_.range(3), function() { return false; });

        var connecting = db.onConnection( function() {
            gotConnection[0] = true;
            return delay(20);
        }).then( function() {
            return db.onConnection( function() {
                gotConnection[1] = true;
                return delay(20);
            });
        }).then( function() {
            return db.onConnection( function() {
                gotConnection[2] = true;
                return delay(20);
            });
        });

        connecting.then( function() {
            assert(_.all(gotConnection), "Got all connections");
            done();
        }).otherwise(done);
    });

    test("rollback on application exception when in transaction", function(done) {
        db = createDb({poolSize: 1});
        var inserting = db.transaction( function(conn) {
            var querying = conn.query("INSERT INTO foo VALUES (DEFAULT);");
            var erroring = when.reject("error");
            return when.join(querying, erroring);
        });

        inserting.otherwise(function() {
            return db.query("SELECT count(*) from foo;").then( function(result) {
                assert.equal(result[0].count, '0');
                done();
            });
        }).otherwise(done);
    });

    test("rollback on db error when in transaction", function(done) {
        db = createDb({poolSize: 1});
        var inserting = db.transaction( function(conn) {
            var querying = conn.query("INSERT INTO foo VALUES (DEFAULT);");
            var erroring = conn.query("bogus");
            return when.join(querying, erroring);
        });

        inserting.otherwise(function() {
            return db.query("SELECT count(*) from foo;").then( function(result) {
                assert.equal(result[0].count, '0');
                done();
            });
        }).otherwise(done);
    });

    test("raw connection does not auto rollback", function(done) {
        db = createDb({poolSize: 1});
        var inserting = db.onConnection( function(conn) {
            var querying = conn.query("INSERT INTO foo VALUES (DEFAULT);");
            var erroring = when.reject("error");
            return when.join(querying, erroring);
        });

        inserting.otherwise(function() {
            return db.query("SELECT count(*) from foo;").then( function(result) {
                assert.equal(result[0].count, '1');
                done();
            });
        }).otherwise(done);
    });

});
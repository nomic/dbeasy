"use strict";
/*global suite: false, test: false, setup: false*/
var Promise = require("bluebird"),
    _ = require("lodash"),
    dbeasy = require("../index.js"),
    client = require("../client.js"),
    assert = require("assert"),
    createDb = require("./util").createDb;

suite("Cliet", function() {

    var db;

    var createTestTable = function(db) {
        return db.onConnection( function(conn) {
            return Promise.all([
                conn.query("CREATE TABLE foo (bar int);"),
                conn.query("CREATE TABLE fooid (id int, bar int);")
            ]);
        }).then(_.noop);
    };

    var dropTestTable = function(db) {
        return db.onConnection( function(conn) {
            return Promise.all([
                conn.query("DROP TABLE IF EXISTS foo;"),
                conn.query("DROP TABLE IF EXISTS fooid;")
            ]);
        }).then(_.noop);
    };


    setup(function(done) {
        if (db) {
            db.close();
        }
        db = createDb({poolSize: 1});
        dropTestTable(db).then(function() {
            return createTestTable(db).then(function() {
                db.close();
                db = null;
                done();
            });
        }).catch(done);
    });

    test("create a db", function(done) {
        db = createDb();
        db.query("select 1")
        .then(function(){done();})
        .catch(done);

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
        }, 1000);

    });

    test("don't deadlock on parallel connection requests", function(done) {
        db = createDb({poolSize: 1});
        var gotConnection = _.map(_.range(10), function() { return false; });

        Promise.all(
            _.map(_.range(10), function(i) {
                return db.onConnection(function() {
                    gotConnection[i] = true;
                    return Promise.delay(20);
                });
            })
        )
        .then( function() {
            assert(_.all(gotConnection), "Got all connections");
            done();
        })
        .catch(done);
    });

    test("don't deadlock on sequential connection requests", function(done) {
        db = createDb({poolSize: 1});
        var gotConnection = _.map(_.range(3), function() { return false; });

        var connecting = db.onConnection( function() {
            gotConnection[0] = true;
            return Promise.delay(20);
        }).then( function() {
            return db.onConnection( function() {
                gotConnection[1] = true;
                return Promise.delay(20);
            });
        }).then( function() {
            return db.onConnection( function() {
                gotConnection[2] = true;
                return Promise.delay(20);
            });
        });

        connecting.then( function() {
            assert(_.all(gotConnection), "Got all connections");
            done();
        }).catch(done);
    });

    test("rollback on application exception when in transaction", function(done) {
        db = createDb({poolSize: 1});
        var inserting = db.transaction( function(conn) {
            var querying = conn.query("INSERT INTO foo VALUES (DEFAULT);");
            var erroring = Promise.reject("error");
            return Promise.all([querying, erroring]);
        });

        inserting.catch(function() {
            return db.query("SELECT count(*) from foo;").then( function(result) {
                assert.equal(result[0].count, '0');
                done();
            });
        }).catch(done);
    });

    test("rollback on db error when in transaction", function(done) {
        db = createDb({poolSize: 1});
        var inserting = db.transaction( function(conn) {
            var querying = conn.query("INSERT INTO foo VALUES (DEFAULT);");
            var erroring = conn.query("bogus");
            return Promise.all([querying, erroring]);
        });

        inserting.catch(function() {
            return db.query("SELECT count(*) from foo;").then( function(result) {
                assert.equal(result[0].count, '0');
                done();
            });
        }).catch(done);
    });

    test("raw connection does not auto rollback", function(done) {
        db = createDb({poolSize: 1});
        var inserting = db.onConnection( function(conn) {
            var querying = conn.query("INSERT INTO foo VALUES (DEFAULT);");
            var erroring = Promise.reject("error");
            return Promise.all([querying, erroring]);
        });

        inserting.catch(function() {
            return db.query("SELECT count(*) from foo;").then( function(result) {
                assert.equal(result[0].count, '1');
                done();
            });
        }).catch(done);
    });

    test("prepare a statement", function(done) {
        db = createDb({loadpath: __dirname, poolSize:10});

        var preparing = db.prepare('test_query');

        var inserting = preparing.then(function() {
            return db.query('INSERT INTO foo VALUES (1), (2), (3), (4);');
        });

        var querying = inserting.then(function() {
            return db.exec('test_query', 2);
        });

        querying.then(function(result) {
            assert.equal(result.length, 2);
            done();
        }).catch(done);
    });

    test("prepare all statements in a directory", function(done) {
        db = createDb({loadpath: __dirname, poolSize:10});

        db.prepareDir(__dirname + '/test_sql')
        .then(function() { return db.exec('select_1'); })
        .then(function(rows) { assert.equal(rows[0].one, 1); })
        .then(function() { return db.execTemplate('select_2'); })
        .then(function(rows) { assert.equal(rows[0].two, 2); })
        .then(done, done);
    });

    test("parseNamedParams", function() {
        var text = (
            "--" +
            "-- $1: foo" +
            "-- $2: bar" +
            "--"
        );

        var actual = client.parseNamedParams(text);
        assert.equal(actual[0], 'foo');
        assert.equal(actual[1], 'bar');
    });

    test("prepare statemnt with named args", function(done) {
        db = createDb({loadpath: __dirname, poolSize:10});

        db.prepare('select_named_args')
        .then(function() {
            return db.exec('select_named_args', {
                foo: 'F',
                bar: 'B'
            });
        })
        .then(function(rows) {
            assert.equal(rows[0].foo, 'F');
            assert.equal(rows[0].bar, 'B');
        })
        .then(done, done);
    });

    test("prepare a statement from a template", function(done) {
        db = createDb({loadpath: __dirname, poolSize:10});

        var preparing = db.prepareTemplate('descending', 'test_query_template.sql.hbs', {direction: 'DESC'});

        var inserting = preparing.then(function() {
            return db.query('INSERT INTO foo VALUES (1), (2), (3), (4);');
        });

        var querying = inserting.then(function() {
            return db.exec('descending', 2);
        });

        querying.then(function(result) {
            assert.equal(result.length, 2);
            assert.equal(result[0].bar, 4);
            assert.equal(result[1].bar, 3);
            done();
        }).catch(done);
    });


    test("db.execTemplate", function(done) {
        db = createDb({loadpath: __dirname, poolSize:10});

        var preparing = db.prepare('test_query_template.sql.hbs');

        var inserting = preparing.then(function() {
            return db.query('INSERT INTO foo VALUES (1), (2), (3), (4);');
        });

        var querying = inserting.then(function() {
            return db.execTemplate('test_query_template', {direction: 'DESC'}, 2);
        });

        querying.then(function(result) {
            assert.equal(result.length, 2);
            assert.equal(result[0].bar, 4);
            assert.equal(result[1].bar, 3);
            done();
        }).catch(done);
    });

    test("db.exec: write then read consistency", function(done) {

        db = createDb({loadpath: __dirname, poolSize:10});
        var counter = 0;

        db.prepare('update_stmt', 'UPDATE fooid SET bar = bar+1 WHERE id = 0;');
        db.prepare('select_stmt', 'SELECT * from fooid;');
        return db.query('INSERT INTO fooid VALUES (0, 0);')
        .then(function() {
            function again(db) {
                if (counter === 10) return;

                return Promise.all([
                    db.exec('update_stmt'),
                    db.exec('update_stmt'),
                    db.exec('update_stmt'),
                    db.exec('update_stmt'),
                ])
                .then(function(){
                    return db.exec('select_stmt').then( function(result) {
                        assert.equal(result[0].bar, (counter+1)*4);
                        counter += 1;
                        return again(db);
                    });
                });
            }
            return again(db);
        }).catch(function(err) {console.log(err.cause); throw err;}).then(function() {done();}, done);

    });

});

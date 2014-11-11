'use strict';
var _ = require('lodash'),
    _str = require('underscore.string'),
    Promise = require('bluebird'),
    store = require('./store');

module.exports = function(client) {

    var migrator = {};
    var onReady = Promise.all([
        ensureSpecTableExists(),
        client.__prepareSql()
    ]);

    // A falsey can be passed in for a default field
    // so that it does not get added, or an alternate
    // definition will override it.
    function addDefaultFields(fields, defaults) {
        var result = _.extend({}, defaults, fields);

        var omit = [];
        _.each(result, function(def, name) {
            if (! result[name]) {
                omit.push(name);
            }
        });

        return _.omit(result, omit);
    }

    function specTableExists() {
        return client.query([
            'SELECT 1 FROM information_schema.tables',
            ' WHERE table_name=\'spec\'',
            '   AND table_schema=\'dbeasy_store\';'].join('\n'));
    }

    function getSpec(specName) {
        return client.query(
            'SELECT spec FROM dbeasy_store.spec WHERE name = $1',
            specName)
        .then(function(specRows) {
            return specRows[0] ? specRows[0].spec : null;
        });
    }

    function saveSpec(specName, spec) {
        return client.exec('__save_spec', {name: specName, spec: spec});
    }

    function ensureSpecTableExists() {
        return specTableExists()
        .then(function(results) {
            if (results.length) return;
            return client.synchronize('specTable', function() {
                return specTableExists()
                .then(function(results) {
                    if (results.length) return;
                    return client.exec('__create_spec_table');
                });
            });
        });
    }

    function fieldsToDDL(fields, isRefs) {
        return _.transform(fields, function(ddl, spec, name) {
            name = _str.underscored(name);
            if (isRefs) {
                name += '_id';
            }
            ddl.push([name, spec].join(' '));
            return ddl;
        }, []);
    }

    function doSpecUpdate(specName, spec) {
        var nameParts = specName.split('.');
        var namespace = (nameParts.length > 1) ? nameParts[0] : null,
            table = _str.underscored(specName);
        return client.synchronize(specName, function() {
            return getSpec(specName)
            .then(function(savedSpec) {
                // Even if we checked this before entering this
                // critical section, do it again to avoid wasted work
                // when several processes are coming up at the same time.
                if (_.isEqual(spec, savedSpec)) return;

                return ensureNamespace(namespace)
                .then(function() {
                    return client.transaction(function(conn) {
                        return Promise.all([
                            conn.execTemplate('__create_store_table', {
                                tableName: table,
                                columnDefinitions: fieldsToDDL(spec.fields),
                                refDefinitions: fieldsToDDL(spec.refs, true),
                                hasId: !!spec.fields.id
                            }),
                            saveSpec(specName, spec)
                        ]);
                    });
                });
            });
        });
    }

    function addNamespace(namespace) {
        var schema = _str.underscored(namespace);
        return client.query('SELECT * FROM information_schema.schemata WHERE schema_name = $1;', schema)
        .then(function(results) {
            return results.length || client.query('CREATE SCHEMA ' + schema);
        });
    }

    function ensureNamespace(namespace) {
        return addNamespace(namespace);
    }

    migrator.ensureStore = function(specName, spec) {
        spec = _.defaults(spec || {}, {
            fields: {}
        });

        var unknownKeys = _.keys(_.omit(spec, 'fields'));
        if (unknownKeys.length !== 0) {
            throw new Error('Malformed spec; unknown keys: ' + unknownKeys);
        }

        spec.fields = addDefaultFields(spec.fields, store.defaultFields);

        return onReady
        .then(function() {
            return getSpec(specName);
        })
        .then(function(savedSpec) {
            if (! _.isEqual(spec, savedSpec)) {
                return doSpecUpdate(specName, spec);
            }
        });

    };

    migrator.dropNamespace = function(namespace) {
        return onReady
        .then(function() {
            return Promise.all([
                // Drop the remembered specs
                client.query([
                    'DELETE FROM dbeasy_store.spec',
                    'WHERE name LIKE \'' + namespace + '.%\';'
                ].join('\n')),

                // Erase the data
                ensureNamespace(namespace)
                .then(function() {
                    return client.query([
                        'DROP SCHEMA ',
                        _str.underscored(namespace) + ' CASCADE;'
                    ].join('\n'));
                })
            ]);
        });
    };

    return migrator;

};


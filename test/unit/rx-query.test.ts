import assert from 'assert';
import AsyncTestUtil from 'async-test-util';
import config, { describeParallel } from './config.ts';
import clone from 'clone';

import {
    schemaObjects,
    schemas,
    humansCollection,
    isNode,
    HumanDocumentType,
} from '../../plugins/test-utils/index.mjs';

import {
    isRxQuery,
    createRxDatabase,
    RxJsonSchema,
    promiseWait,
    randomCouchString,
    ensureNotFalsy,
    deepFreeze,
    now, uncacheRxQuery, RxCollection,
} from '../../plugins/core/index.mjs';

import { firstValueFrom } from 'rxjs';

import {Cache, clearQueryCache} from '../helper/cache.ts';

const RESTORE_QUERY_MAX_TIME_AGO = 7 * 24 * 60 * 60 * 1000;

describe('rx-query.test.ts', () => {
    describeParallel('.constructor', () => {
        it('should throw dev-mode error on wrong query object', async () => {
            const col = await humansCollection.create(0);

            await AsyncTestUtil.assertThrows(
                () => col.find({ foo: 'bar' } as any),
                'RxTypeError',
                'no valid query params'
            );

            col.database.destroy();
        });
        it('should throw error when custom index not in schema indexes', async () => {
            const col = await humansCollection.create(0);
            await AsyncTestUtil.assertThrows(
                () => col.find({
                    selector: {},
                    index: ['f', 'o', 'b', 'a', 'r']
                }).getPreparedQuery(),
                'RxError',
                'not in schem'
            );
            col.database.destroy();
        });
        it('should NOT throw error when custom index is in schema indexes', async () => {
            const col = await humansCollection.createAgeIndex(0);
            col.find({
                selector: {},
                index: ['age']
            }).getPreparedQuery();
            col.database.destroy();
        });
    });
    describeParallel('.toJSON()', () => {
        it('should produce the correct selector-object', async () => {
            const col = await humansCollection.create(0);
            const q = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');
            const queryObj = q.mangoQuery;
            assert.deepStrictEqual(queryObj, {
                selector: {
                    name: {
                        '$ne': 'Alice'
                    },
                    age: {
                        '$gt': 18,
                        '$lt': 67
                    }
                },
                sort: [{
                    age: 'desc'
                }],
                limit: 10
            });
            col.database.destroy();
        });
    });
    describeParallel('.toString()', () => {
        it('should get a valid string-representation', async () => {
            const col = await humansCollection.create(0);
            const q = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');
            const str = q.toString();
            const mustString = '{"op":"find","other":{"queryBuilderPath":"age"},"query":{"limit":10,"selector":{"age":{"$gt":18,"$lt":67},"name":{"$ne":"Alice"}},"sort":[{"age":"desc"}]}}';
            assert.strictEqual(str, mustString);
            const str2 = q.toString();
            assert.strictEqual(str2, mustString);

            col.database.destroy();
        });
        it('should get a valid string-representation with two sort params', async () => {
            const col = await humansCollection.createAgeIndex();
            const q = col.find().sort({
                passportId: 'desc', age: 'desc'
            });
            const str = q.toString();
            const mustString = '{"op":"find","other":{},"query":{"selector":{},"sort":[{"passportId":"desc"},{"age":"desc"}]}}';
            assert.strictEqual(str, mustString);
            const str2 = q.toString();
            assert.strictEqual(str2, mustString);

            col.database.destroy();
        });
        it('ISSUE #190: should contain the regex', async () => {
            const col = await humansCollection.create(0);
            const queryWithoutRegex = col.find();
            const queryWithRegex = queryWithoutRegex.where('color').regex('foobar');
            const queryString = queryWithRegex.toString();

            assert.ok(queryString.includes('foobar'));
            col.database.destroy();
        });
        it('same queries should return the same string', async () => {
            const col1 = await humansCollection.create(0);
            const col2 = await humansCollection.create(0);

            const query1 = col1.find()
                .where('age').gt(10)
                .where('name').ne('foobar')
                .sort('passportId').toString();

            const query2 = col2.find()
                .where('age').gt(10)
                .where('name').ne('foobar')
                .sort('passportId').toString();

            assert.strictEqual(query1, query2);
            col1.database.destroy();
            col2.database.destroy();
        });
        it('same queries should return the same string even if on same collection', async () => {
            const col = await humansCollection.create(0);

            const query1 = col.find()
                .where('age').gt(10)
                .where('name').ne('foobar')
                .sort('passportId').toString();

            const query2 = col.find()
                .where('age').gt(10)
                .where('name').ne('foobar')
                .sort('passportId').toString();

            assert.strictEqual(query1, query2);
            col.database.destroy();
        });
    });
    describeParallel('immutable', () => {
        it('should not be the same object (sort)', async () => {
            const col = await humansCollection.create(0);
            const q = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');
            const q2 = q.sort('name');
            assert.ok(isRxQuery(q2));
            assert.notStrictEqual(q, q2);
            col.database.destroy();
        });
        it('should not be the same object (where)', async () => {
            const col = await humansCollection.create(0);
            const q = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');
            const q2 = q.where('name').eq('foobar');
            assert.ok(isRxQuery(q2));
            assert.notStrictEqual(q, q2);
            assert.ok(q.id < q2.id);
            col.database.destroy();
        });
    });
    describeParallel('QueryCache.js', () => {
        it('return the same object', async () => {
            const col = await humansCollection.create(0);
            const q = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');
            const q2 = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');

            assert.deepStrictEqual(q, q2);
            assert.strictEqual(q.id, q2.id);
            col.database.destroy();
        });
        it('should return the same object after exec', async () => {
            const col = await humansCollection.createPrimary(0);
            const docData = schemaObjects.simpleHumanData();
            await col.insert(docData);
            const query = col.findOne(docData.passportId);
            await query.exec();
            const query2 = col.findOne(docData.passportId);
            await query2.exec();
            assert.strictEqual(query.id, query2.id);
            col.database.destroy();
        });
        it('should have the correct amount of cached queries', async () => {
            const col = await humansCollection.create(0);
            const q3 = col.find()
                .where('name').ne('Bob');
            assert.ok(q3);
            const q = col.find()
                .where('name').ne('Alice');
            assert.ok(q);
            const q2 = col.find()
                .where('name').ne('Bob');
            assert.ok(q2);
            assert.strictEqual(col._queryCache._map.size, 4);
            col.database.destroy();
        });
        it('return another object', async () => {
            const col = await humansCollection.create(0);
            const q = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');
            const q2 = col.find()
                .where('name').ne('foobar')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age');

            assert.notStrictEqual(q, q2);
            assert.notStrictEqual(q.id, q2.id);
            col.database.destroy();
        });
        it('ISSUE: ensure its the same query', async () => {
            const col = await humansCollection.create(0);

            const query1 = col.find()
                .where('age').gt(10)
                .where('name').ne('foobar')
                .sort('passportId');

            const query2 = col.find()
                .where('age').gt(10)
                .where('name').ne('foobar')
                .sort('passportId');

            assert.ok(query1 === query2);
            col.database.destroy();
        });

        it('should distinguish between different sort-orders', async () => {
            const col = await humansCollection.create(0);
            const q = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('-age')
                .sort('name');
            const q2 = col.find()
                .where('name').ne('Alice')
                .where('age').gt(18).lt(67)
                .limit(10)
                .sort('name')
                .sort('-age');

            assert.notStrictEqual(q, q2);
            assert.notStrictEqual(q.id, q2.id);
            col.database.destroy();
        });
    });
    describeParallel('result caching', () => {
        /**
         * The object stored in the query cache should be
         * exact the same as the object used in a document data.
         * This ensures that we do not use double the memory
         * by storing the data multiple times.
         */
        it('should reuse the cached result object in the document', async () => {
            const col = await humansCollection.create(1);
            const query = col.find({
                selector: {
                    firstName: {
                        $ne: 'foobar'
                    }
                }
            });
            const docs = await query.exec();
            const doc = docs[0];
            if (!doc) {
                throw new Error('doc missing');
            }

            const docDataObject = doc._data;
            const inQueryCacheObject = ensureNotFalsy(query._result).docsData[0];

            assert.ok(
                docDataObject === inQueryCacheObject
            );

            col.database.destroy();
        });
    });
    describeParallel('.doesDocMatchQuery()', () => {
        it('should match', async () => {
            const col = await humansCollection.create(0);
            const q = col.find().where('firstName').ne('foobar');
            const docData = schemaObjects.humanData();
            assert.ok(q.doesDocumentDataMatch(docData));
            col.database.destroy();
        });
        it('should not match', async () => {
            const col = await humansCollection.create(0);
            const q = col.find().where('firstName').ne('foobar');
            const docData = schemaObjects.humanData();
            docData.firstName = 'foobar';
            assert.strictEqual(false, q.doesDocumentDataMatch(docData));
            col.database.destroy();
        });
        it('should match ($gt)', async () => {
            const col = await humansCollection.create(0);
            const q = col.find().where('age').gt(1);
            const docData = schemaObjects.humanData();
            docData.age = 5;
            assert.ok(q.doesDocumentDataMatch(docData));
            col.database.destroy();
        });
        it('should not match ($gt)', async () => {
            const col = await humansCollection.create(0);
            const q = col.find().where('age').gt(100);
            const docData = schemaObjects.humanData();
            docData.age = 5;
            assert.strictEqual(false, q.doesDocumentDataMatch(docData));
            col.database.destroy();
        });
        it('BUG: this should match', async () => {
            const col = await humansCollection.create(0);
            const q = col.find();

            const docData = {
                passportId: 'foobar',
                color: 'green',
                hp: 100,
                maxHP: 767,
                name: 'asdfsadf',
                _rev: '1-971bfd0b8749eb33b6aae7f6c0dc2cd4'
            };

            assert.strictEqual(true, q.doesDocumentDataMatch(docData));
            col.database.destroy();
        });
    });
    describeParallel('.exec()', () => {
        it('reusing exec should not make a execOverDatabase', async () => {
            const col = await humansCollection.create(2);
            const q = col.find().where('passportId').ne('Alice');


            let results = await q.exec();
            assert.strictEqual(results.length, 2);
            assert.strictEqual(q._execOverDatabaseCount, 1);

            await promiseWait(5);
            results = await q.exec();
            assert.strictEqual(results.length, 2);
            assert.strictEqual(q._execOverDatabaseCount, 1);

            col.database.destroy();
        });
        it('should execOverDatabase when still subscribed and changeEvent comes in', async () => {
            const col = await humansCollection.create(2);

            // it is assumed that this query can never handled by event-reduce
            const query = col.find().sort('-passportId').limit(1);

            const fired: any[] = [];
            const sub1 = query.$.subscribe(res => {
                fired.push(res);
            });

            await AsyncTestUtil.waitUntil(() => fired.length === 1, 1000);

            assert.strictEqual(query._execOverDatabaseCount, 1);
            assert.strictEqual(query._latestChangeEvent, 2);

            const addObj = schemaObjects.humanData();
            addObj.passportId = schemaObjects.TEST_DATA_CHARSET_LAST_SORTED.repeat(10);
            await col.insert(addObj);
            assert.strictEqual(query.collection._changeEventBuffer.getCounter(), 3);

            await AsyncTestUtil.waitUntil(() => query._latestChangeEvent === 3, 1000);
            assert.strictEqual(query._latestChangeEvent, 3);

            await AsyncTestUtil.waitUntil(() => fired.length === 2, 1000);
            assert.strictEqual(fired[1].pop().passportId, addObj.passportId);
            sub1.unsubscribe();
            col.database.destroy();
        });
        it('reusing exec should execOverDatabase when change happened that cannot be optimized', async () => {
            const col = await humansCollection.create(2);

            // it is assumed that this query can never handled by event-reduce
            const q = col.find()
                .where('firstName').ne('AliceFoobar')
                .sort('passportId')
                .skip(1);

            let results = await q.exec();
            assert.strictEqual(results.length, 1);
            assert.strictEqual(q._execOverDatabaseCount, 1);
            assert.strictEqual(q._latestChangeEvent, 2);

            const addDoc = schemaObjects.humanData();

            // set _id to first value to force a re-exec-over database
            addDoc.passportId = '1-aaaaaaaaaaaaaaaaaaaaaaaaaaa';
            addDoc.firstName = 'NotAliceFoobar';

            await col.insert(addDoc);
            assert.strictEqual(q.collection._changeEventBuffer.getCounter(), 3);

            assert.strictEqual(q._latestChangeEvent, 2);

            await promiseWait(1);
            results = await q.exec();
            assert.strictEqual(results.length, 2);
            assert.strictEqual(q._execOverDatabaseCount, 2);

            col.database.destroy();
        });
        it('querying fast should still return the same RxDocument', async () => {
            if (
                !isNode
            ) {
                return;
            }
            // use a 'slow' adapter because memory might be to fast
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
            });
            const cols = await db.addCollections({
                humans: {
                    schema: schemas.human
                }
            });
            const c = cols.humans;
            await c.insert(schemaObjects.humanData());

            const query1 = c.findOne().where('age').gt(0);
            const query2 = c.findOne().where('age').gt(1);
            const docs = await Promise.all([
                query1.exec(),
                query2.exec()
            ]);
            assert.ok(docs[0] === docs[1]);

            db.destroy();
        });
        it('querying after insert should always return the correct amount', async () => {
            const col = await humansCollection.create(0);

            const amount = 100;
            const query = col.find({
                selector: {
                    age: {
                        $gt: 1
                    }
                }
            });
            let inserted = 0;
            while (inserted < amount) {
                const docData = schemaObjects.humanData();
                docData.age = 10;
                await col.insert(docData);
                inserted = inserted + 1;
                const results = await query.exec();
                assert.strictEqual(results.length, inserted);
            }

            col.database.destroy();
        });
        it('should not make more requests then needed', async () => {
            const col = await humansCollection.createPrimary(0);
            const docData = schemaObjects.simpleHumanData();
            const otherData = () => {
                const data = clone(docData);
                data.firstName = AsyncTestUtil.randomString();
                return data;
            };
            await col.insert(docData);

            const emitted = [];
            const query = col.findOne(docData.passportId);
            query.$.subscribe((data: any) => emitted.push(data.toJSON()));

            await AsyncTestUtil.waitUntil(() => emitted.length === 1);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            const doc = await query.exec();
            assert.ok(doc);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            await col.upsert(otherData());
            await AsyncTestUtil.waitUntil(() => emitted.length === 2);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            await col.incrementalUpsert(otherData());
            await AsyncTestUtil.waitUntil(() => emitted.length === 3);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            await Promise.all(
                new Array(2)
                    .fill(0)
                    .map(() => otherData())
                    .map(data => col.incrementalUpsert(data))
            );
            await AsyncTestUtil.waitUntil(() => emitted.length === 5);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            await Promise.all(
                new Array(10)
                    .fill(0)
                    .map(() => otherData())
                    .map(data => col.incrementalUpsert(data))
            );
            await AsyncTestUtil.waitUntil(() => emitted.length === 15);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            col.database.destroy();
        });
        it('should not make more requests then needed on incremental upsert', async () => {
            const col = await humansCollection.createPrimary(0);
            const docData = schemaObjects.simpleHumanData();
            let count = 0;
            const otherData = () => {
                const data = clone(docData);
                data.firstName = '' + count;
                count++;
                return data;
            };

            const emitted = [];
            const query = col.findOne(docData.passportId);
            query.$.subscribe(doc => {
                if (!doc) emitted.push(null);
                else emitted.push(doc.toJSON());
            });

            await Promise.all(
                new Array(10)
                    .fill(0)
                    .map(() => otherData())
                    .map(data => col.incrementalUpsert(data))
            );

            assert.strictEqual(query._execOverDatabaseCount, 1);
            col.database.destroy();
        });
        it('exec from other database-instance', async () => {
            if (!config.storage.hasPersistence) {
                return;
            }
            const dbName = randomCouchString(10);
            const schema = schemas.averageSchema();
            const db = await createRxDatabase({
                name: dbName,
                eventReduce: true,
                storage: config.storage.getStorage(),
            });
            const cols = await db.addCollections({
                human: {
                    schema
                }
            });
            const col = cols.human;

            await Promise.all(
                new Array(10)
                    .fill(0)
                    .map(() => schemaObjects.averageSchemaData())
                    .map(data => col.insert(data))
            );

            await db.destroy();

            const db2 = await createRxDatabase({
                name: dbName,
                storage: config.storage.getStorage(),
                eventReduce: true,
                ignoreDuplicate: true
            });
            const cols2 = await db2.addCollections({
                human: {
                    schema
                }
            });
            const col2 = cols2.human;

            const allDocs = await col2.find().exec();
            assert.strictEqual(allDocs.length, 10);

            db2.destroy();
        });
        it('exec(true) should throw if missing', async () => {
            const c = await humansCollection.create(0);

            await AsyncTestUtil.assertThrows(
                () => c.findOne().exec(true),
                'RxError',
                'throwIfMissing'
            );

            c.database.destroy();
        });
        it('exec(true) should throw used with non-findOne', async () => {
            const c = await humansCollection.create(0);
            await AsyncTestUtil.assertThrows(
                () => c.find().exec(true),
                'RxError',
                'findOne'
            );
            c.database.destroy();
        });
        it('isFindOneByIdQuery(): .findOne(documentId) should use RxStorage().findDocumentsById() instead of RxStorage().query()', async () => {
            const c = await humansCollection.create();
            const docData = schemaObjects.humanData();
            const docId = 'foobar';
            docData.passportId = docId;
            await c.insert(docData);


            // overwrite .query() to track the amount of calls
            let queryCalls = 0;
            const queryBefore = c.storageInstance.query.bind(c.storageInstance);
            c.storageInstance.query = function (preparedQuery) {
                queryCalls = queryCalls + 1;
                return queryBefore(preparedQuery);
            };

            /**
             * None of these operations should lead to a call to .query()
             */
            const operations = [
                () => c.findOne(docId).exec(true),
                () => c.find({
                    selector: {
                        passportId: docId
                    },
                    limit: 1
                }).exec(),
                () => c.find({
                    selector: {
                        passportId: {
                            $eq: docId
                        }
                    },
                    limit: 1
                }).exec(),
                () => c.find({
                    selector: {
                        passportId: {
                            $eq: docId
                        }
                    }
                    /**
                     * Even without limit here,
                     * it should detect that we look for a document that is $eq
                     * to the primary key, so it can always
                     * only find one document.
                     */
                }).exec(),
                // same with id arrays
                () => c.find({
                    selector: {
                        passportId: {
                            $in: [
                                docId,
                                'foobar'
                            ]
                        }
                    },
                })
            ];
            for (const operation of operations) {
                await operation();
            }

            assert.strictEqual(queryCalls, 0);
            c.database.destroy();
        });
    });
    describeParallel('updates to the result of the query', () => {
        describe('RxQuery.update()', () => {
            it('updates a value on a query', async () => {
                const c = await humansCollection.create(2);
                const query = c.find();
                const updateResult = await query.update({
                    $set: {
                        firstName: 'new first name'
                    }
                });
                assert.strictEqual(updateResult.length, 2);

                // the returned docs must the at the "latest" revision
                for (const doc of updateResult) {
                    const latest = doc.getLatest();
                    assert.ok(latest === doc, 'doc must be latest');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                const docs = await query.exec();
                for (const doc of docs) {
                    assert.strictEqual(doc._data.firstName, 'new first name');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                c.database.destroy();
            });
            it('$unset a value on a query', async () => {
                const c = await humansCollection.create(2);
                const query = c.find();
                await query.update({
                    $unset: {
                        age: ''
                    }
                });
                const docs = await query.exec();
                for (const doc of docs)
                    assert.strictEqual(doc._data.age, undefined);
                c.database.destroy();
            });
            it('dont crash when findOne with no result', async () => {
                const c = await humansCollection.create(2);
                const query = c.findOne().where('age').gt(1000000);
                const updateResult = await query.update({
                    $set: {
                        firstName: 'new first name'
                    }
                });
                assert.strictEqual(updateResult, null);
                const doc = await query.exec();
                assert.strictEqual(doc, null);
                c.database.destroy();
            });
        });
        describe('RxQuery.patch()', () => {
            it('updates a value on a query', async () => {
                const c = await humansCollection.create(2);
                const query = c.find();
                const updateResult = await query.patch({
                    firstName: 'new first name'
                });
                // the returned docs must the at the "latest" revision
                for (const doc of updateResult) {
                    const latest = doc.getLatest();
                    assert.ok(latest === doc, 'doc must be latest');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                const docs = await query.exec();
                for (const doc of docs) {
                    assert.strictEqual(doc._data.firstName, 'new first name');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                c.database.destroy();
            });
            it('unset a value on a query by patching with undefined', async () => {
                const c = await humansCollection.create(2);
                const query = c.find();
                await query.patch({
                    age: undefined
                });
                const docs = await query.exec();
                for (const doc of docs) {
                    assert.strictEqual(doc._data.age, undefined);
                }
                c.database.destroy();
            });
            it('dont crash when findOne with no result', async () => {
                const c = await humansCollection.create(2);
                const query = c.findOne().where('age').gt(1000000);
                await query.patch({
                    firstName: 'new first name'
                });
                const doc = await query.exec();
                assert.strictEqual(doc, null);
                c.database.destroy();
            });
        });
        describe('RxQuery.modify()', () => {
            it('updates a value on a query', async () => {
                const c = await humansCollection.create(2);
                const query = c.find();
                const updateResult = await query.modify(docData => {
                    docData.firstName = 'new first name';
                    return docData;
                });
                // the returned docs must the at the "latest" revision
                for (const doc of updateResult) {
                    const latest = doc.getLatest();
                    assert.ok(latest === doc, 'doc must be latest');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                const docs = await query.exec();
                for (const doc of docs) {
                    assert.strictEqual(doc._data.firstName, 'new first name');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                c.database.destroy();
            });
        });
        describe('incremental functions', () => {
            it('.incrementalPatch()', async () => {
                const c = await humansCollection.create(2);
                const query = c.find();
                const updateResult = await query.incrementalPatch({
                    firstName: 'new first name'
                });
                // the returned docs must the at the "latest" revision
                for (const doc of updateResult) {
                    const latest = doc.getLatest();
                    assert.ok(latest === doc, 'doc must be latest');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                const docs = await query.exec();
                for (const doc of docs) {
                    assert.strictEqual(doc._data.firstName, 'new first name');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                c.database.destroy();
            });
            it('.incrementalModify()', async () => {
                const c = await humansCollection.create(2);
                const query = c.find();
                const updateResult = await query.incrementalModify(docData => {
                    docData.firstName = 'new first name';
                    return docData;
                });
                // the returned docs must the at the "latest" revision
                for (const doc of updateResult) {
                    const latest = doc.getLatest();
                    assert.ok(latest === doc, 'doc must be latest');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                const docs = await query.exec();
                for (const doc of docs) {
                    assert.strictEqual(doc._data.firstName, 'new first name');
                    assert.strictEqual(doc.isInstanceOfRxDocument, true);
                }
                c.database.destroy();
            });
        });
    });
    describeParallel('issues', () => {
        it('#278 queryCache breaks when pointer out of bounds', async () => {
            const c = await humansCollection.createPrimary(0);

            // insert some docs
            const insertAmount = 100;
            await c.bulkInsert(
                new Array(insertAmount)
                    .fill(0)
                    .map((_v, idx) => schemaObjects.humanData(undefined, idx))
            );

            // make and exec query
            const query = c.find();
            const docs = await query.exec();
            assert.strictEqual(docs.length, insertAmount);

            // produces changeEvents
            await c.bulkInsert(
                new Array(300) // higher than ChangeEventBuffer.limit
                    .fill(0)
                    .map(() => schemaObjects.humanData())
            );

            // re-exec query
            const docs2 = await query.exec();
            assert.strictEqual(docs2.length, 400);

            // try same with upserts
            const docData = new Array(200)
                .fill(0)
                .map(() => schemaObjects.humanData());
            await c.bulkInsert(docData);

            const docs3 = await query.exec();
            assert.strictEqual(docs3.length, 600);

            let docData2 = clone(docData);
            // because we have no bulkUpsert, we only upsert 10 docs to speed up the test.
            docData2 = docData2.slice(0, 10);
            docData2.forEach((doc: any) => doc.lastName = doc.lastName + '1');
            await Promise.all(
                docData2.map(doc => c.upsert(doc))
            );

            const docs4 = await query.exec();
            assert.strictEqual(docs4.length, 600);

            c.database.remove();
        });
        it('#585 sort by sub-path not working', async () => {
            if (['lokijs'].includes(config.storage.name)) {
                // TODO fix wrong sort order in lokijs
                return;
            }
            const schema = {
                version: 0,
                type: 'object',
                primaryKey: 'id',
                keyCompression: false,
                properties: {
                    id: {
                        type: 'string',
                        maxLength: 100
                    },
                    info: {
                        type: 'object',
                        properties: {
                            title: {
                                type: 'string',
                                maxLength: 100
                            },
                        },
                    }
                },
                indexes: ['info.title']
            };
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
            });
            const cols = await db.addCollections({
                humans: {
                    schema
                }
            });
            const col = cols.humans;

            await col.insert({
                id: '1',
                info: {
                    title: 'bbtest'
                }
            });
            await col.insert({
                id: '2',
                info: {
                    title: 'aatest'
                }
            });
            await col.insert({
                id: '3',
                info: {
                    title: 'cctest'
                }
            });

            const query = col
                .find()
                .sort('info.title');
            const foundDocs = await query.exec();
            assert.strictEqual(foundDocs.length, 3);
            assert.strictEqual(foundDocs[0].info.title, 'aatest');

            const foundDocsDesc = await col
                .find()
                .sort('-info.title')
                .exec();
            assert.strictEqual(foundDocsDesc.length, 3);
            assert.strictEqual(foundDocsDesc[0].info.title, 'cctest');

            db.remove();
        });
        it('#698 Same query producing a different result', async () => {
            const mySchema: RxJsonSchema<{ id: string; event_id: number; user_id: string; created_at: number; }> = {
                version: 0,
                keyCompression: false,
                primaryKey: 'id',
                type: 'object',
                properties: {
                    id: {
                        type: 'string',
                        maxLength: 100
                    },
                    event_id: {
                        type: 'number'
                    },
                    user_id: {
                        type: 'string'
                    },
                    created_at: {
                        type: 'number',
                        minimum: 0,
                        maximum: 10000000000000000,
                        multipleOf: 1
                    }
                },
                indexes: ['created_at']
            };
            const collection = await humansCollection.createBySchema(mySchema);

            await collection.insert({
                id: randomCouchString(12),
                event_id: 1,
                user_id: '6',
                created_at: 1337
            });
            await collection.insert({
                id: randomCouchString(12),
                event_id: 2,
                user_id: '6',
                created_at: 1337
            });


            const selector = {
                $and: [{
                    event_id: {
                        $eq: 2
                    }
                }, {
                    user_id: {
                        $eq: '6'
                    }
                },
                {
                    created_at: {
                        $gt: 0
                    }
                }, {
                    user_id: {
                        $eq: '6'
                    }
                },
                {
                    created_at: {
                        $gt: 0
                    }
                }
                ]
            };

            const resultDocs1 = await collection
                .find({
                    selector
                })
                .exec();
            const resultData1: any[] = resultDocs1.map(doc => doc.toJSON());

            const resultDocs2 = await collection
                .find()
                .where('event_id').eq(2)
                .where('user_id').eq('6')
                .where('created_at').gt(0)
                .exec();
            const resultData2 = resultDocs2.map(doc => doc.toJSON());

            assert.strictEqual(resultData1.length, 1);
            assert.strictEqual(resultData1[0]['event_id'], 2);
            assert.deepStrictEqual(resultData1, resultData2);

            collection.database.destroy();
        });
        it('698#issuecomment-402604237 mutating a returned array should not affect exec-calls afterwards', async () => {
            const c = await humansCollection.create(2);
            const query = c.find();

            // exec-calls
            const result1: any = await query.exec();
            assert.strictEqual(result1.length, 2);
            result1.push({
                foo: 'bar'
            });
            const result2 = await query.exec();
            assert.strictEqual(result2.length, 2);

            c.database.destroy();

            // subscriptions
            const c2 = await humansCollection.create(2);
            const query2 = c2.find();
            const res1: any = await firstValueFrom(query2.$);
            res1.push({
                foo: 'bar'
            });
            const res2 = await firstValueFrom(query2.$);
            assert.strictEqual(res2.length, 2);

            c2.database.destroy();
        });
        it('#815 Allow null value for strings', async () => {
            // create a schema
            const mySchema = {
                version: 0,
                primaryKey: 'passportId',
                type: 'object',
                properties: {
                    passportId: {
                        type: 'string',
                        maxLength: 100
                    },
                    firstName: {
                        type: 'string'
                    },
                    lastName: {
                        type: ['string', 'null']
                    },
                    age: {
                        type: 'integer',
                        minimum: 0,
                        maximum: 150
                    }
                }
            };

            // generate a random database-name
            const name = randomCouchString(10);

            // create a database
            const db = await createRxDatabase({
                name,
                storage: config.storage.getStorage(),
                eventReduce: true,
                ignoreDuplicate: true
            });
            // create a collection
            const collections = await db.addCollections({
                mycollection: {
                    schema: mySchema
                }
            });
            const collection = collections.mycollection;

            // insert a document
            await collection.insert({
                passportId: 'foobar',
                firstName: 'Bob1',
                age: 56
            });
            await collection.insert({
                passportId: 'foobaz',
                firstName: 'Bob2',
                lastName: null,
                age: 56
            });

            const queryOK = collection.find();
            const docsOK = await queryOK.exec();
            assert.strictEqual(docsOK.length, 2);

            db.destroy();
        });
        /**
         * via gitter at 11 November 2019 10:10
         */
        it('gitter: query with regex does not return correct results', async () => {
            // create a schema
            const mySchema = {
                version: 0,
                primaryKey: 'passportId',
                type: 'object',
                properties: {
                    passportId: {
                        type: 'string',
                        maxLength: 100
                    },
                    firstName: {
                        type: 'string'
                    },
                    lastName: {
                        type: ['string', 'null']
                    }
                }
            };
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
                eventReduce: true,
                ignoreDuplicate: true
            });

            // create a collection
            const collections = await db.addCollections({
                mycollection: {
                    schema: mySchema
                }
            });
            const collection = collections.mycollection;

            // insert documents
            await collection.bulkInsert([
                {
                    passportId: 'doc1',
                    firstName: 'John',
                    lastName: 'Doe'
                }, {
                    passportId: 'doc2',
                    firstName: 'Martin',
                    lastName: 'Smith'
                }
            ]);
            const allDocs = await collection.find().exec();
            assert.strictEqual(allDocs.length, 2);

            // test 1 with RegExp object
            const result1 = await collection.find({
                selector: {
                    lastName: {
                        $regex: '^Doe$',
                        $options: 'i'
                    }
                }
            }).exec();

            // test 2 with regex string
            const result2 = await collection.find({
                selector: {
                    lastName: { $regex: '^Doe$' }
                }
            }).exec();


            // both results should only have the doc1
            assert.strictEqual(result1.length, 1);
            assert.strictEqual(result1[0].passportId, 'doc1');
            assert.deepStrictEqual(
                result1.map(d => d.toJSON()),
                result2.map(d => d.toJSON())
            );

            db.remove();
        });
        it('#2071 RxCollection.findOne().exec() returns deleted document while find().exec() not', async () => {
            const c = await humansCollection.create(1);

            // delete it
            const doc = await c.findOne();
            await doc.remove();

            // now find() returns empty array
            const docs = await c.find().exec();
            assert.strictEqual(docs.length, 0);

            // findOne() still returns the deleted object
            const doc2 = await c.findOne().exec();
            assert.strictEqual(doc2, null);

            c.database.remove();
        });
        it('#2213 prepareQuery should handle all comparison operators', async () => {
            const collection = await humansCollection.createAgeIndex(0);
            await collection.insert({
                passportId: 'foobar',
                firstName: 'Bob',
                lastName: 'Kelso',
                age: 56
            });

            await collection.insert({
                passportId: 'foobar2',
                firstName: 'Bob2',
                lastName: 'Kelso2',
                age: 58
            });

            const myDocument = await collection.findOne({
                selector: {
                    age: {
                        $gte: 57,
                    },
                },
                sort: [{ age: 'asc' }]
            }).exec(true);

            assert.strictEqual(myDocument.age, 58);

            collection.database.remove();
        });
        it('should not mutate the query input', async () => {
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
                eventReduce: false
            });
            const schema = clone(schemas.human);
            schema.keyCompression = false;

            const cols = await db.addCollections({
                humans: {
                    schema
                }
            });
            const c = cols.humans;



            const docDataMatching = schemaObjects.humanData('docMatching');
            docDataMatching.age = 42;
            await c.insert(docDataMatching);

            const docDataNotMatching = schemaObjects.humanData('docNotMatching');
            docDataNotMatching.age = 99;
            await c.insert(docDataNotMatching);

            /**
             * Deep freeze the params so that it will throw
             * at the first place it is mutated.
             */
            const queryParams = deepFreeze({
                selector: {
                    age: 42
                }
            });
            const queryMatching = c.find(queryParams);
            const queryMatchingOne = c.findOne(queryParams);
            if (queryMatching.mangoQuery.limit) {
                throw new Error('queryMatching must not have a limit ' + JSON.stringify(queryMatching.mangoQuery));
            }
            const res1 = await queryMatching.exec();
            const resOne1 = await queryMatchingOne.exec();
            assert.strictEqual(res1.length, 1);
            assert.ok(resOne1);
            assert.strictEqual(resOne1.age, 42);
            db.destroy();
        });
        /**
        * via gitter @sfordjasiri 27.8.2020 10:27
        */

        it('gitter: mutating find-params causes different results', async () => {
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
                eventReduce: false
            });
            const schema = clone(schemas.human);
            schema.keyCompression = false;

            const cols = await db.addCollections({
                humans: {
                    schema
                }
            });
            const c = cols.humans;



            const docDataMatching = schemaObjects.humanData('docMatching');
            docDataMatching.age = 42;
            await c.insert(docDataMatching);

            const docDataNotMatching = schemaObjects.humanData('docNotMatching');
            docDataNotMatching.age = 99;
            await c.insert(docDataNotMatching);


            const queryParams = {
                selector: {
                    age: 42
                }
            };
            const queryMatching = c.find(queryParams);
            const queryMatchingOne = c.findOne(queryParams);
            if (queryMatching.mangoQuery.limit) {
                throw new Error('queryMatching must not have a limit ' + JSON.stringify(queryMatching.mangoQuery));
            }
            const res1 = await queryMatching.exec();
            const resOne1 = await queryMatchingOne.exec();
            assert.strictEqual(res1.length, 1);
            assert.ok(resOne1);
            assert.strictEqual(resOne1.age, 42);

            queryParams.selector.age = 0;

            // trigger a write so the results are not cached
            const addData = schemaObjects.humanData('a-trigger-write');
            addData.age = 55;
            await c.insert(addData);

            const res2 = await queryMatching.exec();
            const resOne2 = await queryMatchingOne.exec();

            assert.strictEqual(res2.length, 1);
            assert.ok(res2);
            assert.strictEqual(resOne2.age, 42);

            db.destroy();
        });

        it('#3498 RxQuery returns outdated result in second subscription', async () => {
            const schema = {
                version: 0,
                primaryKey: 'id',
                type: 'object',
                properties: {
                    id: {
                        type: 'string',
                        maxLength: 100
                    },
                    field: {
                        type: 'boolean'
                    }
                }
            } as const;
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
                eventReduce: true,
                ignoreDuplicate: true
            });
            const collection = (await db.addCollections({
                collection: {
                    schema
                }
            })).collection;

            const doc = await collection.insert({ id: 'testid', field: false });

            // Bug only happens the second time the query is used
            const result1 = await collection.find({ selector: { field: false } }).exec();
            assert.strictEqual(result1.length, 1);

            await doc.update({
                $set: {
                    field: true
                }
            });

            const obs = collection.find({ selector: { field: false } }).$;
            const result2a: any[][] = [];
            const result2b: any[][] = [];
            const sub2 = obs.subscribe((d) => result2b.push(d));
            const sub1 = obs.subscribe((d) => result2a.push(d));

            await promiseWait(5);

            sub1.unsubscribe();
            sub2.unsubscribe();

            assert.strictEqual(Math.max(...result2a.map(r => r.length)), 0);
            assert.strictEqual(Math.max(...result2b.map(r => r.length)), 0);

            db.destroy();
        });
        it('#3631 Sorting a query adds in deleted documents', async () => {
            const c = await humansCollection.createAgeIndex(1);
            const doc = await c.findOne().exec(true);
            await doc.remove();

            const queryResult = await c.find({
                selector: {},
                sort: [
                    { age: 'asc' }
                ]
            }).exec();

            // should not have found the deleted document
            assert.strictEqual(queryResult.length, 0);

            c.database.destroy();
        });
        it('#4552 $elemMatch query not working when there are many documents in the collection', async () => {
            const c = await humansCollection.createNested(100);
            const result = await c.find({
                selector: {
                    mainSkill: {
                        $elemMatch: {
                            name: {
                                $eq: 'foobar'
                            }
                        }
                    }
                }
            }).exec();
            assert.strictEqual(result.length, 0);
            c.database.remove();
        });
        it('#4586 query-builder copies other param', async () => {
            const col = await humansCollection.create(0);
            const q = col.find();
            const key = 'some-plugin-key';
            const data = 'some-plugin-data';
            q.other[key] = data;

            const newQ = q.where('name').ne('Alice');

            assert.strictEqual(newQ.other[key], data);

            col.database.destroy();
        });
        it('#4773 should not return deleted documents when queried by a primary key', async () => {
            const c = await humansCollection.create();
            const docData = schemaObjects.humanData();
            await c.insert(docData);
            const doc = await c.findOne(docData.passportId).exec();
            assert.ok(doc);
            await c.findOne(docData.passportId).remove();
            const doc2 = await c.findOne(docData.passportId).exec();
            assert.strictEqual(doc2, null);
            const doc3 = await c.findOne({ selector: { passportId: { $eq: [docData.passportId] } } }).exec();
            assert.strictEqual(doc3, null);
            const docs = await c.find({ selector: { passportId: docData.passportId } }).exec();
            assert.strictEqual(docs.length, 0);
            c.database.destroy();
        });
        it('primaryKey with value "constructor", breaks .findOne()', async () => {
            const mySchema = {
                version: 0,
                primaryKey: 'passportId',
                type: 'object',
                properties: {
                    passportId: {
                        type: 'string',
                        maxLength: 100
                    }
                }
            };
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
                eventReduce: true,
                ignoreDuplicate: true
            });

            // create a collection
            const collections = await db.addCollections({
                mycollection: {
                    schema: mySchema
                }
            });
            const collection = collections.mycollection;

            let has = await collection.findOne('constructor').exec();
            assert.ok(!has);
            has = await collection.findOne('toString').exec();
            assert.ok(!has);

            const byId = await collection.findByIds(['constructor']).exec();
            assert.ok(!byId.has('constructor'));

            db.destroy();
        });
    });

    async function setUpLimitBufferCollectionAndQuery(enableLimitBufferSize?: number, numRowsTotal=20, skipRows?: number) {
        const limitRows = 10;
        const collection = await humansCollection.create(numRowsTotal);

        // Setup a query where the limit buffer would be useful.
        // This .find initially matches all docs in the collection
        let query = collection.find({selector: {
            firstName: {
                $ne: 'Dollaritas'
            }
        }}).sort('-lastName').limit(limitRows);

        if (skipRows !== undefined) {
            query = query.skip(skipRows);
        }

        if (enableLimitBufferSize !== undefined) {
            query.enableLimitBuffer(enableLimitBufferSize);
        }

        const initialResults = await query.exec();

        assert.strictEqual(initialResults.length, Math.min(limitRows, numRowsTotal));
        assert.strictEqual(query._execOverDatabaseCount, 1);

        // We already have a change event for each row from humansCollection.create:
        assert.strictEqual(query._latestChangeEvent, numRowsTotal);

        return {query, collection, numRowsTotal, limitRows, initialResults};
    }

    async function removeSingleDocFromMatchingQuery(collection: Awaited<ReturnType<typeof setUpLimitBufferCollectionAndQuery>>['collection'], doc: HumanDocumentType) {
        await collection.find({selector: {passportId: doc.passportId}}).update({
            $set: {
                firstName: 'Dollaritas'
            }
        });
    }

    describeParallel('Limit Buffer', () => {
        it('By default, limit queries will have to re-exec when item is removed', async () => {
            // Set up the query, without using the limit buffer:
            const { query, collection, numRowsTotal, limitRows, initialResults } = await setUpLimitBufferCollectionAndQuery(undefined);

            // Now, make a change that removes a single doc from the result set
            await removeSingleDocFromMatchingQuery(collection, initialResults[0]);

            // Re-exec the query:
            const updatedResults = await query.exec();
            // Confirm the change was processed, and the results are correct:
            assert.strictEqual(updatedResults.length, limitRows);
            assert.notStrictEqual(updatedResults[0].passportId, initialResults[0].passportId);
            assert.strictEqual(query.collection._changeEventBuffer.counter, numRowsTotal + 1);
            assert.strictEqual(query._latestChangeEvent, numRowsTotal + 1);

            // Confirm that the query had to run via db again instead of using the query cache:
            assert.strictEqual(query._execOverDatabaseCount, 2);

            collection.database.destroy();
        });
        it('Limit buffer works properly in usual cases', async () => {
            const limitBufferSize = 5;
            const {query, collection, numRowsTotal, limitRows, initialResults} = await setUpLimitBufferCollectionAndQuery(limitBufferSize, 30);

            // Now, make a change that removes a single doc from the result set
            await removeSingleDocFromMatchingQuery(collection, initialResults[0]);

            // Re-exec the query:
            const updatedResults = await query.exec();
            // Confirm the change was processed, and the results are correct:
            assert.strictEqual(updatedResults.length, limitRows);
            assert.notStrictEqual(updatedResults[0].passportId, initialResults[0].passportId);
            assert.strictEqual(query.collection._changeEventBuffer.counter, numRowsTotal + 1);
            assert.strictEqual(query._latestChangeEvent, numRowsTotal + 1);

            // Confirm that the query DID NOT exec over the db again, because it used the query cache via limit buffer:
            assert.strictEqual(query._execOverDatabaseCount, 1);
            // And that one item was taken from the limit buffer:
            assert.strictEqual(query._limitBufferResults?.length, limitBufferSize - 1);

            // Do it all again to make sure this is consistent across multiple updates:
            await removeSingleDocFromMatchingQuery(collection, initialResults[8]);
            const updatedResultsAgain = await query.exec();
            assert.strictEqual(updatedResultsAgain.length, limitRows);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            // However, if we "use up" the whole limit buffer (5 documents),
            // the query will have to re-exec. Let's remove 3 more items to show that:
            for (const doc of initialResults.slice(1, 4)) {
                await removeSingleDocFromMatchingQuery(collection, doc);
                await query.exec();
                assert.strictEqual(query._execOverDatabaseCount, 1);
            }

            // The Limit buffer should now be empty:
            assert.strictEqual(query._limitBufferResults?.length, 0);

            // So removing one more item will require a re-exec on the db:
            await removeSingleDocFromMatchingQuery(collection, initialResults[4]);
            await query.exec();
            assert.strictEqual(query._execOverDatabaseCount, 2);

            // After this re-exec on the db, the limit buffer should be filled again:
            assert.strictEqual(query._limitBufferResults?.length, limitBufferSize);

            // And further removals will use the new limit buffer again:
            await removeSingleDocFromMatchingQuery(collection, initialResults[5]);
            const finalResults = await query.exec();
            assert.strictEqual(finalResults.length, limitRows);
            assert.strictEqual(query._execOverDatabaseCount, 2);
            assert.strictEqual(query._limitBufferResults?.length, limitBufferSize - 1);

            collection.database.destroy();
        });
        it('Limit buffer doesn\'t do anything when fewer than LIMIT items', async () => {
            // Set up with only 8 rows total, but a limit of 10 (and limit buffer 5):
            const limitBufferSize = 5;
            const {query, collection, numRowsTotal, initialResults} = await setUpLimitBufferCollectionAndQuery(limitBufferSize, 8);

            // Now, make a change that removes a single doc from the result set
            await removeSingleDocFromMatchingQuery(collection, initialResults[0]);

            // Re-exec the query after removing one, so the results should be 7 docs now:
            const updatedResults = await query.exec();
            // Confirm the change was processed, and the results are correct:
            assert.strictEqual(updatedResults.length, numRowsTotal - 1);
            assert.notStrictEqual(updatedResults[0].passportId, initialResults[0].passportId);

            // And the limitBuffer wasn't filled at all:
            assert.strictEqual(query._limitBufferResults, null);

            // The query wouldn't have to re-exec because of the normal query cache:
            assert.strictEqual(query._execOverDatabaseCount, 1);

            collection.database.destroy();
        });
        it('Limit buffer works with skip=0', async () => {
            // Set up with a skip=0 (limit buffer should work normally)
            const limitBufferSize = 5;
            const {query, collection, initialResults} = await setUpLimitBufferCollectionAndQuery(limitBufferSize, 20, 0);
            assert.strictEqual(query._limitBufferResults?.length, limitBufferSize);
            await removeSingleDocFromMatchingQuery(collection, initialResults[1]);
            await query.exec();
            assert.strictEqual(query._execOverDatabaseCount, 1);
            collection.database.destroy();
        });
        it('Limit buffer does nothing with a non-zero skip', async () => {
            const limitBufferSize = 5;
            const {query, collection, initialResults} = await setUpLimitBufferCollectionAndQuery(limitBufferSize, 20, 10);
            assert.strictEqual(query._limitBufferResults, null);
            await removeSingleDocFromMatchingQuery(collection, initialResults[1]);
            await query.exec();
            assert.strictEqual(query._execOverDatabaseCount, 2);
            collection.database.destroy();
        });
        it('Limit buffer does nothing if item is removed from results due to sort changing only', async () => {
            // Do a normal setup with the limit, and confirm the limit buffer gets filled:
            const limitBufferSize = 5;
            const {query, collection, initialResults} = await setUpLimitBufferCollectionAndQuery(limitBufferSize, 20);
            assert.strictEqual(query._limitBufferResults?.length, limitBufferSize);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            // Instead of removing an item from the results by making it break the query selector
            // (what removeSingleDocFromMatchingQuery does) just move it to the end of the sort
            // which will kick it out of the query results due to the LIMIT
            await collection.find({selector: {passportId: initialResults[0].passportId}}).update({
                $set: {
                    lastName: 'AAAAAAAAAAAAAAA'
                }
            });

            // Explicitly, the limit buffer does not replace items in this case (although it technically
            // could with little trouble in the future, we just haven't implemented it)
            // so the query should re-run on the database to fill in the missing document:
            const updatedResults = await query.exec();
            assert.strictEqual(query._execOverDatabaseCount, 2);
            assert.notStrictEqual(updatedResults[0].passportId, initialResults[0].passportId);
            collection.database.destroy();
        });
        it('Limit buffer omits buffered items that have been modified to no longer', async () => {
            const limitBufferSize = 5;
            const {query, collection, initialResults} = await setUpLimitBufferCollectionAndQuery(limitBufferSize, 20);

            if (query._limitBufferResults === null) {
                throw new Error('_limitBufferResults not set');
            }
            // Get the first item from the limit buffer, and change it so it no longer matches the query selector:
            const firstBufferItem = query._limitBufferResults[0];
            await collection.find({selector: {passportId: firstBufferItem.passportId}}).update({
                $set: {
                    firstName: 'Dollaritas'
                }
            });
            // Now, remove an item from the initial results, so that the buffer _should_ be used
            // to fill the last item in the updated results.
            await removeSingleDocFromMatchingQuery(collection, initialResults[1]);

            // Make sure we DO NOT pull the modified item from the limit buffer, as it no longer matches query:
            const updatedResults = await query.exec();
            assert.notStrictEqual(updatedResults[updatedResults.length - 1].passportId, firstBufferItem.passportId);

            collection.database.destroy();
        });
    });

    async function setUpPersistentQueryCacheCollection() {
        const collection = await humansCollection.create(0);
        return {collection};
    }

    describeParallel('Persistent Query Cache', () => {
        it('query fills cache', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const query = collection.find({ limit: 1 });
            const cache = new Cache();
            query.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            const human1 = schemaObjects.humanData();
            const human2 = schemaObjects.humanData();

            await collection.bulkInsert([human1, human2]);
            await query.exec();

            assert.strictEqual(cache.size, 2);

            collection.database.destroy();
        });

        it('does not query from database after restoring from persistent query cache', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData();
            const human2 = schemaObjects.humanData();

            await collection.bulkInsert([human1, human2]);

            const query = collection.find({ limit: 2 });

            // fill cache
            const queryId = query.persistentQueryId();
            const cache = new Cache();
            await cache.setItem(`qc:${queryId}`, [human1.passportId, human2.passportId]);
            await cache.setItem(`qc:${queryId}:lwt`, `${now()}`);
            query.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            // execute query
            const result = await query.exec();

            assert.strictEqual(result.length, 2);
            assert.strictEqual(query._execOverDatabaseCount, 0);

            collection.database.destroy();
        });

        it('does not query from database after modifying a document', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData();
            const human1Age = human1.age;

            await collection.bulkInsert([human1]);

            const query1 = collection.find({ selector: { age: human1Age }});

            // fill cache
            const queryId = query1.persistentQueryId();
            const cache = new Cache();
            await cache.setItem(`qc:${queryId}`, [human1.passportId]);
            await cache.setItem(`qc:${queryId}:lwt`, `${now()}`);
            query1.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            // execute query
            const result1 = await query1.exec();
            assert.strictEqual(result1.length, 1);

            const human1Doc = result1[0];
            await human1Doc.modify(data => {
              data.age += 1;
              return data;
            });

            clearQueryCache(collection);

            const query2 = collection.find({ selector: { age: human1Age }});
            query2.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            const result2 = await query2.exec();

            assert.strictEqual(result1.length, 1);
            assert.strictEqual(result2.length, 0);
            assert.strictEqual(query1._execOverDatabaseCount, 0);
            assert.strictEqual(query2._execOverDatabaseCount, 0);

            collection.database.destroy();
        });

        it('does not query from database after adding an object', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData();
            const human2 = schemaObjects.humanData();
            const human3 = schemaObjects.humanData();

            await collection.bulkInsert([human1, human2]);

            const query = collection.find({ limit: 3 });
            const queryId = query.persistentQueryId();
            const cache = new Cache();
            await cache.setItem(`qc:${queryId}`, [human1.passportId, human2.passportId]);
            await cache.setItem(`qc:${queryId}:lwt`, `${now()}`);
            query.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            const result1 = await query.exec();

            await collection.insert(human3);

            const result2 = await query.exec();

            assert.strictEqual(result1.length, 2);
            assert.strictEqual(result2.length, 3);
            assert.strictEqual(query._execOverDatabaseCount, 0);

            collection.database.destroy();
        });

        it('does return docs from cache in correct order and with limits applied', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData('1', 30);
            const human2 = schemaObjects.humanData('2', 40);
            const human3 = schemaObjects.humanData('3', 50);

            await collection.bulkInsert([human2, human3]);

            const query1 = collection.find({ limit: 2, sort: [{age: 'asc'}] });
            const queryId = query1.persistentQueryId();
            const lwt = now();

            const cache = new Cache();
            await cache.setItem(`qc:${queryId}`, [human2.passportId, human3.passportId]);
            await cache.setItem(`qc:${queryId}:lwt`, `${lwt}`);

            await collection.insert(human1);

            clearQueryCache(collection);

            const query2 = collection.find({ limit: 2, sort: [{age: 'asc'}] });
            query2.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            const result2 = await query2.exec();

            assert.strictEqual(query1._execOverDatabaseCount, 0);
            assert.strictEqual(query2._execOverDatabaseCount, 0);
            assert.deepStrictEqual(result2.map(item => item.passportId), ['1', '2']);

            collection.database.destroy();
        });

        it('removing an item from the database, but not from cache does not lead to wrong results after restoring', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData('1', 30);
            const human2 = schemaObjects.humanData('2', 40);
            const human3 = schemaObjects.humanData('3', 50);

            await collection.bulkInsert([human1, human2, human3]);

            const query1 = collection.find({ limit: 2, sort: [{age: 'asc'}] });
            const queryId = query1.persistentQueryId();
            const lwt = now();

            const cache = new Cache();
            await cache.setItem(`qc:${queryId}`, [human1.passportId, human2.passportId, human3.passportId]);
            await cache.setItem(`qc:${queryId}:lwt`, `${lwt}`);

            const removeQuery = collection.find({ selector: { passportId: '2' }});
            await removeQuery.remove();

            clearQueryCache(collection);

            const query2 = collection.find({ limit: 2, sort: [{age: 'asc'}] });
            query2.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            assert.strictEqual(cache.getItem(`qc:${queryId}`).length, 3);

            const result2 = await query2.exec();

            assert.strictEqual(query1._execOverDatabaseCount, 0);
            assert.strictEqual(query2._execOverDatabaseCount, 0);
            assert.deepStrictEqual(result2.map(item => item.passportId), ['1', '3']);

            collection.database.destroy();
        });

        it('old cache values are updated when documents are modified', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData('1', 30);

            await collection.bulkInsert([human1]);

            // fill cache
            const cache = new Cache();
            const query1 = collection.find({limit: 1});
            query1.enableLimitBuffer(5).enablePersistentQueryCache(cache);
            const queryId = query1.persistentQueryId();

            const result1 = await query1.exec();
            assert.strictEqual(result1.length, 1);
            assert.strictEqual(cache.size, 2);

            clearQueryCache(collection);

            // go back in time
            const lwt = now() - 7200 * 1000; // go back in time (2hrs)
            await cache.setItem(`qc:${queryId}:lwt`, `${lwt}`);

            const query2 = collection.find({limit: 1});
            query2.enableLimitBuffer(5).enablePersistentQueryCache(cache);
            await query2._persistentQueryCacheLoaded;

            await result1[0].remove();

            await query2.exec();

            const currLwt = Number(await cache.getItem(`qc:${queryId}:lwt`));
            assert.strictEqual(currLwt > lwt, true);

            collection.database.destroy();
        });

        it('query from database when cache is empty', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData();
            await collection.bulkInsert([human1]);

            const query = collection.find({ limit: 3 });

            const cache = new Cache();
            query.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            const result = await query.exec();

            assert.strictEqual(result.length, 1);
            assert.strictEqual(query._execOverDatabaseCount, 1);

            collection.database.destroy();
        });

        it('will re-execute queries if they were cached a long time ago', async () => {
            const {collection} = await setUpPersistentQueryCacheCollection();

            const human1 = schemaObjects.humanData('1', 30);
            await collection.bulkInsert([human1]);

            // fill cache
            const cache = new Cache();
            const query1 = collection.find({limit: 1});
            query1.enableLimitBuffer(5).enablePersistentQueryCache(cache);
            const queryId = query1.persistentQueryId();

            await query1.exec();
            clearQueryCache(collection);

            // If we restore the same query, it shouldn't need to re-exec:
            const querySoon = collection.find({limit: 1});
            querySoon.enableLimitBuffer(5).enablePersistentQueryCache(cache);
            await querySoon.exec();
            assert.strictEqual(querySoon._execOverDatabaseCount, 0);

            clearQueryCache(collection);

            // Now, simulate the query having been cached over a week ago.
            // It should have to re-exec.
            const lwt = now() - RESTORE_QUERY_MAX_TIME_AGO - 1000;
            await cache.setItem(`qc:${queryId}:lwt`, `${lwt}`);

            const queryLater = collection.find({limit: 1});
            queryLater.enableLimitBuffer(5).enablePersistentQueryCache(cache);

            await queryLater.exec();
            assert.strictEqual(queryLater._execOverDatabaseCount, 1);

            collection.database.destroy();
        });

        describe('persisting queries with limit buffers', () => {
            async function setUpLimitBufferSituation() {
                const {collection} = await setUpPersistentQueryCacheCollection();
                await collection.bulkInsert([
                    schemaObjects.humanData('1', 30),
                    schemaObjects.humanData('2', 40),
                    schemaObjects.humanData('3', 50),
                    schemaObjects.humanData('4', 60),
                    schemaObjects.humanData('5', 70),
                ]);

                // wait 1 second so that not all docs are included in lwt
                await new Promise((resolve) => {
                    setTimeout(resolve, 500);
                });

                // Cache a limited query:
                const query = collection.find({ limit: 2, sort: [{age: 'asc'}], selector: { age: { $gt: 10 } } });
                const cache = new Cache();

                return { query, cache, collection };
            }

            function simulateNewSession(collection: RxCollection) {
                clearQueryCache(collection);
                collection._docCache.cacheItemByDocId.clear();
            }

            // This is how it should operate when we don't persist limit buffers:
            it('limit buffer not enabled, still gives correct results through re-execution', async () => {
                const { collection, query, cache} = await setUpLimitBufferSituation();

                // persist with no limit buffer enabled
                await query.enablePersistentQueryCache(cache);
                const originalResults = await query.exec();
                assert.deepStrictEqual(originalResults.map(h => h.passportId), ['1', '2']);

                // Now, get into a state where that query is no longer in memory (eg new tab)
                // (but, the query should still be persisted on disk)
                simulateNewSession(collection);
                assert.strictEqual(cache.size, 2);

                // while the query is not in memory, remove one of the items from the query results
                await collection.find({selector: { passportId: '1'}}).update({
                    $set: { age: 1 }
                });

                // now when we create the query again, it has no way of knowing how to fill the missing item
                const queryAgain = collection.find(query.mangoQuery);
                assert.strictEqual(queryAgain._execOverDatabaseCount, 0);

                await queryAgain.enablePersistentQueryCache(cache);
                const updatedResults = await queryAgain.exec();

                // We must re-exec the query to make it correct.
                assert.strictEqual(queryAgain._execOverDatabaseCount, 1);
                assert.deepStrictEqual(updatedResults.map(h => h.passportId), ['2', '3']);
                collection.database.destroy();
            });

            it('limit buffer enabled, restores normal changes, results correctly with no re-exec', async () => {
                const { collection, query, cache} = await setUpLimitBufferSituation();

                // Persist WITH the limit buffer enabled
                query.enableLimitBuffer(5).enablePersistentQueryCache(cache);

                const originalResults = await query.exec();
                assert.deepStrictEqual(originalResults.map(h => h.passportId), ['1', '2']);
                assert.strictEqual(query._limitBufferResults?.length, 3);
                assert.strictEqual(cache.size, 2);

                // remove one of the items from the query results
                await collection.find({ selector: { passportId: '1' } }).update({
                    $set: { age: 1 }
                });

                simulateNewSession(collection);

                // now when we create the query again, it should fill in the missing element from the limit buffer
                const queryAgain = collection.find(query.mangoQuery);
                queryAgain.enableLimitBuffer(5).enablePersistentQueryCache(cache);

                const updatedResults = await queryAgain.exec();

                // The query should use the limit buffer to restore the results, and not need to re-exec the query
                assert.strictEqual(queryAgain._execOverDatabaseCount, 0);
                assert.deepStrictEqual(updatedResults.map(h => h.passportId), ['2', '3']);

                // There should now only be 2 items left in the limit buffer, it used the first one up to fill the results
                assert.strictEqual(queryAgain._limitBufferResults?.length, 2);

                collection.database.destroy();
            });

            it('limit buffer enabled, restores missing changes, results correctly with no re-exec', async () => {
                const { collection, query, cache} = await setUpLimitBufferSituation();

                // Persist WITH the limit buffer enabled
                query.enableLimitBuffer(5).enablePersistentQueryCache(cache);

                const originalResults = await query.exec();
                assert.deepStrictEqual(originalResults.map(h => h.passportId), ['1', '2']);
                assert.strictEqual(query._limitBufferResults?.length, 3);

                // uncache the query first, before changes are made
                simulateNewSession(collection);
                assert.strictEqual(cache.size, 2);

                // remove one of the items from the query results while query is not listening in memory
                await collection.find({ selector: { passportId: '1' } }).update({
                    $set: { age: 1 }
                });

                // now when we create the query again, it will fill in the missing element from the limit buffer
                const queryAgain = collection.find(query.mangoQuery);
                queryAgain.enableLimitBuffer(5).enablePersistentQueryCache(cache);

                const updatedResults = await queryAgain.exec();

                // The query should use the limit buffer to restore the results, and not need to re-exec the query
                assert.strictEqual(queryAgain._execOverDatabaseCount, 0);
                assert.deepStrictEqual(updatedResults.map(h => h.passportId), ['2', '3']);

                // There should now only be 2 items left in the limit buffer, it used the first one up to fill the results
                assert.strictEqual(queryAgain._limitBufferResults?.length, 2);

                collection.database.destroy();
            });

            it('limit buffer enabled, but gets exhausted', async () => {
                const { collection, query, cache} = await setUpLimitBufferSituation();

                // Persist WITH the limit buffer enabled, but only one doc
                query.enableLimitBuffer(1).enablePersistentQueryCache(cache);
                await query.exec();
                simulateNewSession(collection);

                // remove two of the items from the query results
                await collection.find({ selector: { passportId: '1' } }).update({
                    $set: { age: 1 }
                });
                await collection.find({ selector: { passportId: '2' } }).update({
                    $set: { age: 1 }
                });

                // now when we create the query again, it will fill in the missing element from the limit buffer
                // but then still need another item to hit the limit=2
                const queryAgain = collection.find(query.mangoQuery);
                queryAgain.enableLimitBuffer(1).enablePersistentQueryCache(cache);

                const updatedResults = await queryAgain.exec();

                // The query will have to still re-exec, but give the correct results
                assert.strictEqual(queryAgain._execOverDatabaseCount, 1);
                assert.deepStrictEqual(updatedResults.map(h => h.passportId), ['3', '4']);

                // And re-fill the 1 item in limit buffer:
                assert.strictEqual(queryAgain._limitBufferResults?.length, 1);
                assert.strictEqual(queryAgain._limitBufferResults?.[0].passportId, '5');

                collection.database.destroy();
            });

            it('limit buffer enabled, with a bunch of deletions', async () => {
                const { collection, query, cache} = await setUpLimitBufferSituation();

                // Persist WITH the limit buffer enabled
                query.enableLimitBuffer(3).enablePersistentQueryCache(cache);
                await query.exec();
                simulateNewSession(collection);

                // delete one item from the results, and one item from the limit buffer:
                await collection.find({ selector: { passportId: '1' } }).remove();
                await collection.find({ selector: { passportId: '3' } }).remove();

                const queryAgain = collection.find(query.mangoQuery);
                queryAgain.enableLimitBuffer(3).enablePersistentQueryCache(cache);

                const updatedResults = await queryAgain.exec();

                // The query should be able to fill up from the limit buffer
                assert.strictEqual(queryAgain._execOverDatabaseCount, 0);
                assert.deepStrictEqual(updatedResults.map(h => h.passportId), ['2', '4']);
                assert.strictEqual(queryAgain._limitBufferResults?.length, 1);

                // But if we go further, and use the last items from the limit buffer, we'll have to re-exec:
                uncacheRxQuery(collection._queryCache, queryAgain);
                await collection.find({ selector: { passportId: '4' } }).remove();
                await collection.find({ selector: { passportId: '5' } }).remove();

                const queryFinal = collection.find(query.mangoQuery);
                queryFinal.enableLimitBuffer(3).enablePersistentQueryCache(cache);

                const finalResults = await queryFinal.exec();
                assert.strictEqual(queryFinal._execOverDatabaseCount, 1);
                assert.deepStrictEqual(finalResults.map(h => h.passportId), ['2']);

                collection.database.destroy();
            });

            it('limit buffer enabled, doc added and limit buffer items changed, still restores correctly', async () => {
                const { collection, query, cache} = await setUpLimitBufferSituation();

                // Persist WITH the limit buffer enabled
                query.enableLimitBuffer(5).enablePersistentQueryCache(cache);

                await query.exec();

                simulateNewSession(collection);

                // Let's make 3 changes:
                // 1. remove both of the original results
                // 2. add in a new doc that should now be in the results
                // 3. modify one of the items in the limit buffer to change the correct order there
                await collection.find({ selector: { passportId: '1' } }).update({
                    $set: { age: 1 }
                });
                await collection.find({ selector: { passportId: '2' } }).update({
                    $set: { age: 1 }
                });
                // the new item should now be the first result, since it has the lowest age
                await collection.bulkUpsert([
                    schemaObjects.humanData('6', 20),
                ]);
                // change what would be the next result (passport id 3) to still match the filter, but now be last (so not in the results)
                await collection.find({ selector: { passportId: '3' } }).update({
                    $set: { age: 100 }
                });

                const queryAgain = collection.find(query.mangoQuery);
                queryAgain.enableLimitBuffer(5).enablePersistentQueryCache(cache);
                const updatedResults = await queryAgain.exec();

                // The query should use the limit buffer to restore the results, and not need to re-exec the query
                assert.strictEqual(queryAgain._execOverDatabaseCount, 0);

                // But it should also correctly fill in the new document into the correct position, and also handle the sort change
                assert.deepStrictEqual(updatedResults.map(h => h.passportId), ['6', '4']);

                // The two items in limit buffer should be in the correct order:
                assert.deepStrictEqual(queryAgain._limitBufferResults?.map((d) => d.passportId), ['5', '3']);

                collection.database.destroy();
            });

            it('limit buffer enabled, all items in buffer used but we have more matching non-buffer items', async () => {
                const { collection, query, cache} = await setUpLimitBufferSituation();

                // Persist WITH the limit buffer enabled
                query.enableLimitBuffer(2).enablePersistentQueryCache(cache);
                await query.exec();
                simulateNewSession(collection);

                // remove the 2 results, so we use up the 2 items in the limit buffer:
                await collection.find({ selector: { passportId: '1' } }).remove();
                await collection.find({ selector: { passportId: '2' } }).update({
                    $set: { age: 1 }
                });
                // But also add in some new docs, that match the filter but are sorted last
                await collection.bulkUpsert([
                    schemaObjects.humanData('6', 90),
                    schemaObjects.humanData('7', 90),
                ]);

                const queryAgain = collection.find(query.mangoQuery);
                queryAgain.enableLimitBuffer(2).enablePersistentQueryCache(cache);

                const updatedResults = await queryAgain.exec();

                // In this case we can use the limit buffer without re-execing, and still get correct results:
                assert.strictEqual(queryAgain._execOverDatabaseCount, 0);
                assert.deepStrictEqual(updatedResults.map(h => h.passportId), ['3', '4']);

                // But the new limit buffer will be empty -- we can't use the new documents because we don't know
                // how they would be sorted relative to other documents
                assert.strictEqual(queryAgain._limitBufferResults?.length, 0);

                simulateNewSession(collection);

                // If one more doc is removed from our results, we will HAVE to re-exec to ensure
                // correct results, test that:
                await collection.find({ selector: { passportId: '3' } }).update({
                    $set: { age: 1 }
                });

                const queryFinal = collection.find(query.mangoQuery);
                queryFinal.enableLimitBuffer(2).enablePersistentQueryCache(cache);

                const finalResults = await queryFinal.exec();

                // Query re-execs, and gives correct results:
                assert.strictEqual(queryFinal._execOverDatabaseCount, 1);
                assert.deepStrictEqual(finalResults.map(h => h.passportId), ['4', '5']);

                // When we re-exec, the limit buffer will also get filled:
                assert.deepStrictEqual(queryFinal._limitBufferResults?.map(h => h.passportId), ['6', '7']);

                collection.database.destroy();
            });

            it('Handles case where we have fewer than LIMIT matches', async () => {
                const { collection, cache } = await setUpLimitBufferSituation();

                const query = collection.find({ limit: 3, sort: [{age: 'asc'}], selector: { age: { $lt: 45 } } });
                query.enableLimitBuffer(2).enablePersistentQueryCache(cache);
                await query.exec();
                simulateNewSession(collection);

                // Remove something, still correct and no-re-exec
                await collection.find({ selector: { passportId: '1' } }).remove();

                const queryRemoved = collection.find(query.mangoQuery);
                queryRemoved.enableLimitBuffer(2).enablePersistentQueryCache(cache);
                const removedResults = await queryRemoved.exec();
                assert.strictEqual(queryRemoved._execOverDatabaseCount, 0);
                assert.deepStrictEqual(removedResults.map(h => h.passportId), ['2']);

                simulateNewSession(collection);

                // Now add some matching docs. Since they change, they should now be in results with no re-exec.
                await collection.find({ selector: { passportId: '5' } }).update({
                    $set: { age: 1 }
                });
                await collection.bulkUpsert([
                    schemaObjects.humanData('6', 2),
                    schemaObjects.humanData('7', 3),
                ]);
                const queryAdded = collection.find(query.mangoQuery);
                queryAdded.enableLimitBuffer(2).enablePersistentQueryCache(cache);
                const addedResults = await queryRemoved.exec();
                assert.strictEqual(queryAdded._execOverDatabaseCount, 0);
                assert.deepStrictEqual(addedResults.map(h => h.passportId), ['5', '6', '7']);

                collection.database.destroy();
            });
        });
    });
});

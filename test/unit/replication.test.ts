/**
 * These tests are for the replication primitives plugin.
 * Notice that not all edge cases are tested because
 * we do that inside of the GraphQL replication plugin.
 */

import assert from 'assert';
import {
    clone,
    wait,
    waitUntil
} from 'async-test-util';

import config, { describeParallel } from './config.ts';
import {
    schemaObjects,
    schemas,
    humansCollection,
    isFastMode,
    ensureReplicationHasNoErrors
} from '../../plugins/test-utils/index.mjs';

import {
    wrappedValidateAjvStorage
} from '../../plugins/validate-ajv/index.mjs';

import {
    RxCollection,
    ensureNotFalsy,
    randomCouchString,
    rxStorageInstanceToReplicationHandler,
    normalizeMangoQuery,
    RxError,
    RxTypeError,
    createRxDatabase,
    RxReplicationPullStreamItem,
    lastOfArray,
    RxJsonSchema,
    createBlob,
    RxAttachmentCreator,
    DeepReadonly,
    requestIdlePromise,
    prepareQuery
} from '../../plugins/core/index.mjs';

import {
    replicateRxCollection
} from '../../plugins/replication/index.mjs';

import type {
    ReplicationPullHandler,
    ReplicationPushHandler,
    RxReplicationWriteToMasterRow,
    RxStorage
} from '../../plugins/core/index.mjs';
import { firstValueFrom, Observable, Subject } from 'rxjs';
import type { HumanWithCompositePrimary, HumanWithTimestampDocumentType } from '../../src/plugins/test-utils/schema-objects.ts';


type CheckpointType = any;
type TestDocType = HumanWithTimestampDocumentType;

/**
 * Creates a pull handler that always returns
 * all documents.
 */
export function getPullHandler<RxDocType>(
    remoteCollection: RxCollection<RxDocType, {}, {}, {}>
): ReplicationPullHandler<RxDocType, CheckpointType> {
    const helper = rxStorageInstanceToReplicationHandler(
        remoteCollection.storageInstance,
        remoteCollection.database.conflictHandler as any,
        remoteCollection.database.token
    );
    const handler: ReplicationPullHandler<RxDocType, CheckpointType> = async (
        latestPullCheckpoint: CheckpointType | null,
        batchSize: number
    ) => {
        const result = await helper.masterChangesSince(latestPullCheckpoint, batchSize);
        return result;
    };
    return handler;
}
export function getPullStream<RxDocType>(
    remoteCollection: RxCollection<RxDocType, {}, {}, {}>
): Observable<RxReplicationPullStreamItem<RxDocType, any>> {
    const helper = rxStorageInstanceToReplicationHandler(
        remoteCollection.storageInstance,
        remoteCollection.conflictHandler,
        remoteCollection.database.token
    );
    return helper.masterChangeStream$;
}
export function getPushHandler<RxDocType>(
    remoteCollection: RxCollection<RxDocType, {}, {}, {}>
): ReplicationPushHandler<RxDocType> {
    const helper = rxStorageInstanceToReplicationHandler(
        remoteCollection.storageInstance,
        remoteCollection.conflictHandler,
        remoteCollection.database.token
    );
    const handler: ReplicationPushHandler<RxDocType> = async (
        rows: RxReplicationWriteToMasterRow<RxDocType>[]
    ) => {
        const result = await helper.masterWrite(rows);
        return result;
    };
    return handler;
}


describe('replication.test.ts', () => {
    if (!config.storage.hasReplication) {
        return;
    }
    const REPLICATION_IDENTIFIER_TEST = 'replication-ident-tests';
    async function getTestCollections(docsAmount: { local: number; remote: number; }): Promise<{
        localCollection: RxCollection<TestDocType, {}, {}, {}>;
        remoteCollection: RxCollection<TestDocType, {}, {}, {}>;
    }> {
        const localCollection = await humansCollection.createHumanWithTimestamp(docsAmount.local, randomCouchString(10), false);
        const remoteCollection = await humansCollection.createHumanWithTimestamp(docsAmount.remote, randomCouchString(10), false);
        return {
            localCollection,
            remoteCollection
        };
    }
    async function ensureEqualState<RxDocType>(
        collectionA: RxCollection<RxDocType>,
        collectionB: RxCollection<RxDocType>,
        context?: string
    ) {
        const [
            docsA,
            docsB
        ] = await Promise.all([
            collectionA.find().exec().then(docs => docs.map(d => d.toJSON(true))),
            collectionB.find().exec().then(docs => docs.map(d => d.toJSON(true)))
        ]);

        docsA.forEach((docA, idx) => {
            const docB = docsB[idx];
            const cleanDocToCompare = (doc: DeepReadonly<RxDocType>) => {
                return Object.assign({}, doc, {
                    _meta: undefined,
                    _rev: undefined
                });
            };
            try {
                assert.deepStrictEqual(
                    cleanDocToCompare(docA),
                    cleanDocToCompare(docB)
                );
            } catch (err) {
                console.log('## ERROR: State not equal (context: "' + context + '")');
                console.log(JSON.stringify(docA, null, 4));
                console.log(JSON.stringify(docB, null, 4));
                throw new Error('STATE not equal (context: "' + context + '")');
            }
        });
    }

    let storageWithValidation: RxStorage<any, any>;
    describe('init', () => {
        it('create storage', () => {
            storageWithValidation = wrappedValidateAjvStorage({
                storage: config.storage.getStorage()
            });
        });
    });
    describeParallel('non-live replication', () => {
        it('should replicate both sides', async () => {
            const docsPerSide = 15;
            const { localCollection, remoteCollection } = await getTestCollections({
                local: docsPerSide,
                remote: docsPerSide
            });


            const batchSize = 12;
            const pullHandler = getPullHandler(remoteCollection);
            const pushHandler = getPushHandler(remoteCollection);
            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: false,
                pull: {
                    batchSize,
                    handler: (lastPulledCheckpoint: CheckpointType, pullBatchSize: number) => {
                        // ensure the batchSize from the settings is respected
                        assert.strictEqual(pullBatchSize, batchSize);
                        return pullHandler(lastPulledCheckpoint, pullBatchSize);
                    }
                },
                push: {
                    batchSize,
                    handler: (docs) => {
                        if (docs.length > batchSize) {
                            throw new Error('push got more docs then the batch size');
                        }
                        return pushHandler(docs);
                    }
                }
            });
            ensureReplicationHasNoErrors(replicationState);

            await replicationState.awaitInitialReplication();

            const docsLocal = await localCollection.find().exec();
            const docsRemote = await remoteCollection.find().exec();

            assert.strictEqual(
                docsLocal.length,
                docsRemote.length
            );
            assert.strictEqual(
                docsLocal.length,
                docsPerSide * 2
            );

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
        it('should allow asynchronous push and pull modifiers', async () => {
            const docsPerSide = 5;
            const { localCollection, remoteCollection } = await getTestCollections({
                local: docsPerSide,
                remote: docsPerSide
            });
            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: false,
                pull: {
                    handler: getPullHandler(remoteCollection),
                    modifier: async (doc) => {
                        await wait(isFastMode() ? 10 : 100);
                        doc = clone(doc);
                        doc.name = 'pull-modified';
                        return doc;
                    }
                },
                push: {
                    handler: getPushHandler(remoteCollection),
                    modifier: async (doc) => {
                        await wait(isFastMode() ? 10 : 100);
                        doc = clone(doc);
                        doc.name = 'push-modified';
                        return doc;
                    }
                }
            });
            ensureReplicationHasNoErrors(replicationState);
            await replicationState.awaitInitialReplication();

            const docsLocal = await localCollection.find().exec();
            const docsRemote = await remoteCollection.find().exec();

            const pushModifiedRemote = docsRemote.filter(d => d.name === 'push-modified');
            assert.strictEqual(pushModifiedRemote.length, docsPerSide);

            const pullModifiedLocal = docsLocal.filter(d => d.name === 'pull-modified');
            /**
             * Pushed documents will be also pull modified
             * when they are fetched from the master again.
             * So here we just do a gte check instead of a strict equal.
             */
            assert.ok(pullModifiedLocal.length >= docsPerSide);

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
        it('should skip the document when the push-modifier returns null', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({
                local: 0,
                remote: 0
            });
            await localCollection.bulkInsert(
                new Array(10).fill(0).map((_v, idx) => {
                    return schemaObjects.humanWithTimestampData({
                        name: 'from-local',
                        age: idx + 1
                    });
                })
            );
            const replicationState = replicateRxCollection<HumanWithTimestampDocumentType, any>({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: false,
                pull: {
                    handler: getPullHandler(remoteCollection)
                },
                push: {
                    handler: getPushHandler(remoteCollection),
                    modifier: (doc) => {
                        // skip every second document
                        if (doc.age % 2 === 0) {
                            return null;
                        }
                        return doc;
                    }
                }
            });
            ensureReplicationHasNoErrors(replicationState);
            await replicationState.awaitInitialReplication();

            const docsRemote = await remoteCollection.find().exec();
            assert.strictEqual(docsRemote.length, 5);

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
        it('should not save pulled documents that do not match the schema', async () => {
            const amount = 5;
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: amount });

            /**
             * Use collection with different schema
             * to provoke validation errors.
             */
            const otherSchema = clone(schemas.humanWithTimestamp);
            otherSchema.properties.age.maximum = 0;
            const otherSchemaCollection = await humansCollection.createBySchema(
                otherSchema,
                undefined,
                storageWithValidation
            );

            const replicationState = replicateRxCollection({
                collection: otherSchemaCollection as any,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: false,
                pull: {
                    handler: getPullHandler(remoteCollection)
                },
                push: {
                    handler: getPushHandler(remoteCollection)
                },
                retryTime: 100
            });
            const errors: (RxError | RxTypeError)[] = [];
            replicationState.error$.subscribe(err => errors.push(err));
            await replicationState.awaitInitialReplication();

            await wait(isFastMode() ? 0 : 100);

            const docsLocal = await otherSchemaCollection.find().exec();
            assert.strictEqual(docsLocal.length, 0);


            assert.strictEqual(errors.length, amount);
            assert.ok(JSON.stringify(errors[0].parameters).includes('maximum'));


            localCollection.database.destroy();
            remoteCollection.database.destroy();
            otherSchemaCollection.database.destroy();
        });
        it('should never resolve awaitInitialReplication() on erroring replication', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 10, remote: 10 });
            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: false,
                pull: {
                    handler: async () => {
                        await wait(0);
                        throw new Error('must throw on pull');
                    }
                },
                push: {
                    handler: async () => {
                        await wait(0);
                        throw new Error('must throw on push');
                    }
                },
                retryTime: 100
            });
            await firstValueFrom(replicationState.error$);

            let hasResolved = false;
            replicationState.awaitInitialReplication().then(() => {
                hasResolved = true;
            });
            await wait(isFastMode() ? 200 : 500);
            assert.strictEqual(hasResolved, false);

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
        it('should never resolve awaitInitialReplication() on canceled replication', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 10, remote: 10 });
            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: false,
                retryTime: 100,
                autoStart: true,
                pull: {
                    handler: async () => {
                        await wait(0);
                        throw new Error('must throw on pull');
                    }
                },
                push: {
                    handler: async () => {
                        await wait(0);
                        throw new Error('must throw on push');
                    }
                }
            });
            await firstValueFrom(replicationState.error$);
            let hasResolved = false;
            replicationState.awaitInitialReplication().then(() => {
                hasResolved = true;
            });
            await replicationState.cancel();

            await wait(isFastMode() ? 200 : 500);
            assert.strictEqual(hasResolved, false);

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
    });
    describeParallel('live replication', () => {
        it('should replicate all writes', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });

            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: getPullHandler(remoteCollection)
                },
                push: {
                    handler: getPushHandler(remoteCollection)
                }
            });
            ensureReplicationHasNoErrors(replicationState);

            await replicationState.awaitInitialReplication();
            const docsRemoteQuery = await remoteCollection.findOne();

            // insert
            const id = 'foobar';
            const docData = schemaObjects.humanWithTimestampData({
                id
            });
            let doc = await localCollection.insert(docData);
            await waitUntil(async () => {
                const remoteDoc = await docsRemoteQuery.exec();
                return !!remoteDoc;
            });

            // UPDATE
            doc = await doc.incrementalPatch({
                age: 100
            });
            await waitUntil(async () => {
                const remoteDoc = await docsRemoteQuery.exec(true);
                return remoteDoc.age === 100;
            });

            // DELETE
            await wait(100);
            await doc.remove();
            await waitUntil(async () => {
                const remoteDoc = await docsRemoteQuery.exec();
                return !remoteDoc;
            });

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
        /**
         * @link https://github.com/pubkey/rxdb/issues/3994
         */
        it('#3994 should respect the push.batchSize', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });

            const batchSize = 2;
            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: getPullHandler(remoteCollection)
                },
                push: {
                    handler: (docs) => {
                        if (docs.length === 0 || docs.length > batchSize) {
                            throw new Error('push.batchSize(' + batchSize + ') not respected ' + docs.length);
                        }
                        return getPushHandler(remoteCollection)(docs as any);
                    },
                    batchSize
                }
            });
            ensureReplicationHasNoErrors(replicationState);

            /**
             * Insert many documents at once to
             * produce an eventBulk that contains many documents
             */
            await localCollection.bulkInsert(
                new Array(10).fill(0).map((() => schemaObjects.humanWithTimestampData()))
            );

            await replicationState.awaitInSync();

            const docsOnRemote = await remoteCollection.find().exec();
            assert.strictEqual(
                docsOnRemote.length,
                10
            );

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
        it('should emit active$ when a replication cycle is running', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });
            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: getPullHandler(remoteCollection)
                },
                push: {
                    handler: getPushHandler(remoteCollection)
                }
            });
            ensureReplicationHasNoErrors(replicationState);

            const values: boolean[] = [];
            replicationState.active$.subscribe((active) => {
                values.push(active);
            });

            await replicationState.awaitInitialReplication();
            assert.strictEqual(
                values.length > 0,
                true
            );
            assert.strictEqual(
                values.includes(true),
                true
            );
            assert.strictEqual(
                values[values.length - 1],
                false
            );

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
    });
    describeParallel('other', () => {
        describe('autoStart', () => {
            it('should run first replication by default', async () => {
                const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });
                const replicationState = replicateRxCollection({
                    collection: localCollection,
                    replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                    live: false,
                    autoStart: true,
                    waitForLeadership: false,
                    pull: {
                        handler: getPullHandler(remoteCollection)
                    }
                });
                await replicationState.awaitInitialReplication();
                assert.ok(
                    ensureNotFalsy(replicationState.internalReplicationState).stats.down.downstreamResyncOnce > 0
                );

                localCollection.database.destroy();
                remoteCollection.database.destroy();
            });
            it('should not run first replication when autoStart is set to false', async () => {
                const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });
                const replicationState = replicateRxCollection({
                    collection: localCollection,
                    replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                    live: false,
                    autoStart: false,
                    waitForLeadership: false,
                    pull: {
                        handler: getPullHandler(remoteCollection)
                    }
                });

                await wait(100);


                // not replicated
                assert.ok(!replicationState.internalReplicationState);

                localCollection.database.destroy();
                remoteCollection.database.destroy();
            });
        });
        describe('.awaitInSync()', () => {
            it('should resolve after some time', async () => {
                const { localCollection, remoteCollection } = await getTestCollections({ local: 5, remote: 5 });

                const replicationState = replicateRxCollection({
                    collection: localCollection,
                    replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                    live: false,
                    pull: {
                        handler: getPullHandler(remoteCollection)
                    },
                    push: {
                        handler: getPushHandler(remoteCollection)
                    }
                });
                await replicationState.awaitInSync();

                localCollection.database.destroy();
                remoteCollection.database.destroy();
            });
            it('should never resolve when offline', async () => {
                const { localCollection, remoteCollection } = await getTestCollections({ local: 5, remote: 5 });

                const replicationState = replicateRxCollection({
                    collection: localCollection,
                    replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                    live: false,
                    pull: {
                        handler: async () => {
                            await wait(100);
                            throw new Error('always error');
                        }
                    },
                    push: {
                        handler: getPushHandler(remoteCollection)
                    }
                });
                let resolved = false;
                replicationState.awaitInSync().then(() => {
                    resolved = true;
                });
                await wait(isFastMode() ? 100 : 400);
                assert.strictEqual(resolved, false);

                localCollection.database.destroy();
                remoteCollection.database.destroy();
            });
        });
        it('should clean up the replication meta storage when the get collection gets removed', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 5, remote: 5 });
            const localDbName = localCollection.database.name;

            const replicationState1 = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: getPullHandler(remoteCollection)
                },
                push: {
                    handler: getPushHandler(remoteCollection)
                }
            });
            ensureReplicationHasNoErrors(replicationState1);
            await replicationState1.awaitInitialReplication();

            async function docsInMeta(repState: typeof replicationState1): Promise<number> {
                const metaInstance = ensureNotFalsy(repState.metaInstance);
                const prepared = prepareQuery(
                    metaInstance.schema,
                    normalizeMangoQuery(
                        metaInstance.schema,
                        {}
                    )
                );
                const result = await metaInstance.query(prepared);
                return result.documents.length;
            }

            await localCollection.remove();
            await localCollection.database.destroy();

            const localCollection2 = await humansCollection.createHumanWithTimestamp(0, localDbName, false);

            let continueReplication: Function | null = undefined as any;
            const continues = new Promise(res => {
                continueReplication = res;
            });

            const replicationState2 = replicateRxCollection({
                collection: localCollection2,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: async (chkpt, bSize) => {
                        await continues;
                        return getPullHandler(remoteCollection)(chkpt, bSize);
                    }
                },
                push: {
                    handler: async (docs) => {
                        await continues;
                        return getPushHandler(remoteCollection)(docs);
                    }
                },
                autoStart: false
            });
            ensureReplicationHasNoErrors(replicationState2);

            await replicationState2.start();
            const docsInMetaAfter = await docsInMeta(replicationState2);

            /**
             * Because in the localCollection2 we do not insert any documents,
             * there must be less documents in the meta collection
             * because it only contains the checkpoints.
             */
            assert.strictEqual(docsInMetaAfter, 0);
            ensureNotFalsy(continueReplication)();

            /**
             * the re-created collection should have re-run the replication
             * and contain all documents from the remove.
             */
            await replicationState2.awaitInitialReplication();
            const localDocs = await localCollection2.find().exec();
            const remoteDocs = await remoteCollection.find().exec();

            assert.deepStrictEqual(
                localDocs.map(d => d.toJSON()),
                remoteDocs.map(d => d.toJSON())
            );

            localCollection2.database.destroy();
            remoteCollection.database.destroy();
        });
        it('should respect the initial push checkpoint', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });

            let lastLocalCheckpoint: any;
            localCollection.checkpoint$.subscribe(checkpoint => lastLocalCheckpoint = checkpoint);
            await localCollection.insert(schemaObjects.humanWithTimestampData());

            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: getPullHandler(remoteCollection)
                },
                push: {
                    handler: getPushHandler(remoteCollection),
                    initialCheckpoint: lastLocalCheckpoint
                }
            });
            ensureReplicationHasNoErrors(replicationState);
            await replicationState.awaitInitialReplication();

            const remoteDocs = await remoteCollection.find().exec();
            assert.deepEqual(remoteDocs.length, 0);

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
        it('should respect the initial pull checkpoint', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });

            let lastRemoteCheckpoint: any;
            remoteCollection.checkpoint$.subscribe(checkpoint => lastRemoteCheckpoint = checkpoint);
            await remoteCollection.insert(schemaObjects.humanWithTimestampData());

            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: getPullHandler(remoteCollection),
                    initialCheckpoint: lastRemoteCheckpoint
                },
                push: {
                    handler: getPushHandler(remoteCollection),
                }
            });
            ensureReplicationHasNoErrors(replicationState);
            await replicationState.awaitInitialReplication();

            const localDocs = await localCollection.find().exec();
            assert.deepEqual(localDocs.length, 0);

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
    });
    describeParallel('RxReplicationState.remove()', () => {
        it('should remove the replication state and start the replication from scratch', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 1, remote: 1 });
            const calledCheckpoints: any[] = [];
            const startReplication = async () => {
                const replicationState = replicateRxCollection({
                    collection: localCollection,
                    replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                    live: true,
                    pull: {
                        handler: (checkpoint, batchSize) => {
                            calledCheckpoints.push(checkpoint);
                            return getPullHandler(remoteCollection)(checkpoint, batchSize);
                        },
                    },
                    push: {
                        handler: getPushHandler(remoteCollection),
                    }
                });
                await replicationState.awaitInSync();
                return replicationState;
            };
            let currentReplicationState = await startReplication();
            await currentReplicationState.remove();

            currentReplicationState = await startReplication();

            assert.deepStrictEqual(calledCheckpoints, [undefined, undefined]);

            localCollection.database.destroy();
            remoteCollection.database.destroy();
        });
    });
    describeParallel('attachment replication', () => {
        if (!config.storage.hasAttachments) {
            return;
        }
        /**
         * Here we use a RxDatabase insteaf of the plain RxStorageInstance.
         * This makes handling attachment easier
         */
        it('attachments replication: up and down with streaming', async () => {
            const localCollection = await humansCollection.createAttachments(3);
            const remoteCollection = await humansCollection.createAttachments(3);

            const localDocs = await localCollection.find().exec();
            const remoteDocs = await remoteCollection.find().exec();

            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: true,
                pull: {
                    handler: getPullHandler(remoteCollection),
                    stream$: getPullStream(remoteCollection)
                },
                push: {
                    handler: getPushHandler(remoteCollection),
                }
            });
            ensureReplicationHasNoErrors(replicationState);
            await replicationState.awaitInitialReplication();

            function getRandomAttachment(
                id = randomCouchString(10),
                size = 20
            ): RxAttachmentCreator {
                const attachmentData = randomCouchString(size);
                const dataBlob = createBlob(attachmentData, 'text/plain');
                return {
                    id,
                    data: dataBlob,
                    type: 'text/plain'
                };
            }

            await replicationState.awaitInitialReplication();
            await ensureEqualState(localCollection, remoteCollection, 'first sync');

            // add attachments
            await remoteDocs[0].getLatest().putAttachment(getRandomAttachment('master1'));
            await localDocs[0].getLatest().putAttachment(getRandomAttachment('fork1'));

            await Promise.all([
                remoteDocs[1].getLatest().putAttachment(getRandomAttachment()),
                remoteDocs[2].getLatest().putAttachment(getRandomAttachment()),
                localDocs[1].getLatest().putAttachment(getRandomAttachment()),
                localDocs[2].getLatest().putAttachment(getRandomAttachment())
            ]);

            await replicationState.reSync();
            await replicationState.awaitInSync();
            await requestIdlePromise();
            await ensureEqualState(localCollection, remoteCollection, 'after adding');

            // add more attachments to docs that already have attachments
            await remoteDocs[0].getLatest().putAttachment(getRandomAttachment('master2'));
            await localDocs[0].getLatest().putAttachment(getRandomAttachment('fork2'));

            await replicationState.reSync();
            await replicationState.awaitInSync();
            await ensureEqualState(localCollection, remoteCollection, 'after add more');

            // overwrite attachments
            await remoteDocs[0].getLatest().putAttachment(getRandomAttachment('master1', 5));
            await localDocs[0].getLatest().putAttachment(getRandomAttachment('fork1', 5));

            await replicationState.reSync();
            await replicationState.awaitInSync();
            await ensureEqualState(localCollection, remoteCollection, 'after overwrite');


            /**
             * The meta instance should not contain attachments data
             */
            const metaStorage = ensureNotFalsy(replicationState.metaInstance);
            const preparedQuery = prepareQuery(
                metaStorage.schema,
                normalizeMangoQuery(
                    metaStorage.schema,
                    {}
                )
            );
            const result = await metaStorage.query(preparedQuery);
            const metaDocs = result.documents;
            metaDocs.forEach(doc => {
                if (doc.isCheckpoint !== '1' && doc.docData._attachments) {
                    Object.values(doc.docData._attachments).forEach((attachment) => {
                        if ((attachment as RxAttachmentCreator).data) {
                            throw new Error('meta doc contains attachment data');
                        }
                    });
                }
            });

            await localCollection.database.destroy();
            await remoteCollection.database.destroy();
        });
    });
    describeParallel('issues', () => {
        /**
         * @link https://github.com/pubkey/rxdb/pull/7804
         */
        function setupDownstreamTestReplication(
            local: RxCollection<TestDocType>,
            remote: RxCollection<TestDocType>
        ) {
            return replicateRxCollection<TestDocType, any>({
                collection: local,
                replicationIdentifier: 'downstream-test',
                live: true,
                pull: {
                    handler: getPullHandler(remote),
                    stream$: getPullStream(remote)
                },
                push: {
                    handler: getPushHandler(remote)
                }
            });
        }

        /**
         * Simulates a crash between fork write and meta write in downstream.
         *
         * When the process dies after forkInstance.bulkWrite() succeeds but before
         * metaInstance.bulkWrite() completes, the fork has the new state but the
         * assumed master in meta is stale. On the next downstream cycle, this
         * mismatch is detected as a "non-upstream-replicated local write" and the
         * document is skipped - expecting upstream to resolve the conflict. But
         * upstream never picks it up because the fork write came from downstream,
         * leaving the document permanently stuck.
         */
        it('#7804 (1/2) should recover downstream sync after meta write is lost between fork and meta write (simulated crash)', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });
            const docId = 'crash-test-doc';

            // Insert initial document on remote
            await remoteCollection.insert(schemaObjects.humanWithTimestampData({
                id: docId,
                name: 'Initial',
                age: 1
            }));

            // Start replication and let it sync the initial document
            const replicationState = setupDownstreamTestReplication(localCollection, remoteCollection);
            ensureReplicationHasNoErrors(replicationState);
            await replicationState.awaitInitialReplication();

            // Verify initial sync
            const initialLocal = await localCollection.findOne(docId).exec(true);
            assert.strictEqual(initialLocal.name, 'Initial');

            // Monkey-patch metaInstance.bulkWrite to silently drop the next
            // downstream meta write - simulating a crash between fork and meta write.
            const metaInstance = ensureNotFalsy(replicationState.internalReplicationState).input.metaInstance;
            const originalBulkWrite = metaInstance.bulkWrite.bind(metaInstance);
            let metaWriteDropped = false;

            metaInstance.bulkWrite = function (rows: any[], context: string) {
                if (context === 'replication-down-write-meta' && !metaWriteDropped) {
                    metaWriteDropped = true;
                    // Silently swallow the write: fork already persisted, meta is lost.
                    return Promise.resolve({ success: [], error: [] });
                }
                return originalBulkWrite(rows, context);
            } as any;

            // Update the document on remote.
            // Downstream will write the new state to the fork, but the
            // corresponding meta write is silently dropped above.
            const remoteDoc = await remoteCollection.findOne(docId).exec(true);
            const internalState = ensureNotFalsy(replicationState.internalReplicationState);
            let prevDown = internalState.streamQueue.down;
            await remoteDoc.incrementalPatch({
                name: 'FirstUpdate',
                age: 2
            });

            // Wait for the downstream cycle to finish
            await waitUntil(() => internalState.streamQueue.down !== prevDown, undefined, 40);
            await internalState.streamQueue.down;

            assert.ok(metaWriteDropped, 'Meta write should have been intercepted and dropped');
            const afterFirst = await localCollection.findOne(docId).exec(true);
            assert.strictEqual(afterFirst.name, 'FirstUpdate');

            // Restore original bulkWrite so meta works normally again.
            metaInstance.bulkWrite = originalBulkWrite;

            // At this point the replication state is:
            //   forkState       = { name: 'FirstUpdate', age: 2 }
            //   assumedMaster   = { name: 'Initial', age: 1 }      (stale - meta write was lost)
            //
            // Without the origin marker, downstream sees forkState != assumedMaster
            // and treats it as a "non-upstream-replicated local write", skipping the document.

            // Update the document on remote again.
            prevDown = internalState.streamQueue.down;
            const remoteDoc2 = await remoteCollection.findOne(docId).exec(true);
            await remoteDoc2.incrementalPatch({
                name: 'SecondUpdate',
                age: 3
            });

            // Wait for downstream to finish processing, then verify it recovered.
            await waitUntil(() => internalState.streamQueue.down !== prevDown, undefined, 40);
            await internalState.streamQueue.down;

            const localDoc = await localCollection.findOne(docId).exec(true);
            assert.strictEqual(localDoc.name, 'SecondUpdate', 'should have replicated the second update from the remote');
            assert.strictEqual(localDoc.age, 3);

            await replicationState.cancel();
            await localCollection.database.destroy();
            await remoteCollection.database.destroy();
        });
        it('#7804 (2/2) should sync downstream updates when local and remote have different documents', async () => {
            const { localCollection, remoteCollection } = await getTestCollections({ local: 0, remote: 0 });
            const docId = 'different-doc';

            // Insert different documents in each
            await remoteCollection.insert(schemaObjects.humanWithTimestampData({
                id: docId,
                name: 'RemoteDocument',
                age: 10
            }));
            await localCollection.insert(schemaObjects.humanWithTimestampData({
                id: docId,
                name: 'LocalDocument',
                age: 20
            }));

            const replicationState = setupDownstreamTestReplication(localCollection, remoteCollection);
            ensureReplicationHasNoErrors(replicationState);

            await replicationState.awaitInitialReplication();
            await replicationState.awaitInSync();

            // Update document on remote
            const internalState = ensureNotFalsy(replicationState.internalReplicationState);
            const prevDown = internalState.streamQueue.down;
            const remoteDoc = await remoteCollection.findOne(docId).exec(true);
            await remoteDoc.incrementalPatch({
                name: 'UpdatedFromRemote',
                age: 999
            });

            // Wait for downstream cycle to complete
            await waitUntil(() => internalState.streamQueue.down !== prevDown, 1000, 10);
            await internalState.streamQueue.down;

            // Verify local received the update
            const localDoc = await localCollection.findOne(docId).exec(true);
            assert.strictEqual(localDoc.name, 'UpdatedFromRemote');
            assert.strictEqual(localDoc.age, 999);

            await replicationState.cancel();
            await localCollection.database.destroy();
            await remoteCollection.database.destroy();
        });
        it('#4190 Composite Primary Keys broken on replicated collections', async () => {
            const db = await createRxDatabase({
                name: randomCouchString(10),
                storage: config.storage.getStorage(),
                eventReduce: true,
                ignoreDuplicate: true
            });
            const collections = await db.addCollections({
                mycollection: {
                    schema: schemas.humanCompositePrimary
                }
            });
            const mycollection: RxCollection<HumanWithCompositePrimary> = collections.mycollection;

            const pullStream$ = new Subject<RxReplicationPullStreamItem<any, CheckpointType>>();
            let fetched = false;
            const replicationState = replicateRxCollection({
                replicationIdentifier: 'replicate-' + randomCouchString(10),
                collection: mycollection,
                pull: {
                    // eslint-disable-next-line require-await
                    handler: async (lastCheckpoint) => {
                        const docs: HumanWithCompositePrimary[] = (fetched) ?
                            [] :
                            [schemaObjects.humanWithCompositePrimary()];
                        fetched = true;
                        const lastDoc = lastOfArray(docs);
                        return {
                            documents: docs,
                            checkpoint: !lastDoc
                                ? lastCheckpoint
                                : {
                                    id: mycollection.schema.getPrimaryOfDocumentData(lastDoc),
                                    updatedAt: Date.now()
                                }
                        };
                    },
                    batchSize: 1,
                    stream$: pullStream$.asObservable()
                },
            });
            ensureReplicationHasNoErrors(replicationState);

            await replicationState.awaitInitialReplication();

            // clean up afterwards
            db.destroy();
        });
        it('#4315 Id length limit reached with composite key', async () => {
            const primaryKeyLength = 500;
            async function getCollection(): Promise<RxCollection<TestDocType>> {
                const db = await createRxDatabase({
                    name: randomCouchString(10),
                    storage: storageWithValidation,
                    eventReduce: true,
                    ignoreDuplicate: true
                });
                const schema: RxJsonSchema<TestDocType> = clone(schemas.humanWithTimestamp);
                schema.properties.id.maxLength = primaryKeyLength;
                const collections = await db.addCollections({
                    mycollection: {
                        schema
                    }
                });
                const mycollection: RxCollection<TestDocType> = collections.mycollection;
                return mycollection;
            }

            const remoteCollection = await getCollection();
            const localCollection = await getCollection();


            const docA = schemaObjects.humanWithTimestampData({
                id: randomCouchString(primaryKeyLength)
            });
            await remoteCollection.insert(docA);
            const docB = schemaObjects.humanWithTimestampData({
                id: randomCouchString(primaryKeyLength)
            });
            await localCollection.insert(docB);

            const pullHandler = getPullHandler(remoteCollection);
            const pushHandler = getPushHandler(remoteCollection);
            const batchSize = 10;
            const replicationState = replicateRxCollection({
                collection: localCollection,
                replicationIdentifier: REPLICATION_IDENTIFIER_TEST,
                live: false,
                pull: {
                    batchSize,
                    handler: (lastPulledCheckpoint: CheckpointType, pullBatchSize: number) => {
                        return pullHandler(lastPulledCheckpoint, pullBatchSize);
                    }
                },
                push: {
                    batchSize,
                    handler: (docs) => {
                        return pushHandler(docs);
                    }
                }
            });
            ensureReplicationHasNoErrors(replicationState);


            await replicationState.awaitInitialReplication();

            const docsLocal = await localCollection.find().exec();
            const docsRemote = await remoteCollection.find().exec();

            assert.strictEqual(docsLocal.length, 2);
            assert.strictEqual(docsRemote.length, 2);


            remoteCollection.database.destroy();
            localCollection.database.destroy();
        });
        it('#5571 Replication observation mode ignored when push handler is waiting for response from backend', async () => {
            const serverCollection = await humansCollection.create(0);
            const clientCollection = await humansCollection.create(0);
            const replicationState = replicateRxCollection({
                replicationIdentifier: 'replicate-' + randomCouchString(10),
                collection: clientCollection,
                pull: {
                    handler: (lastPulledCheckpoint: CheckpointType, pullBatchSize: number) => {
                        return getPullHandler(serverCollection)(lastPulledCheckpoint, pullBatchSize);
                    },
                    stream$: getPullStream(serverCollection).pipe(
                    )
                },
                push: {
                    handler: async (rows) => {
                        // simulate that the server is modifying the pushed document.
                        rows = rows.map(row => {
                            row = clone(row);
                            row.newDocumentState.lastName = 'server-modified';
                            return row;
                        });
                        const resultPromise = getPushHandler(serverCollection)(rows);
                        await wait(50);
                        const result = await resultPromise;
                        return result;
                    }
                },
            });
            ensureReplicationHasNoErrors(replicationState);
            await replicationState.awaitInitialReplication();

            await clientCollection.insert(schemaObjects.humanData('first'));
            await replicationState.awaitInSync();

            const docOnClient = await clientCollection.findOne().exec(true);
            assert.strictEqual(docOnClient.lastName, 'server-modified');

            serverCollection.database.destroy();
            clientCollection.database.destroy();
        });
    });
});

import _createClass from "@babel/runtime/helpers/createClass";
import { BehaviorSubject, firstValueFrom, merge } from 'rxjs';
import { mergeMap, filter, map, startWith, distinctUntilChanged, shareReplay } from 'rxjs/operators';
import { areRxDocumentArraysEqual, ensureNotFalsy, now, overwriteGetterForCaching, pluginMissing, PROMISE_RESOLVE_FALSE, RX_META_LWT_MINIMUM, RXJS_SHARE_REPLAY_DEFAULTS, sortObject, stringifyFilter } from './plugins/utils';
import { newRxError } from './rx-error';
import { runPluginHooks } from './hooks';
import { calculateNewResults } from './event-reduce';
import { triggerCacheReplacement } from './query-cache';
import { getQueryMatcher, getSortComparator, normalizeMangoQuery } from './rx-query-helper';
var _queryCount = 0;
var newQueryID = function () {
  return ++_queryCount;
};

// allow changes to be 100ms older than the actual lwt value
var UPDATE_DRIFT = 100;
export var RxQueryBase = /*#__PURE__*/function () {
  /**
   * Some stats then are used for debugging and cache replacement policies
   */

  // used in the query-cache to determine if the RxQuery can be cleaned up.

  // used to count the subscribers to the query

  /**
   * Contains the current result state
   * or null if query has not run yet.
   */

  function RxQueryBase(op, mangoQuery, collection,
  // used by some plugins
  other = {}) {
    this.id = newQueryID();
    this._execOverDatabaseCount = 0;
    this._creationTime = now();
    this._lastEnsureEqual = 0;
    this.uncached = false;
    this.refCount$ = new BehaviorSubject(null);
    this._result = null;
    this._latestChangeEvent = -1;
    this._lastExecStart = 0;
    this._lastExecEnd = 0;
    this._limitBufferSize = null;
    this._limitBufferResults = null;
    this._persistentQueryCacheResult = undefined;
    this._persistentQueryCacheResultLwt = undefined;
    this._ensureEqualQueue = PROMISE_RESOLVE_FALSE;
    this.op = op;
    this.mangoQuery = mangoQuery;
    this.collection = collection;
    this.other = other;
    if (!mangoQuery) {
      this.mangoQuery = _getDefaultQuery();
    }
    this.isFindOneByIdQuery = isFindOneByIdQuery(this.collection.schema.primaryPath, mangoQuery);
  }
  var _proto = RxQueryBase.prototype;
  /**
   * Returns an observable that emits the results
   * This should behave like an rxjs-BehaviorSubject which means:
   * - Emit the current result-set on subscribe
   * - Emit the new result-set when an RxChangeEvent comes in
   * - Do not emit anything before the first result-set was created (no null)
   */
  /**
   * set the new result-data as result-docs of the query
   * @param newResultData json-docs that were received from the storage
   */
  _proto._setResultData = function _setResultData(newResultData) {
    if (typeof newResultData === 'number') {
      this._result = {
        docsData: [],
        docsKeys: [],
        docsMap: new Map(),
        docsDataMap: new Map(),
        count: newResultData,
        docs: [],
        time: now()
      };
      return;
    } else if (newResultData instanceof Map) {
      newResultData = Array.from(newResultData.values());
    }
    var docsDataMap = new Map();
    var docsMap = new Map();
    var docs = newResultData.map(docData => this.collection._docCache.getCachedRxDocument(docData));

    /**
     * Instead of using the newResultData in the result cache,
     * we directly use the objects that are stored in the RxDocument
     * to ensure we do not store the same data twice and fill up the memory.
     */
    var docsKeys = [];
    var docsData = docs.map(doc => {
      docsDataMap.set(doc.primary, doc._data);
      docsMap.set(doc.primary, doc);
      docsKeys.push(doc.primary);
      return doc._data;
    });
    this._result = {
      docsData,
      docsKeys,
      docsMap,
      docsDataMap,
      count: docsData.length,
      docs,
      time: now()
    };
  }

  /**
   * executes the query on the database
   * @return results-array with document-data
   */;
  _proto._execOverDatabase = async function _execOverDatabase() {
    this._execOverDatabaseCount = this._execOverDatabaseCount + 1;
    this._lastExecStart = now();
    if (this.op === 'count') {
      // if we have a persisted query cache result, use the result
      if (this._persistentQueryCacheResult) {
        // TODO: correct this number, but how?
        return Number(this._persistentQueryCacheResult);
      }
      var preparedQuery = this.getPreparedQuery();
      var result = await this.collection.storageInstance.count(preparedQuery);
      if (result.mode === 'slow' && !this.collection.database.allowSlowCount) {
        throw newRxError('QU14', {
          collection: this.collection,
          queryObj: this.mangoQuery
        });
      } else {
        return result.count;
      }
    }
    if (this.op === 'findByIds') {
      var ids = ensureNotFalsy(this.mangoQuery.selector)[this.collection.schema.primaryPath].$in;
      var ret = new Map();
      var mustBeQueried = [];
      // first try to fill from docCache
      ids.forEach(id => {
        var docData = this.collection._docCache.getLatestDocumentDataIfExists(id);
        if (docData) {
          if (!docData._deleted) {
            var doc = this.collection._docCache.getCachedRxDocument(docData);
            ret.set(id, doc);
          }
        } else {
          mustBeQueried.push(id);
        }
      });
      // everything which was not in docCache must be fetched from the storage
      if (mustBeQueried.length > 0) {
        var docs = await this.collection.storageInstance.findDocumentsById(mustBeQueried, false);
        Object.values(docs).forEach(docData => {
          var doc = this.collection._docCache.getCachedRxDocument(docData);
          ret.set(doc.primary, doc);
        });
      }
      return ret;
    }
    var docsPromise = queryCollection(this);
    return docsPromise.then(docs => {
      this._lastExecEnd = now();
      return docs;
    });
  }

  /**
   * Execute the query
   * To have an easier implementations,
   * just subscribe and use the first result
   */;
  _proto.exec = function exec(throwIfMissing) {
    if (throwIfMissing && this.op !== 'findOne') {
      throw newRxError('QU9', {
        collection: this.collection.name,
        query: this.mangoQuery,
        op: this.op
      });
    }

    /**
     * run _ensureEqual() here,
     * this will make sure that errors in the query which throw inside of the RxStorage,
     * will be thrown at this execution context and not in the background.
     */
    return _ensureEqual(this).then(() => firstValueFrom(this.$)).then(result => {
      if (!result && throwIfMissing) {
        throw newRxError('QU10', {
          collection: this.collection.name,
          query: this.mangoQuery,
          op: this.op
        });
      } else {
        return result;
      }
    });
  }

  /**
   * cached call to get the queryMatcher
   * @overwrites itself with the actual value
   */;
  /**
   * returns a string that is used for equal-comparisons
   * @overwrites itself with the actual value
   */
  _proto.toString = function toString() {
    var stringObj = sortObject({
      op: this.op,
      query: this.mangoQuery,
      other: this.other
    }, true);
    var value = JSON.stringify(stringObj, stringifyFilter);
    this.toString = () => value;
    return value;
  };
  _proto.persistentQueryId = function persistentQueryId() {
    return String(this.collection.database.hashFunction(this.toString()));
  }

  /**
   * returns the prepared query
   * which can be send to the storage instance to query for documents.
   * @overwrites itself with the actual value.
   */;
  _proto.getPreparedQuery = function getPreparedQuery() {
    var hookInput = {
      rxQuery: this,
      // can be mutated by the hooks so we have to deep clone first.
      mangoQuery: normalizeMangoQuery(this.collection.schema.jsonSchema, this.mangoQuery)
    };
    if (this._limitBufferSize !== null && hookInput.mangoQuery.limit) {
      hookInput.mangoQuery.limit = hookInput.mangoQuery.limit + this._limitBufferSize;
    }
    runPluginHooks('prePrepareQuery', hookInput);
    var value = this.collection.database.storage.statics.prepareQuery(this.collection.schema.jsonSchema, hookInput.mangoQuery);
    this.getPreparedQuery = () => value;
    return value;
  }

  /**
   * returns true if the document matches the query,
   * does not use the 'skip' and 'limit'
   */;
  _proto.doesDocumentDataMatch = function doesDocumentDataMatch(docData) {
    // if doc is deleted, it cannot match
    if (docData._deleted) {
      return false;
    }
    return this.queryMatcher(docData);
  }

  /**
   * deletes all found documents
   * @return promise with deleted documents
   */;
  _proto.remove = function remove() {
    return this.exec().then(docs => {
      if (Array.isArray(docs)) {
        // TODO use a bulk operation instead of running .remove() on each document
        return Promise.all(docs.map(doc => doc.remove()));
      } else {
        return docs.remove();
      }
    });
  }

  /**
   * helper function to transform RxQueryBase to RxQuery type
   */;
  /**
   * updates all found documents
   * @overwritten by plugin (optional)
   */
  _proto.update = function update(_updateObj) {
    throw pluginMissing('update');
  }

  // we only set some methods of query-builder here
  // because the others depend on these ones
  ;
  _proto.where = function where(_queryObj) {
    throw pluginMissing('query-builder');
  };
  _proto.sort = function sort(_params) {
    throw pluginMissing('query-builder');
  };
  _proto.skip = function skip(_amount) {
    throw pluginMissing('query-builder');
  };
  _proto.limit = function limit(_amount) {
    throw pluginMissing('query-builder');
  };
  _proto.enableLimitBuffer = function enableLimitBuffer(bufferSize) {
    if (this._limitBufferSize !== null) {
      // Limit buffer has already been enabled, do nothing:
      return this;
    }
    if (this._lastExecStart !== 0) {
      console.error('Can\'t use limit buffer if query has already executed');
      return this;
    }
    if (this.mangoQuery.skip || !this.mangoQuery.limit) {
      console.error('Right now, limit buffer only works on non-skip, limit queries.');
      return this;
    }
    this._limitBufferSize = bufferSize;
    return this;
  };
  _proto.enablePersistentQueryCache = function enablePersistentQueryCache(backend) {
    if (this._persistentQueryCacheBackend) {
      // We've already tried to enable the query cache
      return this;
    }
    this._persistentQueryCacheBackend = backend;
    this._persistentQueryCacheLoaded = this._restoreQueryCacheFromPersistedState();
    return this;
  };
  _proto._restoreQueryCacheFromPersistedState = async function _restoreQueryCacheFromPersistedState() {
    if (!this._persistentQueryCacheBackend) {
      // no cache backend provided, do nothing
      return;
    }
    if (this._persistentQueryCacheResult) {
      // we already restored the cache once, no need to run twice
      return;
    }
    if (this.mangoQuery.skip) {
      console.error('The persistent query cache only works on non-skip queries.');
      return;
    }
    var persistentQueryId = this.persistentQueryId();
    var value = await this._persistentQueryCacheBackend.getItem("qc:" + persistentQueryId);
    if (!value) {
      // eslint-disable-next-line no-console
      console.log("no persistent query cache found in the backend, returning early " + this.toString());
      return;
    }
    // eslint-disable-next-line no-console
    console.time("Restoring persistent querycache " + this.toString());
    var lwt = await this._persistentQueryCacheBackend.getItem("qc:" + persistentQueryId + ":lwt");
    var primaryPath = this.collection.schema.primaryPath;
    this._persistentQueryCacheResult = value ?? undefined;
    this._persistentQueryCacheResultLwt = lwt ?? undefined;

    // if this is a regular query, also load documents into cache
    if (Array.isArray(value) && value.length > 0) {
      var persistedQueryCacheIds = new Set(this._persistentQueryCacheResult);
      var docsData = [];

      // query all docs updated > last persisted, limit to an arbitrary 1_000_000 (10x of what we consider our largest library)
      var {
        documents: changedDocs
      } = await this.collection.storageInstance.getChangedDocumentsSince(1_000_000,
      // make sure we remove the monotonic clock (xxx.01, xxx.02) from the lwt timestamp to avoid issues with
      // lookups in indices (dexie)
      {
        id: '',
        lwt: Math.floor(Number(lwt)) - UPDATE_DRIFT
      });
      for (var changedDoc of changedDocs) {
        var docWasInOldPersistedResults = persistedQueryCacheIds.has(changedDoc[primaryPath]);
        var docMatchesNow = this.doesDocumentDataMatch(changedDoc);
        if (docWasInOldPersistedResults && !docMatchesNow && this.mangoQuery.limit) {
          // Unfortunately if any doc was removed from the results since the last result,
          // there is no way for us to be sure our calculated results are correct.
          // So we should simply give up and re-exec the query.
          this._persistentQueryCacheResult = value ?? undefined;
          this._persistentQueryCacheResultLwt = lwt ?? undefined;
          return;
        }
        if (docWasInOldPersistedResults) {
          /*
          * no need to fetch again, we already got the doc from the list of changed docs, and therefore we filter
          * deleted docs as well
          */
          persistedQueryCacheIds.delete(changedDoc[primaryPath]);
        }

        // ignore deleted docs or docs that do not match the query
        if (!docMatchesNow) {
          continue;
        }

        // add to document cache
        this.collection._docCache.getCachedRxDocument(changedDoc);

        // add to docs
        docsData.push(changedDoc);
      }

      // fetch remaining persisted doc ids
      var nonRestoredDocIds = [];
      for (var docId of persistedQueryCacheIds) {
        // first try to fill from docCache
        var docData = this.collection._docCache.getLatestDocumentDataIfExists(docId);
        if (docData && this.doesDocumentDataMatch(docData)) {
          docsData.push(docData);
        }
        if (!docData) {
          nonRestoredDocIds.push(docId);
        }
      }

      // otherwise get from storage
      if (nonRestoredDocIds.length > 0) {
        var docsMap = await this.collection.storageInstance.findDocumentsById(nonRestoredDocIds, false);
        Object.values(docsMap).forEach(docData => {
          this.collection._docCache.getCachedRxDocument(docData);
          docsData.push(docData);
        });
      }
      var normalizedMangoQuery = normalizeMangoQuery(this.collection.schema.jsonSchema, this.mangoQuery);
      var sortComparator = getSortComparator(this.collection.schema.jsonSchema, normalizedMangoQuery);
      var skip = normalizedMangoQuery.skip ? normalizedMangoQuery.skip : 0;
      var limit = normalizedMangoQuery.limit ? normalizedMangoQuery.limit : Infinity;
      var skipPlusLimit = skip + limit;
      docsData = docsData.sort(sortComparator);
      docsData = docsData.slice(skip, skipPlusLimit);

      // get query into the correct state
      this._lastEnsureEqual = now();
      this._latestChangeEvent = this.collection._changeEventBuffer.counter;
      this._setResultData(docsData);
    } else if (value && Number.isInteger(Number(value))) {
      // get query into the correct state
      this._lastEnsureEqual = now();
      this._latestChangeEvent = this.collection._changeEventBuffer.counter;
      this._setResultData(Number(value));
    }
    // eslint-disable-next-line no-console
    console.timeEnd("Restoring persistent querycache " + this.toString());
  };
  _createClass(RxQueryBase, [{
    key: "$",
    get: function () {
      if (!this._$) {
        var results$ = this.collection.$.pipe(
        /**
         * Performance shortcut.
         * Changes to local documents are not relevant for the query.
         */
        filter(changeEvent => !changeEvent.isLocal),
        /**
         * Start once to ensure the querying also starts
         * when there where no changes.
         */
        startWith(null),
        // ensure query results are up to date.
        mergeMap(() => _ensureEqual(this)),
        // use the current result set, written by _ensureEqual().
        map(() => this._result),
        // do not run stuff above for each new subscriber, only once.
        shareReplay(RXJS_SHARE_REPLAY_DEFAULTS),
        // do not proceed if result set has not changed.
        distinctUntilChanged((prev, curr) => {
          if (prev && prev.time === ensureNotFalsy(curr).time) {
            return true;
          } else {
            return false;
          }
        }), filter(result => !!result),
        /**
         * Map the result set to a single RxDocument or an array,
         * depending on query type
         */
        map(result => {
          var useResult = ensureNotFalsy(result);
          if (this.op === 'count') {
            return useResult.count;
          } else if (this.op === 'findOne') {
            // findOne()-queries emit RxDocument or null
            return useResult.docs.length === 0 ? null : useResult.docs[0];
          } else if (this.op === 'findByIds') {
            return useResult.docsMap;
          } else {
            // find()-queries emit RxDocument[]
            // Flat copy the array so it won't matter if the user modifies it.
            return useResult.docs.slice(0);
          }
        }));
        this._$ = merge(results$,
        /**
         * Also add the refCount$ to the query observable
         * to allow us to count the amount of subscribers.
         */
        this.refCount$.pipe(filter(() => false)));
      }
      return this._$;
    }

    // stores the changeEvent-number of the last handled change-event

    // time stamps on when the last full exec over the database has run
    // used to properly handle events that happen while the find-query is running
    // Fields used for the Limit Buffer when enabled:
    // Fields used for the persistent query cache when enabled:
    // lwt = latest write time
    /**
     * ensures that the exec-runs
     * are not run in parallel
     */
  }, {
    key: "queryMatcher",
    get: function () {
      var schema = this.collection.schema.jsonSchema;
      var normalizedQuery = normalizeMangoQuery(this.collection.schema.jsonSchema, this.mangoQuery);
      return overwriteGetterForCaching(this, 'queryMatcher', getQueryMatcher(schema, normalizedQuery));
    }
  }, {
    key: "asRxQuery",
    get: function () {
      return this;
    }
  }]);
  return RxQueryBase;
}();
export function _getDefaultQuery() {
  return {
    selector: {}
  };
}

/**
 * run this query through the QueryCache
 */
export function tunnelQueryCache(rxQuery) {
  return rxQuery.collection._queryCache.getByQuery(rxQuery);
}
export function createRxQuery(op, queryObj, collection, other) {
  runPluginHooks('preCreateRxQuery', {
    op,
    queryObj,
    collection,
    other
  });
  var ret = new RxQueryBase(op, queryObj, collection, other);

  // ensure when created with same params, only one is created
  ret = tunnelQueryCache(ret);
  // TODO: clear persistent query cache as well
  triggerCacheReplacement(collection);
  return ret;
}

/**
 * Check if the current results-state is in sync with the database
 * which means that no write event happened since the last run.
 * @return false if not which means it should re-execute
 */
function _isResultsInSync(rxQuery) {
  var currentLatestEventNumber = rxQuery.asRxQuery.collection._changeEventBuffer.counter;
  if (rxQuery._latestChangeEvent >= currentLatestEventNumber) {
    return true;
  } else {
    return false;
  }
}

/**
 * wraps __ensureEqual()
 * to ensure it does not run in parallel
 * @return true if has changed, false if not
 */
function _ensureEqual(rxQuery) {
  // Optimisation shortcut
  if (rxQuery.collection.database.destroyed || _isResultsInSync(rxQuery)) {
    return PROMISE_RESOLVE_FALSE;
  }
  rxQuery._ensureEqualQueue = rxQuery._ensureEqualQueue.then(() => __ensureEqual(rxQuery));
  return rxQuery._ensureEqualQueue;
}

/**
 * ensures that the results of this query is equal to the results which a query over the database would give
 * @return true if results have changed
 */
async function __ensureEqual(rxQuery) {
  await rxQuery._persistentQueryCacheLoaded;
  rxQuery._lastEnsureEqual = now();

  /**
   * Optimisation shortcuts
   */
  if (
  // db is closed
  rxQuery.collection.database.destroyed ||
  // nothing happened since last run
  _isResultsInSync(rxQuery)) {
    return PROMISE_RESOLVE_FALSE;
  }
  var ret = false;
  var mustReExec = false; // if this becomes true, a whole execution over the database is made
  if (rxQuery._latestChangeEvent === -1) {
    // have not executed yet -> must run
    mustReExec = true;
  }

  /**
   * try to use EventReduce to calculate the new results
   */
  if (!mustReExec) {
    var missedChangeEvents = rxQuery.asRxQuery.collection._changeEventBuffer.getFrom(rxQuery._latestChangeEvent + 1);
    if (missedChangeEvents === null) {
      // changeEventBuffer is of bounds -> we must re-execute over the database
      mustReExec = true;
    } else {
      rxQuery._latestChangeEvent = rxQuery.asRxQuery.collection._changeEventBuffer.counter;
      var runChangeEvents = rxQuery.asRxQuery.collection._changeEventBuffer.reduceByLastOfDoc(missedChangeEvents);
      if (rxQuery._limitBufferResults !== null) {
        var _loop = async function (cE) {
          if (rxQuery._limitBufferResults.find(doc => doc[rxQuery.collection.schema.primaryPath] === cE.documentId)) {
            // If so, the limit buffer is potential invalid -- let's just blow it up
            // TODO: could we instead update the documents in the limit buffer?
            rxQuery._limitBufferResults = null;
            return 1; // break
          }
        };
        // Check if any item in our limit buffer was modified by a change event
        for (var cE of runChangeEvents) {
          if (await _loop(cE)) break;
        }
      }
      if (rxQuery.op === 'count') {
        // 'count' query
        var previousCount = ensureNotFalsy(rxQuery._result).count;
        var newCount = previousCount;
        runChangeEvents.forEach(cE => {
          var didMatchBefore = cE.previousDocumentData && rxQuery.doesDocumentDataMatch(cE.previousDocumentData);
          var doesMatchNow = rxQuery.doesDocumentDataMatch(cE.documentData);
          if (!didMatchBefore && doesMatchNow) {
            newCount++;
          }
          if (didMatchBefore && !doesMatchNow) {
            newCount--;
          }
        });
        if (newCount !== previousCount) {
          ret = true; // true because results changed
          rxQuery._setResultData(newCount);
          await updatePersistentQueryCache(rxQuery);
        }
      } else {
        // 'find' or 'findOne' query
        var eventReduceResult = calculateNewResults(rxQuery, runChangeEvents);
        if (eventReduceResult.runFullQueryAgain) {
          // could not calculate the new results, execute must be done
          mustReExec = true;
        } else if (eventReduceResult.changed) {
          // we got the new results, we do not have to re-execute, mustReExec stays false
          ret = true; // true because results changed
          rxQuery._setResultData(eventReduceResult.newResults);

          /*
           * We usually want to persist the cache every time there is an update to the query to guarantee
           * correctness. Cache persistence has some "cost", and we therefore try to optimize the number of
           * writes.
           * So, if any item in the result set was removed, we re-persist the query.
          */
          if (rxQuery.mangoQuery.limit && eventReduceResult.limitResultsRemoved) {
            await updatePersistentQueryCache(rxQuery);
          }
        }
      }
    }
  }

  // oh no we have to re-execute the whole query over the database
  if (mustReExec) {
    // counter can change while _execOverDatabase() is running so we save it here
    var latestAfter = rxQuery.collection._changeEventBuffer.counter;
    return rxQuery._execOverDatabase().then(newResultData => {
      rxQuery._latestChangeEvent = latestAfter;

      // A count query needs a different has-changed check.
      if (typeof newResultData === 'number') {
        if (!rxQuery._result || newResultData !== rxQuery._result.count) {
          ret = true;
          rxQuery._setResultData(newResultData);
        }
        return ret;
      }
      if (!rxQuery._result || !areRxDocumentArraysEqual(rxQuery.collection.schema.primaryPath, newResultData, rxQuery._result.docsData)) {
        ret = true; // true because results changed
        rxQuery._setResultData(newResultData);
      }
      return ret;
    }).then(async returnValue => {
      await updatePersistentQueryCache(rxQuery);
      return returnValue;
    });
  }
  return ret; // true if results have changed
}

async function updatePersistentQueryCache(rxQuery) {
  if (!rxQuery._persistentQueryCacheBackend) {
    return;
  }
  var backend = rxQuery._persistentQueryCacheBackend;
  var isCount = rxQuery._result?.docs.length === 0 && rxQuery._result.count > 0;
  var key = rxQuery.persistentQueryId();
  var value = isCount ? rxQuery._result?.count?.toString() ?? '0' : rxQuery._result?.docsKeys ?? [];

  // update _persistedQueryCacheResult
  rxQuery._persistentQueryCacheResult = value;

  // eslint-disable-next-line no-console
  console.time("Query persistence: persisting results of " + JSON.stringify(rxQuery.mangoQuery));
  // persist query cache
  var lwt = rxQuery._result?.time ?? RX_META_LWT_MINIMUM;
  await backend.setItem("qc:" + String(key), value);
  await backend.setItem("qc:" + String(key) + ":lwt", lwt.toString());

  // eslint-disable-next-line no-console
  console.timeEnd("Query persistence: persisting results of " + JSON.stringify(rxQuery.mangoQuery));
}

// Refactored out of `queryCollection`: modifies the docResults array to fill it with data
async function _queryCollectionByIds(rxQuery, docResults, docIds) {
  var collection = rxQuery.collection;
  docIds = docIds.filter(docId => {
    // first try to fill from docCache
    var docData = rxQuery.collection._docCache.getLatestDocumentDataIfExists(docId);
    if (docData) {
      if (!docData._deleted) {
        docResults.push(docData);
      }
      return false;
    } else {
      return true;
    }
  });

  // otherwise get from storage
  if (docIds.length > 0) {
    var docsMap = await collection.storageInstance.findDocumentsById(docIds, false);
    Object.values(docsMap).forEach(docData => {
      docResults.push(docData);
    });
  }
}

/**
 * Runs the query over the storage instance
 * of the collection.
 * Does some optimizations to ensure findById is used
 * when specific queries are used.
 */
export async function queryCollection(rxQuery) {
  await rxQuery._persistentQueryCacheLoaded;
  var docs = [];
  var collection = rxQuery.collection;

  /**
   * Optimizations shortcut.
   * If query is find-one-document-by-id,
   * then we do not have to use the slow query() method
   * but instead can use findDocumentsById()
   */
  if (rxQuery.isFindOneByIdQuery) {
    if (Array.isArray(rxQuery.isFindOneByIdQuery)) {
      await _queryCollectionByIds(rxQuery, docs, rxQuery.isFindOneByIdQuery);
    } else {
      var docId = rxQuery.isFindOneByIdQuery;

      // first try to fill from docCache
      var docData = rxQuery.collection._docCache.getLatestDocumentDataIfExists(docId);
      if (!docData) {
        // otherwise get from storage
        var docsMap = await collection.storageInstance.findDocumentsById([docId], false);
        if (docsMap.hasOwnProperty(docId)) {
          docData = docsMap[docId];
        }
      }
      if (docData && !docData._deleted) {
        docs.push(docData);
      }
    }
  } else {
    var preparedQuery = rxQuery.getPreparedQuery();
    var queryResult = await collection.storageInstance.query(preparedQuery);
    if (rxQuery._limitBufferSize !== null && rxQuery.mangoQuery.limit && queryResult.documents.length > rxQuery.mangoQuery.limit) {
      // If there are more than query.limit results, we pull out our buffer items from the
      // last rxQuery._limitBufferSize items of the results.
      rxQuery._limitBufferResults = queryResult.documents.splice(rxQuery.mangoQuery.limit);
    }
    docs = queryResult.documents;
  }
  return docs;
}

/**
 * Returns true if the given query
 * selects exactly one document by its id.
 * Used to optimize performance because these kind of
 * queries do not have to run over an index and can use get-by-id instead.
 * Returns false if no query of that kind.
 * Returns the document id otherwise.
 */
export function isFindOneByIdQuery(primaryPath, query) {
  // must have exactly one operator which must be $eq || $in
  if (!query.skip && query.selector && Object.keys(query.selector).length === 1 && query.selector[primaryPath]) {
    var _value = query.selector[primaryPath];
    if (typeof _value === 'string') {
      return _value;
    } else if (Object.keys(_value).length === 1 && typeof _value.$eq === 'string') {
      return _value.$eq;
    }

    // same with $in string arrays
    if (Object.keys(_value).length === 1 && Array.isArray(_value.$eq) &&
    // must only contain strings
    !_value.$eq.find(r => typeof r !== 'string')) {
      return _value.$eq;
    }
  }
  return false;
}
export function isRxQuery(obj) {
  return obj instanceof RxQueryBase;
}
//# sourceMappingURL=rx-query.js.map
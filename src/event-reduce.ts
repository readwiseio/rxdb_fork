import {
    ActionName,
    calculateActionName,
    runAction,
    QueryParams,
    QueryMatcher,
    DeterministicSortComparator,
    StateResolveFunctionInput,
    ChangeEvent,
    hasLimit,
    isUpdate,
    isDelete,
    isFindOne,
    isInsert,
    hasSkip,
    wasResultsEmpty,
    wasInResult,
    wasSortedAfterLast,
    wasLimitReached,
    wasMatching,
    doesMatchNow,
} from 'event-reduce-js';
import type {
    RxQuery,
    MangoQuery,
    RxChangeEvent,
    StringKeys,
    RxDocumentData
} from './types/index.d.ts';
import { rxChangeEventToEventReduceChangeEvent } from './rx-change-event.ts';
import {
    arrayFilterNotEmpty,
    clone,
    ensureNotFalsy,
    getFromMapOrCreate
} from './plugins/utils/index.ts';
import { getQueryMatcher, getSortComparator, normalizeMangoQuery } from './rx-query-helper.ts';

export type EventReduceResultNeg = {
    runFullQueryAgain: true;
};
export type EventReduceResultPos<RxDocumentType> = {
    runFullQueryAgain: false;
    changed: boolean;
    newResults: RxDocumentType[];
    limitResultsRemoved: boolean;
};
export type EventReduceResult<RxDocumentType> = EventReduceResultNeg | EventReduceResultPos<RxDocumentType>;


export function getSortFieldsOfQuery<RxDocType>(
    primaryKey: StringKeys<RxDocumentData<RxDocType>>,
    query: MangoQuery<RxDocType>
): (string | StringKeys<RxDocType>)[] {
    if (!query.sort || query.sort.length === 0) {
        return [primaryKey];
    } else {
        return query.sort.map(part => Object.keys(part)[0]);
    }
}



export const RXQUERY_QUERY_PARAMS_CACHE: WeakMap<RxQuery, QueryParams<any>> = new WeakMap();
export function getQueryParams<RxDocType>(
    rxQuery: RxQuery<RxDocType>
): QueryParams<RxDocType> {
    return getFromMapOrCreate(
        RXQUERY_QUERY_PARAMS_CACHE,
        rxQuery,
        () => {
            const collection = rxQuery.collection;
            const normalizedMangoQuery = normalizeMangoQuery(
                collection.storageInstance.schema,
                clone(rxQuery.mangoQuery)
            );
            const primaryKey = collection.schema.primaryPath;

            /**
             * Create a custom sort comparator
             * that uses the hooks to ensure
             * we send for example compressed documents to be sorted by compressed queries.
             */
            const sortComparator = getSortComparator(
                collection.schema.jsonSchema,
                normalizedMangoQuery
            );

            const useSortComparator: DeterministicSortComparator<RxDocType> = (docA: RxDocType, docB: RxDocType) => {
                const sortComparatorData = {
                    docA,
                    docB,
                    rxQuery
                };
                return sortComparator(sortComparatorData.docA, sortComparatorData.docB);
            };

            /**
             * Create a custom query matcher
             * that uses the hooks to ensure
             * we send for example compressed documents to match compressed queries.
             */
            const queryMatcher = getQueryMatcher(
                collection.schema.jsonSchema,
                normalizedMangoQuery
            );
            const useQueryMatcher: QueryMatcher<RxDocumentData<RxDocType>> = (doc: RxDocumentData<RxDocType>) => {
                const queryMatcherData = {
                    doc,
                    rxQuery
                };
                return queryMatcher(queryMatcherData.doc);
            };

            const ret: QueryParams<any> = {
                primaryKey: rxQuery.collection.schema.primaryPath as any,
                skip: normalizedMangoQuery.skip,
                limit: normalizedMangoQuery.limit,
                sortFields: getSortFieldsOfQuery(primaryKey, normalizedMangoQuery) as string[],
                sortComparator: useSortComparator,
                queryMatcher: useQueryMatcher
            };
            return ret;
        }
    );
}

// This catches a specific case where we have a limit query (of say LIMIT items), and then
// a document is removed from the result set by the current change. In this case,
// the event-reduce library (rightly) tells us we need to recompute the query to get a
// full result set of LIMIT items.
// However, if we have a "limit buffer", we can instead fill in the missing result from there.
// For more info, see the rx-query.test tests under "Limit Buffer".
// This function checks if we are actually in the specific case where the limit buffer can be used.
function canFillResultSetFromLimitBuffer<RxDocumentType>(s: StateResolveFunctionInput<RxDocumentType>) {
    // We figure out if this event is our special case using the same "state resolve" functions that event-reduce uses:
    // https://github.com/pubkey/event-reduce/blob/fcb46947b29eac97c97dcb05e08af337f362fe5c/javascript/src/states/index.ts#L87
    // (we also keep the state resolve functions in the same order they're defined in event-reduce.js)
    return (
        !isInsert(s) && // inserts can never cause
        (isUpdate(s) || isDelete(s)) && // both updates and deletes can remove a doc from our results
        hasLimit(s) && // only limit queries
        !isFindOne(s) && // if it's a findOne, we have no buffer and have to re-compute
        !hasSkip(s) && // we could potentially make skip queries work later, but for now ignore them -- too hard
        !wasResultsEmpty(s) && // this should never happen
        wasLimitReached(s) && // if not, the event reducer shouldn't have a problem
        // any value of wasFirst(s), position is not relevant for this case, as wasInResults
        // any value of wasLast(s) , position is not relevant for this case, as wasInResults
        // any value of sortParamsChanged(s), eg a doc could be archived but also have last_status_update changed
        wasInResult(s) && // we only care about docs already in the results set being removed
        // any value of wasSortedBeforeFirst(s) -- this is true when the doc is first in the results set
        !wasSortedAfterLast(s) && // I don't think this could be true anyways, but whatever
        // any value of isSortedBeforeFirst(s) -- this is true when the doc is first in order (but it could still be filtered out)
        // any value of isSortedAfterLast(s)
        wasMatching(s) && // it couldn't have been wasInResult unless it was also matching
        !doesMatchNow(s) // Limit buffer only cares rn when the changed doc was indeed removed (so no longer matching)
    );
}


function actionRemovesItemFromResults(action: ActionName): boolean {
    return [
        'removeFirstItem',
        'removeLastItem',
        'removeExisting',
        'runFullQueryAgain',
    ].includes(action);
}


export function calculateNewResults<RxDocumentType>(
    rxQuery: RxQuery<RxDocumentType>,
    rxChangeEvents: RxChangeEvent<RxDocumentType>[]
): EventReduceResult<RxDocumentType> {
    if (!rxQuery.collection.database.eventReduce) {
        return {
            runFullQueryAgain: true
        };
    }
    const queryParams = getQueryParams(rxQuery);
    const previousResults: RxDocumentType[] = ensureNotFalsy(rxQuery._result).docsData.slice(0);
    const previousResultsMap: Map<string, RxDocumentType> = ensureNotFalsy(rxQuery._result).docsDataMap;
    let changed: boolean = false;
    let limitResultsRemoved: boolean = false;

    const eventReduceEvents: ChangeEvent<RxDocumentType>[] = rxChangeEvents
        .map(cE => rxChangeEventToEventReduceChangeEvent(cE))
        .filter(arrayFilterNotEmpty);

    const foundNonOptimizeable = eventReduceEvents.find(eventReduceEvent => {
        const stateResolveFunctionInput: StateResolveFunctionInput<RxDocumentType> = {
            queryParams,
            changeEvent: eventReduceEvent,
            previousResults,
            keyDocumentMap: previousResultsMap
        };

        const actionName: ActionName = calculateActionName(stateResolveFunctionInput);

        if (actionName === 'runFullQueryAgain') {
            if (canFillResultSetFromLimitBuffer(stateResolveFunctionInput) && rxQuery._limitBufferResults !== null && rxQuery._limitBufferResults.length > 0) {
                // replace the missing item with an item from our limit buffer!
                const replacementItem = rxQuery._limitBufferResults.shift();
                if (replacementItem === undefined) {
                    return true;
                }

                changed = true;
                runAction(
                    'removeExisting',
                    queryParams,
                    eventReduceEvent,
                    previousResults,
                    previousResultsMap,
                );
                previousResults.push(replacementItem);
                if (previousResultsMap) {
                    // We have to assume the primaryKey value is a string. According to the rxdb docs, this is always the case:
                    // https://github.com/pubkey/rxdb/blob/c8162c25c7b033fa9f70191512ee84d44d0dd913/docs/rx-schema.html#L2523
                    previousResultsMap.set(replacementItem[rxQuery.collection.schema.primaryPath] as string, replacementItem);
                }
                return false;
            }
            return true;
        } else if (actionName !== 'doNothing') {
            changed = true;
            runAction(
                actionName,
                queryParams,
                eventReduceEvent,
                previousResults,
                previousResultsMap
            );
            if (actionRemovesItemFromResults(actionName)) {
                limitResultsRemoved = true;
            }
            return false;
        }
    });
    if (foundNonOptimizeable) {
        return {
            runFullQueryAgain: true,
        };
    } else {
        return {
            runFullQueryAgain: false,
            changed,
            newResults: previousResults,
            limitResultsRemoved,
        };
    }
}

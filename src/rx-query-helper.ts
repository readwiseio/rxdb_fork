import { LOGICAL_OPERATORS } from './query-planner.ts';
import { getPrimaryFieldOfPrimaryKey } from './rx-schema-helper.ts';
import type {
    DeepReadonly,
    DeterministicSortComparator,
    FilledMangoQuery,
    MangoQuery,
    MangoQueryOperators,
    MangoQuerySelector,
    MangoQuerySortDirection,
    PropertyType,
    QueryMatcher,
    RxDocument,
    RxDocumentData,
    RxJsonSchema,
    RxQuery
} from './types/index.d.ts';
import {
    clone,
    firstPropertyNameOfObject,
    toArray,
    isMaybeReadonlyArray,
    flatClone,
    objectPathMonad,
    ObjectPathMonadFunction
} from './plugins/utils/index.ts';
import {
    compare as mingoSortComparator
} from 'mingo/util';
import { newRxError } from './rx-error.ts';
import { getMingoQuery } from './rx-query-mingo.ts';

/**
 * Normalize the query to ensure we have all fields set
 * and queries that represent the same query logic are detected as equal by the caching.
 */
export function normalizeMangoQuery<RxDocType>(
    schema: RxJsonSchema<RxDocumentData<RxDocType>>,
    mangoQuery: MangoQuery<RxDocType>
): FilledMangoQuery<RxDocType> {
    const primaryKey: string = getPrimaryFieldOfPrimaryKey(schema.primaryKey);
    mangoQuery = flatClone(mangoQuery);

    const normalizedMangoQuery: FilledMangoQuery<RxDocType> = clone(mangoQuery) as any;
    if (typeof normalizedMangoQuery.skip !== 'number') {
        normalizedMangoQuery.skip = 0;
    }

    if (!normalizedMangoQuery.selector) {
        normalizedMangoQuery.selector = {};
    } else {
        normalizedMangoQuery.selector = normalizedMangoQuery.selector;
        /**
         * In mango query, it is possible to have an
         * equals comparison by directly assigning a value
         * to a property, without the '$eq' operator.
         * Like:
         * selector: {
         *   foo: 'bar'
         * }
         * For normalization, we have to normalize this
         * so our checks can perform properly.
         *
         *
         * TODO this must work recursive with nested queries that
         * contain multiple selectors via $and or $or etc.
         */
        Object
            .entries(normalizedMangoQuery.selector)
            .forEach(([field, matcher]) => {
                if (typeof matcher !== 'object' || matcher === null) {
                    (normalizedMangoQuery as any).selector[field] = {
                        $eq: matcher
                    };
                }
            });
    }

    /**
     * Ensure that if an index is specified,
     * the primaryKey is inside of it.
     */
    if (normalizedMangoQuery.index) {
        const indexAr = toArray(normalizedMangoQuery.index);
        if (!indexAr.includes(primaryKey)) {
            indexAr.push(primaryKey);
        }
        normalizedMangoQuery.index = indexAr;
    }

    /**
     * To ensure a deterministic sorting,
     * we have to ensure the primary key is always part
     * of the sort query.
     * Primary sorting is added as last sort parameter,
     * similar to how we add the primary key to indexes that do not have it.
     *
     */
    if (!normalizedMangoQuery.sort) {
        /**
         * If no sort is given at all,
         * we can assume that the user does not care about sort order at al.
         *
         * we cannot just use the primary key as sort parameter
         * because it would likely cause the query to run over the primary key index
         * which has a bad performance in most cases.
         */
        if (normalizedMangoQuery.index) {
            normalizedMangoQuery.sort = normalizedMangoQuery.index.map((field: string) => {
                return { [field as any]: 'asc' } as any;
            });
        } else {
            /**
             * Find the index that best matches the fields with the logical operators
             */
            if (schema.indexes) {
                const fieldsWithLogicalOperator: Set<string> = new Set();
                Object.entries(normalizedMangoQuery.selector).forEach(([field, matcher]) => {
                    let hasLogical = false;
                    if (typeof matcher === 'object' && matcher !== null) {
                        hasLogical = !!Object.keys(matcher).find(operator => LOGICAL_OPERATORS.has(operator));
                    } else {
                        hasLogical = true;
                    }
                    if (hasLogical) {
                        fieldsWithLogicalOperator.add(field);
                    }
                });


                let currentFieldsAmount = -1;
                let currentBestIndexForSort: string[] | readonly string[] | undefined;
                schema.indexes.forEach(index => {
                    const useIndex = isMaybeReadonlyArray(index) ? index : [index];
                    const firstWrongIndex = useIndex.findIndex(indexField => !fieldsWithLogicalOperator.has(indexField));
                    if (
                        firstWrongIndex > 0 &&
                        firstWrongIndex > currentFieldsAmount
                    ) {
                        currentFieldsAmount = firstWrongIndex;
                        currentBestIndexForSort = useIndex;
                    }
                });
                if (currentBestIndexForSort) {
                    normalizedMangoQuery.sort = currentBestIndexForSort.map((field: string) => {
                        return { [field as any]: 'asc' } as any;
                    });
                }

            }

            /**
             * Fall back to the primary key as sort order
             * if no better one has been found
             */
            if (!normalizedMangoQuery.sort) {
                normalizedMangoQuery.sort = [{ [primaryKey]: 'asc' }] as any;
            }
        }
    } else {
        const isPrimaryInSort = normalizedMangoQuery.sort
            .find(p => firstPropertyNameOfObject(p) === primaryKey);
        if (!isPrimaryInSort) {
            normalizedMangoQuery.sort = normalizedMangoQuery.sort.slice(0);
            normalizedMangoQuery.sort.push({ [primaryKey]: 'asc' } as any);
        }
    }

    return normalizedMangoQuery;
}

/**
 * Returns the sort-comparator,
 * which is able to sort documents in the same way
 * a query over the db would do.
 */
export function getSortComparator<RxDocType>(
    schema: RxJsonSchema<RxDocumentData<RxDocType>>,
    query: FilledMangoQuery<RxDocType>
): DeterministicSortComparator<RxDocType> {
    if (!query.sort) {
        throw newRxError('SNH', { query });
    }
    const sortParts: {
        key: string;
        direction: MangoQuerySortDirection;
        getValueFn: ObjectPathMonadFunction<RxDocType>;
    }[] = [];
    query.sort.forEach(sortBlock => {
        const key = Object.keys(sortBlock)[0];
        const direction = Object.values(sortBlock)[0];
        sortParts.push({
            key,
            direction,
            getValueFn: objectPathMonad(key)
        });
    });
    const fun: DeterministicSortComparator<RxDocType> = (a: RxDocType, b: RxDocType) => {
        for (let i = 0; i < sortParts.length; ++i) {
            const sortPart = sortParts[i];
            const valueA = sortPart.getValueFn(a);
            const valueB = sortPart.getValueFn(b);
            if (valueA !== valueB) {
                const ret = sortPart.direction === 'asc' ? mingoSortComparator(valueA, valueB) : mingoSortComparator(valueB, valueA);
                return ret as any;
            }
        }
    };

    return fun;
}

// When the rxdb adapter is sqlite, we escape tag names by wrapping them in quotes:
//  https://github.com/TristanH/rekindled/blob/e42d2fa40305ba98dfab12e8dd7aed07ddb17ebf/reading-clients/shared/database/queryHelpers.ts#L18
//
// Although sqlite can match tag keys escaped with quotes, the rxdb event-reduce query matcher (mingo) says that a document like
// {
//     'tags': 'a': {...}}
// }
// does not match the query:
// {"tags.\"a\"":{"$exists":1}}
//
// What this function does is "fix" the sqlite queries to unescape the tag keys (basically, remove the quotes).
// so that the event reduce library can work properly and not remove tagged documents from results.
function unescapeTagKeysInSelector(query: any): any {
    if (typeof query === 'object' && query !== null) {
        const newQuery: any = Array.isArray(query) ? [] : {};
        // loop through all keys of the object
        for (const key in query) {
            // eslint-disable-next-line no-prototype-builtins
            if (query.hasOwnProperty(key)) {
                // check if key matches the pattern "tags.\"x\""
                let newKey = key;
                if (key.startsWith('tags.') && key.includes('"')) {
                    newKey = key.replace(/"/g, '');
                }
                // recursively process the value
                newQuery[newKey] = unescapeTagKeysInSelector(query[key]);
            }
        }
        return newQuery;
    }
    return query;
}


/**
 * Returns a function
 * that can be used to check if a document
 * matches the query.
 */
export function getQueryMatcher<RxDocType>(
    _schema: RxJsonSchema<RxDocType> | RxJsonSchema<RxDocumentData<RxDocType>>,
    query: FilledMangoQuery<RxDocType>
): QueryMatcher<RxDocumentData<RxDocType>> {
    if (!query.sort) {
        throw newRxError('SNH', { query });
    }

    const mingoQuery = getMingoQuery(unescapeTagKeysInSelector(query.selector as any));

    const fun: QueryMatcher<RxDocumentData<RxDocType>> = (doc: RxDocumentData<RxDocType> | DeepReadonly<RxDocumentData<RxDocType>>) => {
        return mingoQuery.test(doc);
    };
    return fun;
}


export async function runQueryUpdateFunction<RxDocType, RxQueryResult>(
    rxQuery: RxQuery<RxDocType, RxQueryResult>,
    fn: (doc: RxDocument<RxDocType>) => Promise<RxDocument<RxDocType>>
): Promise<RxQueryResult> {
    const docs = await rxQuery.exec();
    if (!docs) {
        // only findOne() queries can return null
        return null as any;
    }
    if (Array.isArray(docs)) {
        return Promise.all(
            docs.map(doc => fn(doc))
        ) as any;
    } else {
        // via findOne()
        const result = await fn(docs as any);
        return result as any;
    }
}

/**
 * Checks if a given selector includes deleted documents.
 * @param selector The MangoQuerySelector to check
 * @returns True if the selector includes deleted documents, false otherwise
 */
export function selectorIncludesDeleted<RxDocType>(
    selector: MangoQuerySelector<RxDocType> | undefined
): boolean {
    if (!selector) {
        return false;
    }

    const isTrue = (value: unknown): boolean =>
        value === true ||
        (typeof value === 'object' &&
            value !== null &&
            '$eq' in value &&
            (value as MangoQueryOperators<boolean>).$eq === true);


    const isNotFalse = (value: unknown): boolean =>
        value === true ||
        (typeof value === 'object' &&
            value !== null &&
            '$ne' in value &&
            (value as MangoQueryOperators<boolean>).$ne === false);

    const hasDeletedTrue = (
        condition: MangoQuerySelector<RxDocType>
    ): boolean =>
        '_deleted' in condition &&
        (isTrue(condition._deleted as PropertyType<RxDocType, '_deleted'>) ||
            isNotFalse(
                condition._deleted as PropertyType<RxDocType, '_deleted'>
            ));

    if ('_deleted' in selector) {
        return (
            isTrue(selector._deleted as PropertyType<RxDocType, '_deleted'>) ||
            isNotFalse(selector._deleted as PropertyType<RxDocType, '_deleted'>)
        );
    }

    if ('$or' in selector && Array.isArray(selector.$or)) {
        return selector.$or.some(hasDeletedTrue);
    }

    if ('$and' in selector && Array.isArray(selector.$and)) {
        return selector.$and.some(hasDeletedTrue);
    }

    if ('$nor' in selector && Array.isArray(selector.$nor)) {
        return !selector.$nor.every((condition) => !hasDeletedTrue(condition));
    }

    return false;
}

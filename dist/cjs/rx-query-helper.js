"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getQueryMatcher = getQueryMatcher;
exports.getSortComparator = getSortComparator;
exports.normalizeMangoQuery = normalizeMangoQuery;
exports.runQueryUpdateFunction = runQueryUpdateFunction;
exports.selectorIncludesDeleted = selectorIncludesDeleted;
var _queryPlanner = require("./query-planner.js");
var _rxSchemaHelper = require("./rx-schema-helper.js");
var _index = require("./plugins/utils/index.js");
var _util = require("mingo/util");
var _rxError = require("./rx-error.js");
var _rxQueryMingo = require("./rx-query-mingo.js");
/**
 * Normalize the query to ensure we have all fields set
 * and queries that represent the same query logic are detected as equal by the caching.
 */
function normalizeMangoQuery(schema, mangoQuery) {
  var primaryKey = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(schema.primaryKey);
  mangoQuery = (0, _index.flatClone)(mangoQuery);
  var normalizedMangoQuery = (0, _index.clone)(mangoQuery);
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
    Object.entries(normalizedMangoQuery.selector).forEach(([field, matcher]) => {
      if (typeof matcher !== 'object' || matcher === null) {
        normalizedMangoQuery.selector[field] = {
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
    var indexAr = (0, _index.toArray)(normalizedMangoQuery.index);
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
      normalizedMangoQuery.sort = normalizedMangoQuery.index.map(field => {
        return {
          [field]: 'asc'
        };
      });
    } else {
      /**
       * Find the index that best matches the fields with the logical operators
       */
      if (schema.indexes) {
        var fieldsWithLogicalOperator = new Set();
        Object.entries(normalizedMangoQuery.selector).forEach(([field, matcher]) => {
          var hasLogical = false;
          if (typeof matcher === 'object' && matcher !== null) {
            hasLogical = !!Object.keys(matcher).find(operator => _queryPlanner.LOGICAL_OPERATORS.has(operator));
          } else {
            hasLogical = true;
          }
          if (hasLogical) {
            fieldsWithLogicalOperator.add(field);
          }
        });
        var currentFieldsAmount = -1;
        var currentBestIndexForSort;
        schema.indexes.forEach(index => {
          var useIndex = (0, _index.isMaybeReadonlyArray)(index) ? index : [index];
          var firstWrongIndex = useIndex.findIndex(indexField => !fieldsWithLogicalOperator.has(indexField));
          if (firstWrongIndex > 0 && firstWrongIndex > currentFieldsAmount) {
            currentFieldsAmount = firstWrongIndex;
            currentBestIndexForSort = useIndex;
          }
        });
        if (currentBestIndexForSort) {
          normalizedMangoQuery.sort = currentBestIndexForSort.map(field => {
            return {
              [field]: 'asc'
            };
          });
        }
      }

      /**
       * Fall back to the primary key as sort order
       * if no better one has been found
       */
      if (!normalizedMangoQuery.sort) {
        normalizedMangoQuery.sort = [{
          [primaryKey]: 'asc'
        }];
      }
    }
  } else {
    var isPrimaryInSort = normalizedMangoQuery.sort.find(p => (0, _index.firstPropertyNameOfObject)(p) === primaryKey);
    if (!isPrimaryInSort) {
      normalizedMangoQuery.sort = normalizedMangoQuery.sort.slice(0);
      normalizedMangoQuery.sort.push({
        [primaryKey]: 'asc'
      });
    }
  }
  return normalizedMangoQuery;
}

/**
 * Returns the sort-comparator,
 * which is able to sort documents in the same way
 * a query over the db would do.
 */
function getSortComparator(schema, query) {
  if (!query.sort) {
    throw (0, _rxError.newRxError)('SNH', {
      query
    });
  }
  var sortParts = [];
  query.sort.forEach(sortBlock => {
    var key = Object.keys(sortBlock)[0];
    var direction = Object.values(sortBlock)[0];
    sortParts.push({
      key,
      direction,
      getValueFn: (0, _index.objectPathMonad)(key)
    });
  });
  var fun = (a, b) => {
    for (var i = 0; i < sortParts.length; ++i) {
      var sortPart = sortParts[i];
      var valueA = sortPart.getValueFn(a);
      var valueB = sortPart.getValueFn(b);
      if (valueA !== valueB) {
        var ret = sortPart.direction === 'asc' ? (0, _util.compare)(valueA, valueB) : (0, _util.compare)(valueB, valueA);
        return ret;
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
function unescapeTagKeysInSelector(query) {
  if (typeof query === 'object' && query !== null) {
    var newQuery = Array.isArray(query) ? [] : {};
    // loop through all keys of the object
    for (var key in query) {
      // eslint-disable-next-line no-prototype-builtins
      if (query.hasOwnProperty(key)) {
        // check if key matches the pattern "tags.\"x\""
        var newKey = key;
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
function getQueryMatcher(_schema, query) {
  if (!query.sort) {
    throw (0, _rxError.newRxError)('SNH', {
      query
    });
  }
  var mingoQuery = (0, _rxQueryMingo.getMingoQuery)(unescapeTagKeysInSelector(query.selector));
  var fun = doc => {
    return mingoQuery.test(doc);
  };
  return fun;
}
async function runQueryUpdateFunction(rxQuery, fn) {
  var docs = await rxQuery.exec();
  if (!docs) {
    // only findOne() queries can return null
    return null;
  }
  if (Array.isArray(docs)) {
    return Promise.all(docs.map(doc => fn(doc)));
  } else {
    // via findOne()
    var result = await fn(docs);
    return result;
  }
}

/**
 * Checks if a given selector includes deleted documents.
 * @param selector The MangoQuerySelector to check
 * @returns True if the selector includes deleted documents, false otherwise
 */
function selectorIncludesDeleted(selector) {
  if (!selector) {
    return false;
  }
  var isTrue = value => value === true || typeof value === 'object' && value !== null && '$eq' in value && value.$eq === true;
  var isNotFalse = value => value === true || typeof value === 'object' && value !== null && '$ne' in value && value.$ne === false;
  var hasDeletedTrue = condition => '_deleted' in condition && (isTrue(condition._deleted) || isNotFalse(condition._deleted));
  if ('_deleted' in selector) {
    return isTrue(selector._deleted) || isNotFalse(selector._deleted);
  }
  if ('$or' in selector && Array.isArray(selector.$or)) {
    return selector.$or.some(hasDeletedTrue);
  }
  if ('$and' in selector && Array.isArray(selector.$and)) {
    return selector.$and.some(hasDeletedTrue);
  }
  if ('$nor' in selector && Array.isArray(selector.$nor)) {
    return !selector.$nor.every(condition => !hasDeletedTrue(condition));
  }
  return false;
}
//# sourceMappingURL=rx-query-helper.js.map
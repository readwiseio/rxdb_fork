import {QueryCacheBackend, RxCollection} from '../../src';

export class Cache implements QueryCacheBackend {
    private readonly items;

    constructor() {
      this.items = new Map();
    }

    getItem(key: string) {
        return this.items.get(key);
    }

    async setItem<T extends string | string[]>(key: string, value: T) {
        this.items.set(key, value);
        return await Promise.resolve(value);
    }

    get size() {
        return this.items.size;
    }

    getItems() {
        return this.items;
    }
}

export function clearQueryCache(collection: RxCollection<any, any, any>) {
  const queryCache = collection._queryCache;
  queryCache._map = new Map();
}

package com.jnks.iot.server.cache;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import com.jnks.iot.server.common.data.HasVersion;
import com.jnks.iot.server.common.data.util.JnksIotPair;

import java.io.Serializable;

public abstract class VersionedCaffeineJnksIotCache<K extends VersionedCacheKey, V extends Serializable & HasVersion> extends CaffeineJnksIotTransactionalCache<K, V> implements VersionedJnksIotCache<K, V> {

    public VersionedCaffeineJnksIotCache(CacheManager cacheManager, String cacheName) {
        super(cacheManager, cacheName);
    }

    @Override
    public JnksIotCacheValueWrapper<V> get(K key) {
        JnksIotPair<Long, V> versionValuePair = doGet(key);
        if (versionValuePair != null) {
            return SimpleJnksIotCacheValueWrapper.wrap(versionValuePair.getSecond());
        }
        return null;
    }

    @Override
    public void put(K key, V value) {
        Long version = getVersion(value);
        if (version == null) {
            return;
        }
        doPut(key, value, version);
    }

    private void doPut(K key, V value, Long version) {
        lock.lock();
        try {
            JnksIotPair<Long, V> versionValuePair = doGet(key);
            if (versionValuePair == null || version > versionValuePair.getFirst()) {
                failAllTransactionsByKey(key);
                cache.put(key, wrapValue(value, version));
            }
        } finally {
            lock.unlock();
        }
    }

    private JnksIotPair<Long, V> doGet(K key) {
        Cache.ValueWrapper source = cache.get(key);
        return source == null ? null : (JnksIotPair<Long, V>) source.get();
    }

    @Override
    public void evict(K key) {
        lock.lock();
        try {
            failAllTransactionsByKey(key);
            cache.evict(key);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void evict(K key, Long version) {
        if (version == null) {
            return;
        }
        doPut(key, null, version);
    }

    @Override
    void doPutIfAbsent(K key, V value) {
        cache.putIfAbsent(key, wrapValue(value, getVersion(value)));
    }

    private JnksIotPair<Long, V> wrapValue(V value, Long version) {
        return JnksIotPair.of(version, value);
    }

}

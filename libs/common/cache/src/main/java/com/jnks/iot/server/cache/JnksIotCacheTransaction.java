package com.jnks.iot.server.cache;

public interface JnksIotCacheTransaction<K, V> {

    void put(K key, V value);

    boolean commit();

    void rollback();

}

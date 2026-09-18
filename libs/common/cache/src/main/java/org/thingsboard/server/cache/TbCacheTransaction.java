package org.thingsboard.server.cache;

public interface TbCacheTransaction<K, V> {

    void put(K key, V value);

    boolean commit();

    void rollback();

}

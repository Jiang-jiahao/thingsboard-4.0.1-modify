package com.jnks.iot.server.dao.entity;

import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.server.cache.VersionedCacheKey;
import com.jnks.iot.server.cache.VersionedJnksIotCache;
import com.jnks.iot.server.common.data.HasVersion;

import java.io.Serializable;

public abstract class CachedVersionedEntityService<K extends VersionedCacheKey, V extends Serializable & HasVersion, E> extends AbstractCachedEntityService<K, V, E> {

    @Autowired
    protected VersionedJnksIotCache<K, V> cache;

}

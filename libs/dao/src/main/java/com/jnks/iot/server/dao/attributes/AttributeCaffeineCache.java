package com.jnks.iot.server.dao.attributes;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.VersionedCaffeineJnksIotCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.kv.AttributeKvEntry;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("AttributeCache")
public class AttributeCaffeineCache extends VersionedCaffeineJnksIotCache<AttributeCacheKey, AttributeKvEntry> {

    public AttributeCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.ATTRIBUTES_CACHE);
    }

}

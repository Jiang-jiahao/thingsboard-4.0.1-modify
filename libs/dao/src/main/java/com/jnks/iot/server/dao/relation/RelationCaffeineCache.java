package com.jnks.iot.server.dao.relation;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineTbTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("RelationCache")
public class RelationCaffeineCache extends CaffeineTbTransactionalCache<RelationCacheKey, RelationCacheValue> {

    public RelationCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.RELATIONS_CACHE);
    }

}

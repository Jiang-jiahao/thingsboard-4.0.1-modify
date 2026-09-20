package com.jnks.iot.server.dao.entity.count;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineTbTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.dao.entity.EntityCountCacheKey;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("EntityCountCache")
public class EntityCountCaffeineCache extends CaffeineTbTransactionalCache<EntityCountCacheKey, Long> {

    public EntityCountCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.ENTITY_COUNT_CACHE);
    }

}

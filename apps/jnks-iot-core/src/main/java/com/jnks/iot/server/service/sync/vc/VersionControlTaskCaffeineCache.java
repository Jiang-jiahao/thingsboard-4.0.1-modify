package com.jnks.iot.server.service.sync.vc;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;

import java.util.UUID;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("VersionControlTaskCache")
public class VersionControlTaskCaffeineCache extends CaffeineJnksIotTransactionalCache<UUID, VersionControlTaskCacheEntry> {

    public VersionControlTaskCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.VERSION_CONTROL_TASK_CACHE);
    }

}

package com.jnks.iot.server.cache.resourceInfo;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;


@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("ResourceInfoCache")
public class ResourceInfoCaffeineCache extends CaffeineJnksIotTransactionalCache<ResourceInfoCacheKey, JnksIotResourceInfo> {

    public ResourceInfoCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.RESOURCE_INFO_CACHE);
    }

}

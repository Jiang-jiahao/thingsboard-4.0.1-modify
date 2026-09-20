package com.jnks.iot.server.cache.resourceInfo;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineTbTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.TbResourceInfo;


@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("ResourceInfoCache")
public class ResourceInfoCaffeineCache extends CaffeineTbTransactionalCache<ResourceInfoCacheKey, TbResourceInfo> {

    public ResourceInfoCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.RESOURCE_INFO_CACHE);
    }

}

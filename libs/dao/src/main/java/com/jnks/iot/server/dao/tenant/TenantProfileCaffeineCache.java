package com.jnks.iot.server.dao.tenant;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.TenantProfile;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("TenantProfileCache")
public class TenantProfileCaffeineCache extends CaffeineJnksIotTransactionalCache<TenantProfileCacheKey, TenantProfile> {

    public TenantProfileCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.TENANT_PROFILE_CACHE);
    }

}

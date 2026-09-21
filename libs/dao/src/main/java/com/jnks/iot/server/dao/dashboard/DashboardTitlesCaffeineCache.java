package com.jnks.iot.server.dao.dashboard;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.id.DashboardId;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("DashboardTitlesCache")
public class DashboardTitlesCaffeineCache extends CaffeineJnksIotTransactionalCache<DashboardId, String> {

    public DashboardTitlesCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.DASHBOARD_TITLES_CACHE);
    }

}

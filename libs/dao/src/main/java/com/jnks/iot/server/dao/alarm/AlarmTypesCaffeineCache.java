package com.jnks.iot.server.dao.alarm;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.EntitySubtype;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("AlarmTypesCache")
public class AlarmTypesCaffeineCache extends CaffeineJnksIotTransactionalCache<TenantId, PageData<EntitySubtype>> {

    public AlarmTypesCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.ALARM_TYPES_CACHE);
    }

}

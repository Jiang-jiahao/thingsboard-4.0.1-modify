package com.jnks.iot.server.service.session;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.gen.transport.TransportProtos;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("SessionCache")
public class SessionCaffeineCache extends CaffeineJnksIotTransactionalCache<DeviceId, TransportProtos.DeviceSessionsCacheEntry> {

    public SessionCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.SESSIONS_CACHE);
    }

}

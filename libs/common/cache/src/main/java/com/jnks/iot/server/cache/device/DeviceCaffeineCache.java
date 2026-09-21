package com.jnks.iot.server.cache.device;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.VersionedCaffeineJnksIotCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.Device;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("DeviceCache")
public class DeviceCaffeineCache extends VersionedCaffeineJnksIotCache<DeviceCacheKey, Device> {

    public DeviceCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.DEVICE_CACHE);
    }

}

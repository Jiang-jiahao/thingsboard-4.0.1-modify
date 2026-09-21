package com.jnks.iot.server.dao.device;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.VersionedCaffeineJnksIotCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.DeviceProfile;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("DeviceProfileCache")
public class DeviceProfileCaffeineCache extends VersionedCaffeineJnksIotCache<DeviceProfileCacheKey, DeviceProfile> {

    public DeviceProfileCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.DEVICE_PROFILE_CACHE);
    }

}

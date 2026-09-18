package org.thingsboard.server.dao.device;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.thingsboard.server.cache.VersionedCaffeineTbCache;
import org.thingsboard.server.common.data.CacheConstants;
import org.thingsboard.server.common.data.DeviceProfile;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("DeviceProfileCache")
public class DeviceProfileCaffeineCache extends VersionedCaffeineTbCache<DeviceProfileCacheKey, DeviceProfile> {

    public DeviceProfileCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.DEVICE_PROFILE_CACHE);
    }

}

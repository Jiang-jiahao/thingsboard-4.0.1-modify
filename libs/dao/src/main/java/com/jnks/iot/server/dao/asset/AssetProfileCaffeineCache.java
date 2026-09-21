package com.jnks.iot.server.dao.asset;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.VersionedCaffeineJnksIotCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.asset.AssetProfile;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("AssetProfileCache")
public class AssetProfileCaffeineCache extends VersionedCaffeineJnksIotCache<AssetProfileCacheKey, AssetProfile> {

    public AssetProfileCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.ASSET_PROFILE_CACHE);
    }

}

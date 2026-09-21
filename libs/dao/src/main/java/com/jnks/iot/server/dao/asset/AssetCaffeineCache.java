package com.jnks.iot.server.dao.asset;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.asset.Asset;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("AssetCache")
public class AssetCaffeineCache extends CaffeineJnksIotTransactionalCache<AssetCacheKey, Asset> {

    public AssetCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.ASSET_CACHE);
    }

}

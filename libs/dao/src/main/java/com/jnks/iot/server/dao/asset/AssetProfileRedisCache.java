package com.jnks.iot.server.dao.asset;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbJsonRedisSerializer;
import com.jnks.iot.server.cache.VersionedRedisTbCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.asset.AssetProfile;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("AssetProfileCache")
public class AssetProfileRedisCache extends VersionedRedisTbCache<AssetProfileCacheKey, AssetProfile> {

    public AssetProfileRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.ASSET_PROFILE_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbJsonRedisSerializer<>(AssetProfile.class));
    }

}

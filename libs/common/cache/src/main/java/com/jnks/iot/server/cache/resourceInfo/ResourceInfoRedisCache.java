package com.jnks.iot.server.cache.resourceInfo;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisJnksIotTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.JnksIotResourceInfo;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("ResourceInfoCache")
public class ResourceInfoRedisCache extends RedisJnksIotTransactionalCache<ResourceInfoCacheKey, JnksIotResourceInfo> {

    public ResourceInfoRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.RESOURCE_INFO_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotJsonRedisSerializer<>(JnksIotResourceInfo.class));
    }
}

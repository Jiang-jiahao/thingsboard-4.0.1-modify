package com.jnks.iot.server.cache.resourceInfo;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisTbTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.TbResourceInfo;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("ResourceInfoCache")
public class ResourceInfoRedisCache extends RedisTbTransactionalCache<ResourceInfoCacheKey, TbResourceInfo> {

    public ResourceInfoRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.RESOURCE_INFO_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbJsonRedisSerializer<>(TbResourceInfo.class));
    }
}

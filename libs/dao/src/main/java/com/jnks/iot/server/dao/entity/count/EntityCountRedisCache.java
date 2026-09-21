package com.jnks.iot.server.dao.entity.count;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisJnksIotTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.dao.entity.EntityCountCacheKey;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("EntityCountCache")
public class EntityCountRedisCache extends RedisJnksIotTransactionalCache<EntityCountCacheKey, Long> {

    public EntityCountRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.ENTITY_COUNT_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotJsonRedisSerializer<>(Long.class));
    }
}

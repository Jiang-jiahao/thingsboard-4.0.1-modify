package com.jnks.iot.server.dao.entityview;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotJsonRedisSerializer;
import com.jnks.iot.server.cache.VersionedRedisJnksIotCache;
import com.jnks.iot.server.common.data.CacheConstants;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("EntityViewCache")
public class EntityViewRedisCache extends VersionedRedisJnksIotCache<EntityViewCacheKey, EntityViewCacheValue> {

    public EntityViewRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.ENTITY_VIEW_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotJsonRedisSerializer<>(EntityViewCacheValue.class));
    }
}

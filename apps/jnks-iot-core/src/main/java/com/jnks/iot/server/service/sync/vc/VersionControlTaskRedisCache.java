package com.jnks.iot.server.service.sync.vc;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisJnksIotTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;

import java.util.UUID;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("VersionControlTaskCache")
public class VersionControlTaskRedisCache extends RedisJnksIotTransactionalCache<UUID, VersionControlTaskCacheEntry> {

    public VersionControlTaskRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.VERSION_CONTROL_TASK_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotJsonRedisSerializer<>(VersionControlTaskCacheEntry.class));
    }
}

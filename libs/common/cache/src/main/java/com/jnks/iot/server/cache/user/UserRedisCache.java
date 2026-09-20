package com.jnks.iot.server.cache.user;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisTbTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.User;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("UserCache")
public class UserRedisCache extends RedisTbTransactionalCache<UserCacheKey, User> {

    public UserRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.USER_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbJsonRedisSerializer<>(User.class));
    }
}

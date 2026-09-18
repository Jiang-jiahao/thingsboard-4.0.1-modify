package org.thingsboard.server.cache.mobile.secret;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import org.thingsboard.server.cache.CacheSpecsMap;
import org.thingsboard.server.cache.RedisTbTransactionalCache;
import org.thingsboard.server.cache.TBRedisCacheConfiguration;
import org.thingsboard.server.cache.TbJsonRedisSerializer;
import org.thingsboard.server.common.data.CacheConstants;
import org.thingsboard.server.common.data.security.model.JwtPair;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("MobileSecretCache")
public class MobileSecretRedisCache extends RedisTbTransactionalCache<String, JwtPair> {

    public MobileSecretRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.MOBILE_SECRET_KEY_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbJsonRedisSerializer<>(JwtPair.class));
    }
}

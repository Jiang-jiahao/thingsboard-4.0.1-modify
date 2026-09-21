package com.jnks.iot.server.dao.user;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisJnksIotTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.settings.UserSettings;
import com.jnks.iot.server.common.data.settings.UserSettingsCompositeKey;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("UserSettingsCache")
public class UserSettingsRedisCache extends RedisJnksIotTransactionalCache<UserSettingsCompositeKey, UserSettings> {

    public UserSettingsRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.USER_SETTINGS_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotJsonRedisSerializer<>(UserSettings.class));
    }
}

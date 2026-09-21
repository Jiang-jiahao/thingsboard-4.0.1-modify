package com.jnks.iot.server.service.sync.vc.autocommit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisJnksIotTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sync.vc.AutoCommitSettings;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("AutoCommitSettingsCache")
public class AutoCommitSettingsRedisCache extends RedisJnksIotTransactionalCache<TenantId, AutoCommitSettings> {

    public AutoCommitSettingsRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.AUTO_COMMIT_SETTINGS_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotJsonRedisSerializer<>(AutoCommitSettings.class));
    }
}

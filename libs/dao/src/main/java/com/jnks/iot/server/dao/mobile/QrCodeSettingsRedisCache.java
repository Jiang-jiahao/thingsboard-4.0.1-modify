package com.jnks.iot.server.dao.mobile;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisTbTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.qrCodeSettings.QrCodeSettings;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("QrCodeSettingsCache")
public class QrCodeSettingsRedisCache extends RedisTbTransactionalCache<TenantId, QrCodeSettings> {

    public QrCodeSettingsRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.QR_CODE_SETTINGS_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbJsonRedisSerializer<>(QrCodeSettings.class));
    }
}

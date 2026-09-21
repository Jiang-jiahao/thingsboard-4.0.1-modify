package com.jnks.iot.server.dao.ota;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisJnksIotTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.OtaPackageInfo;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("OtaPackageCache")
public class OtaPackageRedisCache extends RedisJnksIotTransactionalCache<OtaPackageCacheKey, OtaPackageInfo> {

    public OtaPackageRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.OTA_PACKAGE_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotJsonRedisSerializer<>(OtaPackageInfo.class));
    }
}

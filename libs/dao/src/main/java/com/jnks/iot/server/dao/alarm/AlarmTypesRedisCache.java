package com.jnks.iot.server.dao.alarm;

import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisJnksIotTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.JnksIotTypedJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.EntitySubtype;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.page.PageData;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("AlarmTypesCache")
public class AlarmTypesRedisCache extends RedisJnksIotTransactionalCache<TenantId, PageData<EntitySubtype>> {

    public AlarmTypesRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.ALARM_TYPES_CACHE, cacheSpecsMap, connectionFactory, configuration, new JnksIotTypedJsonRedisSerializer<>(new TypeReference<>() {}));
    }
}

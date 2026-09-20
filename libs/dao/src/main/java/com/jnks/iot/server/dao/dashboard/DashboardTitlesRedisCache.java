package com.jnks.iot.server.dao.dashboard;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisTbTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.id.DashboardId;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("DashboardTitlesCache")
public class DashboardTitlesRedisCache extends RedisTbTransactionalCache<DashboardId, String> {

    public DashboardTitlesRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.DASHBOARD_TITLES_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbJsonRedisSerializer<>(String.class));
    }
}

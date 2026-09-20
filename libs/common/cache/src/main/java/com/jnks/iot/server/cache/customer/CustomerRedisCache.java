package com.jnks.iot.server.cache.customer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CacheSpecsMap;
import com.jnks.iot.server.cache.RedisTbTransactionalCache;
import com.jnks.iot.server.cache.TBRedisCacheConfiguration;
import com.jnks.iot.server.cache.TbJsonRedisSerializer;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.Customer;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "redis")
@Service("CustomerCache")
public class CustomerRedisCache extends RedisTbTransactionalCache<CustomerCacheKey, Customer> {

    public CustomerRedisCache(TBRedisCacheConfiguration configuration, CacheSpecsMap cacheSpecsMap, RedisConnectionFactory connectionFactory) {
        super(CacheConstants.CUSTOMER_CACHE, cacheSpecsMap, connectionFactory, configuration, new TbJsonRedisSerializer<>(Customer.class));
    }
}

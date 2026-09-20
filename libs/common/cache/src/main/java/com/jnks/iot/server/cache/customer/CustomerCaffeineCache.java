package com.jnks.iot.server.cache.customer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineTbTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.Customer;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("CustomerCache")
public class CustomerCaffeineCache extends CaffeineTbTransactionalCache<CustomerCacheKey, Customer> {

    public CustomerCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.CUSTOMER_CACHE);
    }

}

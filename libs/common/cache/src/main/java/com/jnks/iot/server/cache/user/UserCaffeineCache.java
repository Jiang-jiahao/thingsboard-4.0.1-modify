package com.jnks.iot.server.cache.user;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.User;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("UserCache")
public class UserCaffeineCache extends CaffeineJnksIotTransactionalCache<UserCacheKey, User> {

    public UserCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.USER_CACHE);
    }

}

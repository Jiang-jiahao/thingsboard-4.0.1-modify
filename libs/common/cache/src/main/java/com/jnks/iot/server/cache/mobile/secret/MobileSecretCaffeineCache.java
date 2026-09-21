package com.jnks.iot.server.cache.mobile.secret;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineJnksIotTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.security.model.JwtPair;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("MobileSecretCache")
public class MobileSecretCaffeineCache extends CaffeineJnksIotTransactionalCache<String, JwtPair> {

    public MobileSecretCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.MOBILE_SECRET_KEY_CACHE);
    }

}

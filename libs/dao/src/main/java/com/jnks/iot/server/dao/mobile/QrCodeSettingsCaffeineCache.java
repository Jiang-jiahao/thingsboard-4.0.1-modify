package com.jnks.iot.server.dao.mobile;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.CaffeineTbTransactionalCache;
import com.jnks.iot.server.common.data.CacheConstants;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.mobile.qrCodeSettings.QrCodeSettings;

@ConditionalOnProperty(prefix = "cache", value = "type", havingValue = "caffeine", matchIfMissing = true)
@Service("QrCodeSettingsCache")
public class QrCodeSettingsCaffeineCache extends CaffeineTbTransactionalCache<TenantId, QrCodeSettings> {

    public QrCodeSettingsCaffeineCache(CacheManager cacheManager) {
        super(cacheManager, CacheConstants.QR_CODE_SETTINGS_CACHE);
    }

}

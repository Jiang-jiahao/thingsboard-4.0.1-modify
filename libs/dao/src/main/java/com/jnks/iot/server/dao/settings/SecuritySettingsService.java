package com.jnks.iot.server.dao.settings;

import com.jnks.iot.server.common.data.security.model.SecuritySettings;

public interface SecuritySettingsService {

    /**
     * 获取安全策略
     * @return
     */
    SecuritySettings getSecuritySettings();

    SecuritySettings saveSecuritySettings(SecuritySettings securitySettings);

}

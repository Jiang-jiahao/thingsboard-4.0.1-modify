package org.thingsboard.server.dao.settings;

import org.thingsboard.server.common.data.security.model.SecuritySettings;

public interface SecuritySettingsService {

    /**
     * 获取安全策略
     * @return
     */
    SecuritySettings getSecuritySettings();

    SecuritySettings saveSecuritySettings(SecuritySettings securitySettings);

}

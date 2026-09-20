package com.jnks.iot.server.dao.settings;

import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.model.SecuritySettings;
import com.jnks.iot.server.common.data.security.model.UserPasswordPolicy;
import com.jnks.iot.server.dao.service.ConstraintValidator;

import static com.jnks.iot.server.common.data.CacheConstants.SECURITY_SETTINGS_CACHE;

@Service
@RequiredArgsConstructor
public class DefaultSecuritySettingsService implements SecuritySettingsService {

    private final AdminSettingsService adminSettingsService;

    public static final int DEFAULT_MOBILE_SECRET_KEY_LENGTH = 64;

    @Cacheable(cacheNames = SECURITY_SETTINGS_CACHE, key = "'securitySettings'")
    @Override
    public SecuritySettings getSecuritySettings() {
        AdminSettings adminSettings = adminSettingsService.findAdminSettingsByKey(TenantId.SYS_TENANT_ID, "securitySettings");
        SecuritySettings securitySettings;
        if (adminSettings != null) {
            try {
                securitySettings = JacksonUtil.convertValue(adminSettings.getJsonValue(), SecuritySettings.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load security settings!", e);
            }
        } else {
            securitySettings = new SecuritySettings();
            securitySettings.setPasswordPolicy(new UserPasswordPolicy());
            securitySettings.getPasswordPolicy().setMinimumLength(6);
            securitySettings.getPasswordPolicy().setMaximumLength(72);
            securitySettings.setMobileSecretKeyLength(DEFAULT_MOBILE_SECRET_KEY_LENGTH);
            securitySettings.setPasswordResetTokenTtl(24);
            securitySettings.setUserActivationTokenTtl(24);
        }
        return securitySettings;
    }

    @CacheEvict(cacheNames = SECURITY_SETTINGS_CACHE, key = "'securitySettings'")
    @Override
    public SecuritySettings saveSecuritySettings(SecuritySettings securitySettings) {
        ConstraintValidator.validateFields(securitySettings);
        AdminSettings adminSettings = adminSettingsService.findAdminSettingsByKey(TenantId.SYS_TENANT_ID, "securitySettings");
        if (adminSettings == null) {
            adminSettings = new AdminSettings();
            adminSettings.setTenantId(TenantId.SYS_TENANT_ID);
            adminSettings.setKey("securitySettings");
        }
        adminSettings.setJsonValue(JacksonUtil.valueToTree(securitySettings));
        AdminSettings savedAdminSettings = adminSettingsService.saveAdminSettings(TenantId.SYS_TENANT_ID, adminSettings);
        try {
            return JacksonUtil.convertValue(savedAdminSettings.getJsonValue(), SecuritySettings.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load security settings!", e);
        }
    }

}

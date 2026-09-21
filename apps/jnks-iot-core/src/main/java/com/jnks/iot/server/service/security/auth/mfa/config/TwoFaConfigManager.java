package com.jnks.iot.server.service.security.auth.mfa.config;

import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.security.model.mfa.PlatformTwoFaSettings;
import com.jnks.iot.server.common.data.security.model.mfa.account.AccountTwoFaSettings;
import com.jnks.iot.server.common.data.security.model.mfa.account.TwoFaAccountConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderType;

import java.util.Optional;

/**
 * 双因子认证(2FA)配置管理接口
 */
public interface TwoFaConfigManager {

    Optional<AccountTwoFaSettings> getAccountTwoFaSettings(TenantId tenantId, UserId userId);


    Optional<TwoFaAccountConfig> getTwoFaAccountConfig(TenantId tenantId, UserId userId, TwoFaProviderType providerType);

    AccountTwoFaSettings saveTwoFaAccountConfig(TenantId tenantId, UserId userId, TwoFaAccountConfig accountConfig);

    AccountTwoFaSettings deleteTwoFaAccountConfig(TenantId tenantId, UserId userId, TwoFaProviderType providerType);


    Optional<PlatformTwoFaSettings> getPlatformTwoFaSettings(TenantId tenantId, boolean sysadminSettingsAsDefault);

    PlatformTwoFaSettings savePlatformTwoFaSettings(TenantId tenantId, PlatformTwoFaSettings twoFactorAuthSettings) throws JnksIotException;

    void deletePlatformTwoFaSettings(TenantId tenantId);

}

package com.jnks.iot.server.service.security.auth.mfa;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.security.model.mfa.account.TwoFaAccountConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderType;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * 双因子认证服务。
 * <p>
 * 登录第二步：按平台/账号配置生成与校验验证码，并做发送/校验频控。
 *
 * @see DefaultTwoFactorAuthService
 */
public interface TwoFactorAuthService {

    /**
     * 用户是否已配置至少一种 2FA。
     */
    boolean isTwoFaEnabled(TenantId tenantId, UserId userId);

    /**
     * 检查指定提供方在租户下是否可用。
     */
    void checkProvider(TenantId tenantId, TwoFaProviderType providerType) throws JnksIotException;


    /**
     * 按提供方类型准备验证码（如发邮件/短信）。
     */
    void prepareVerificationCode(SecurityUser user, TwoFaProviderType providerType, boolean checkLimits) throws Exception;

    /**
     * 按账号配置准备验证码。
     */
    void prepareVerificationCode(SecurityUser user, TwoFaAccountConfig accountConfig, boolean checkLimits) throws JnksIotException;


    /**
     * 按提供方类型校验验证码。
     */
    boolean checkVerificationCode(SecurityUser user, TwoFaProviderType providerType, String verificationCode, boolean checkLimits) throws JnksIotException;

    /**
     * 按账号配置校验验证码。
     */
    boolean checkVerificationCode(SecurityUser user, String verificationCode, TwoFaAccountConfig accountConfig, boolean checkLimits) throws JnksIotException;


    /**
     * 为用户生成新的 2FA 账号配置（如 TOTP 密钥）。
     */
    TwoFaAccountConfig generateNewAccountConfig(User user, TwoFaProviderType providerType) throws JnksIotException;

}

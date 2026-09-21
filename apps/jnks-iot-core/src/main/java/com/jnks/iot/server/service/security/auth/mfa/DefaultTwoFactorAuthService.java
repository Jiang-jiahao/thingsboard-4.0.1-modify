package com.jnks.iot.server.service.security.auth.mfa;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.LockedException;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cache.limits.RateLimitService;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.limit.LimitedApi;
import com.jnks.iot.server.common.data.security.model.mfa.PlatformTwoFaSettings;
import com.jnks.iot.server.common.data.security.model.mfa.account.TwoFaAccountConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderType;
import com.jnks.iot.server.dao.user.UserService;
import com.jnks.iot.server.service.security.auth.mfa.config.TwoFaConfigManager;
import com.jnks.iot.server.service.security.auth.mfa.provider.TwoFaProvider;
import com.jnks.iot.server.service.security.model.SecurityUser;
import com.jnks.iot.server.service.security.system.SystemSecurityService;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * {@link TwoFactorAuthService} 默认实现。
 * <p>
 * 按 {@link TwoFaProviderType} 分发到具体 Provider；发送/校验走 {@link RateLimitService}，
 * 失败锁定委托 {@link SystemSecurityService}。
 *
 * @see TwoFactorAuthService
 */
@Service
@RequiredArgsConstructor
public class DefaultTwoFactorAuthService implements TwoFactorAuthService {

    private final TwoFaConfigManager configManager;
    private final SystemSecurityService systemSecurityService;
    private final UserService userService;
    private final RateLimitService rateLimitService;
    private final Map<TwoFaProviderType, TwoFaProvider<TwoFaProviderConfig, TwoFaAccountConfig>> providers = new EnumMap<>(TwoFaProviderType.class);

    private static final JnksIotException ACCOUNT_NOT_CONFIGURED_ERROR = new JnksIotException("2FA is not configured for account", JnksIotErrorCode.BAD_REQUEST_PARAMS);
    private static final JnksIotException PROVIDER_NOT_CONFIGURED_ERROR = new JnksIotException("2FA provider is not configured", JnksIotErrorCode.BAD_REQUEST_PARAMS);
    private static final JnksIotException PROVIDER_NOT_AVAILABLE_ERROR = new JnksIotException("2FA provider is not available", JnksIotErrorCode.GENERAL);
    private static final JnksIotException TOO_MANY_REQUESTS_ERROR = new JnksIotException("Too many requests", JnksIotErrorCode.TOO_MANY_REQUESTS);

    /**
     * 用户账号是否已配置 2FA。
     */
    @Override
    public boolean isTwoFaEnabled(TenantId tenantId, UserId userId) {
        return configManager.getAccountTwoFaSettings(tenantId, userId)
                .map(settings -> !settings.getConfigs().isEmpty())
                .orElse(false);
    }

    /**
     * 检查提供方是否可用。
     */
    @Override
    public void checkProvider(TenantId tenantId, TwoFaProviderType providerType) throws JnksIotException {
        getTwoFaProvider(providerType).check(tenantId);
    }


    /**
     * 按提供方加载账号配置后准备验证码。
     */
    @Override
    public void prepareVerificationCode(SecurityUser user, TwoFaProviderType providerType, boolean checkLimits) throws Exception {
        TwoFaAccountConfig accountConfig = configManager.getTwoFaAccountConfig(user.getTenantId(), user.getId(), providerType)
                .orElseThrow(() -> ACCOUNT_NOT_CONFIGURED_ERROR);
        prepareVerificationCode(user, accountConfig, checkLimits);
    }

    /**
     * 按账号配置准备验证码，可选发送频控。
     */
    @Override
    public void prepareVerificationCode(SecurityUser user, TwoFaAccountConfig accountConfig, boolean checkLimits) throws JnksIotException {
        PlatformTwoFaSettings twoFaSettings = configManager.getPlatformTwoFaSettings(user.getTenantId(), true)
                .orElseThrow(() -> PROVIDER_NOT_CONFIGURED_ERROR);
        if (checkLimits) {
            Integer minVerificationCodeSendPeriod = twoFaSettings.getMinVerificationCodeSendPeriod();
            String rateLimit = null;
            if (minVerificationCodeSendPeriod != null && minVerificationCodeSendPeriod > 4) {
                rateLimit = "1:" + minVerificationCodeSendPeriod;
            }
            if (!rateLimitService.checkRateLimit(LimitedApi.TWO_FA_VERIFICATION_CODE_SEND,
                    Pair.of(user.getId(), accountConfig.getProviderType()), rateLimit)) {
                throw TOO_MANY_REQUESTS_ERROR;
            }
        }

        TwoFaProviderConfig providerConfig = twoFaSettings.getProviderConfig(accountConfig.getProviderType())
                .orElseThrow(() -> PROVIDER_NOT_CONFIGURED_ERROR);
        getTwoFaProvider(accountConfig.getProviderType()).prepareVerificationCode(user, providerConfig, accountConfig);
    }


    /**
     * 按提供方加载账号配置后校验验证码。
     */
    @Override
    public boolean checkVerificationCode(SecurityUser user, TwoFaProviderType providerType, String verificationCode, boolean checkLimits) throws JnksIotException {
        TwoFaAccountConfig accountConfig = configManager.getTwoFaAccountConfig(user.getTenantId(), user.getId(), providerType)
                .orElseThrow(() -> ACCOUNT_NOT_CONFIGURED_ERROR);
        return checkVerificationCode(user, verificationCode, accountConfig, checkLimits);
    }

    /**
     * 校验验证码；可选频控与失败锁定，成功后清理限流计数。
     */
    @Override
    public boolean checkVerificationCode(SecurityUser user, String verificationCode, TwoFaAccountConfig accountConfig, boolean checkLimits) throws JnksIotException {
        if (!userService.findUserCredentialsByUserId(user.getTenantId(), user.getId()).isEnabled()) {
            throw new JnksIotException("User is disabled", JnksIotErrorCode.AUTHENTICATION);
        }

        PlatformTwoFaSettings twoFaSettings = configManager.getPlatformTwoFaSettings(user.getTenantId(), true)
                .orElseThrow(() -> PROVIDER_NOT_CONFIGURED_ERROR);
        if (checkLimits) {
            if (!rateLimitService.checkRateLimit(LimitedApi.TWO_FA_VERIFICATION_CODE_CHECK,
                    Pair.of(user.getId(), accountConfig.getProviderType()), twoFaSettings.getVerificationCodeCheckRateLimit())) {
                throw TOO_MANY_REQUESTS_ERROR;
            }
        }
        TwoFaProviderConfig providerConfig = twoFaSettings.getProviderConfig(accountConfig.getProviderType())
                .orElseThrow(() -> PROVIDER_NOT_CONFIGURED_ERROR);

        boolean verificationSuccess = false;
        if (StringUtils.isNotBlank(verificationCode)) {
            if (StringUtils.isNumeric(verificationCode) || accountConfig.getProviderType() == TwoFaProviderType.BACKUP_CODE) {
                verificationSuccess = getTwoFaProvider(accountConfig.getProviderType()).checkVerificationCode(user, verificationCode, providerConfig, accountConfig);
            }
        }
        if (checkLimits) {
            try {
                systemSecurityService.validateTwoFaVerification(user, verificationSuccess, twoFaSettings);
            } catch (LockedException e) {
                cleanUpRateLimits(user.getId());
                throw new JnksIotException(e.getMessage(), JnksIotErrorCode.AUTHENTICATION);
            }
            if (verificationSuccess) {
                cleanUpRateLimits(user.getId());
            }
        }
        return verificationSuccess;
    }

    /**
     * 生成新的 2FA 账号配置。
     */
    @Override
    public TwoFaAccountConfig generateNewAccountConfig(User user, TwoFaProviderType providerType) throws JnksIotException {
        TwoFaProviderConfig providerConfig = getTwoFaProviderConfig(user.getTenantId(), providerType);
        return getTwoFaProvider(providerType).generateNewAccountConfig(user, providerConfig);
    }

    private void cleanUpRateLimits(UserId userId) {
        for (TwoFaProviderType providerType : TwoFaProviderType.values()) {
            rateLimitService.cleanUp(LimitedApi.TWO_FA_VERIFICATION_CODE_SEND, Pair.of(userId, providerType));
            rateLimitService.cleanUp(LimitedApi.TWO_FA_VERIFICATION_CODE_CHECK, Pair.of(userId, providerType));
        }
    }

    private TwoFaProviderConfig getTwoFaProviderConfig(TenantId tenantId, TwoFaProviderType providerType) throws JnksIotException {
        return configManager.getPlatformTwoFaSettings(tenantId, true)
                .flatMap(twoFaSettings -> twoFaSettings.getProviderConfig(providerType))
                .orElseThrow(() -> PROVIDER_NOT_CONFIGURED_ERROR);
    }

    private TwoFaProvider<TwoFaProviderConfig, TwoFaAccountConfig> getTwoFaProvider(TwoFaProviderType providerType) throws JnksIotException {
        return Optional.ofNullable(providers.get(providerType))
                .orElseThrow(() -> PROVIDER_NOT_AVAILABLE_ERROR);
    }

    @Autowired
    private void setProviders(Collection<TwoFaProvider> providers) {
        providers.forEach(provider -> {
            this.providers.put(provider.getType(), provider);
        });
    }

}

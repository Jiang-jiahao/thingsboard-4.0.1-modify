package com.jnks.iot.server.service.security.auth.mfa.provider.impl;

import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.rule.engine.api.MailService;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.model.mfa.account.EmailTwoFaAccountConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.EmailTwoFaProviderConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderType;
import com.jnks.iot.server.service.security.model.SecurityUser;

@Service
public class EmailTwoFaProvider extends OtpBasedTwoFaProvider<EmailTwoFaProviderConfig, EmailTwoFaAccountConfig> {

    private final MailService mailService;

    protected EmailTwoFaProvider(CacheManager cacheManager, MailService mailService) {
        super(cacheManager);
        this.mailService = mailService;
    }

    @Override
    public EmailTwoFaAccountConfig generateNewAccountConfig(User user, EmailTwoFaProviderConfig providerConfig) {
        EmailTwoFaAccountConfig config = new EmailTwoFaAccountConfig();
        config.setEmail(user.getEmail());
        return config;
    }

    @Override
    public void check(TenantId tenantId) throws JnksIotException {
        try {
            mailService.testConnection(tenantId);
        } catch (Exception e) {
            throw new JnksIotException("Mail service is not set up", JnksIotErrorCode.BAD_REQUEST_PARAMS);
        }
    }

    @Override
    protected void sendVerificationCode(SecurityUser user, String verificationCode, EmailTwoFaProviderConfig providerConfig, EmailTwoFaAccountConfig accountConfig) throws JnksIotException {
        try {
            mailService.sendTwoFaVerificationEmail(accountConfig.getEmail(), verificationCode, providerConfig.getVerificationCodeLifetime());
        } catch (Exception e) {
            throw new JnksIotException("Couldn't send 2FA verification email", JnksIotErrorCode.GENERAL);
        }
    }

    @Override
    public TwoFaProviderType getType() {
        return TwoFaProviderType.EMAIL;
    }

}

package com.jnks.iot.server.service.security.auth.mfa.provider.impl;

import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import com.jnks.iot.rule.engine.api.SmsService;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.audit.ActionType;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.security.model.mfa.account.SmsTwoFaAccountConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.SmsTwoFaProviderConfig;
import com.jnks.iot.server.common.data.security.model.mfa.provider.TwoFaProviderType;
import com.jnks.iot.server.dao.audit.AuditLogService;
import com.jnks.iot.server.service.security.model.SecurityUser;

import java.util.Map;

@Service
public class SmsTwoFaProvider extends OtpBasedTwoFaProvider<SmsTwoFaProviderConfig, SmsTwoFaAccountConfig> {

    private final SmsService smsService;
    private final AuditLogService auditLogService;

    public SmsTwoFaProvider(CacheManager cacheManager, SmsService smsService, AuditLogService auditLogService) {
        super(cacheManager);
        this.smsService = smsService;
        this.auditLogService = auditLogService;
    }


    @Override
    public SmsTwoFaAccountConfig generateNewAccountConfig(User user, SmsTwoFaProviderConfig providerConfig) {
        return new SmsTwoFaAccountConfig();
    }

    @Override
    protected void sendVerificationCode(SecurityUser user, String verificationCode, SmsTwoFaProviderConfig providerConfig, SmsTwoFaAccountConfig accountConfig) throws JnksIotException {
        Map<String, String> messageData = Map.of(
                "code", verificationCode,
                "userEmail", user.getEmail()
        );
        String message = JnksIotNodeUtils.processTemplate(providerConfig.getSmsVerificationMessageTemplate(), messageData);
        String phoneNumber = accountConfig.getPhoneNumber();
        try {
            smsService.sendSms(user.getTenantId(), user.getCustomerId(), new String[]{phoneNumber}, message);
            auditLogService.logEntityAction(user.getTenantId(), user.getCustomerId(), user.getId(), user.getName(), user.getId(), user, ActionType.SMS_SENT, null, phoneNumber);
        } catch (JnksIotException e) {
            auditLogService.logEntityAction(user.getTenantId(), user.getCustomerId(), user.getId(), user.getName(), user.getId(), user, ActionType.SMS_SENT, e, phoneNumber);
            throw e;
        }
    }

    @Override
    public void check(TenantId tenantId) throws JnksIotException {
        if (!smsService.isConfigured(tenantId)) {
            throw new JnksIotException("SMS service is not configured", JnksIotErrorCode.BAD_REQUEST_PARAMS);
        }
    }


    @Override
    public TwoFaProviderType getType() {
        return TwoFaProviderType.SMS;
    }

}

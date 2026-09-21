package com.jnks.iot.rule.engine.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.mail.javamail.JavaMailSender;
import com.jnks.iot.server.common.data.ApiFeature;
import com.jnks.iot.server.common.data.ApiUsageRecordState;
import com.jnks.iot.server.common.data.ApiUsageStateValue;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;

public interface MailService {

    void updateMailConfiguration();

    void sendEmail(TenantId tenantId, String email, String subject, String message) throws JnksIotException;

    void sendTestMail(JsonNode config, String email) throws JnksIotException;

    void sendActivationEmail(String activationLink, long ttlMs, String email) throws JnksIotException;

    void sendAccountActivatedEmail(String loginLink, String email) throws JnksIotException;

    void sendResetPasswordEmail(String passwordResetLink, long ttlMs, String email) throws JnksIotException;

    void sendResetPasswordEmailAsync(String passwordResetLink, long ttlMs, String email);

    void sendPasswordWasResetEmail(String loginLink, String email) throws JnksIotException;

    void sendAccountLockoutEmail(String lockoutEmail, String email, Integer maxFailedLoginAttempts) throws JnksIotException;

    void sendTwoFaVerificationEmail(String email, String verificationCode, int expirationTimeSeconds) throws JnksIotException;

    void send(TenantId tenantId, CustomerId customerId, JnksIotEmail jnksIotEmail) throws JnksIotException;

    void send(TenantId tenantId, CustomerId customerId, JnksIotEmail jnksIotEmail, JavaMailSender javaMailSender, long timeout) throws JnksIotException;

    void sendApiFeatureStateEmail(ApiFeature apiFeature, ApiUsageStateValue stateValue, String email, ApiUsageRecordState recordState) throws JnksIotException;

    void testConnection(TenantId tenantId) throws Exception;

    boolean isConfigured(TenantId tenantId);

}

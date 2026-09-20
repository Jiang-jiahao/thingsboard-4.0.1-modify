package com.jnks.iot.server.service.sms;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.NestedRuntimeException;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.api.SmsService;
import com.jnks.iot.rule.engine.api.sms.SmsSender;
import com.jnks.iot.rule.engine.api.sms.SmsSenderFactory;
import com.jnks.iot.server.common.data.AdminSettings;
import com.jnks.iot.server.common.data.ApiUsageRecordKey;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sms.config.SmsProviderConfiguration;
import com.jnks.iot.server.common.data.sms.config.TestSmsRequest;
import com.jnks.iot.server.common.stats.TbApiUsageReportClient;
import com.jnks.iot.server.dao.settings.AdminSettingsService;
import com.jnks.iot.server.service.apiusage.TbApiUsageStateService;

@Service
@Slf4j
public class DefaultSmsService implements SmsService {

    private final SmsSenderFactory smsSenderFactory;
    private final AdminSettingsService adminSettingsService;
    private final TbApiUsageStateService apiUsageStateService;
    private final TbApiUsageReportClient apiUsageClient;

    private SmsSender smsSender;

    public DefaultSmsService(SmsSenderFactory smsSenderFactory, AdminSettingsService adminSettingsService, TbApiUsageStateService apiUsageStateService, TbApiUsageReportClient apiUsageClient) {
        this.smsSenderFactory = smsSenderFactory;
        this.adminSettingsService = adminSettingsService;
        this.apiUsageStateService = apiUsageStateService;
        this.apiUsageClient = apiUsageClient;
    }

    @PostConstruct
    private void init() {
        updateSmsConfiguration();
    }

    @PreDestroy
    private void destroy() {
        if (this.smsSender != null) {
            this.smsSender.destroy();
        }
    }

    @Override
    public void updateSmsConfiguration() {
        AdminSettings settings = adminSettingsService.findAdminSettingsByKey(TenantId.SYS_TENANT_ID, "sms");
        if (settings != null) {
            try {
                JsonNode jsonConfig = settings.getJsonValue();
                SmsProviderConfiguration configuration = JacksonUtil.convertValue(jsonConfig, SmsProviderConfiguration.class);
                SmsSender newSmsSender = this.smsSenderFactory.createSmsSender(configuration);
                if (this.smsSender != null) {
                    this.smsSender.destroy();
                }
                this.smsSender = newSmsSender;
            } catch (Exception e) {
                log.error("Failed to create SMS sender", e);
            }
        }
    }

    protected int sendSms(String numberTo, String message) throws JnksIotException {
        if (this.smsSender == null) {
            throw new JnksIotException("Unable to send SMS: no SMS provider configured!", JnksIotErrorCode.GENERAL);
        }
        return this.sendSms(this.smsSender, numberTo, message);
    }

    @Override
    public void sendSms(TenantId tenantId, CustomerId customerId, String[] numbersTo, String message) throws JnksIotException {
        if (apiUsageStateService.getApiUsageState(tenantId).isSmsSendEnabled()) {
            int smsCount = 0;
            try {
                for (String numberTo : numbersTo) {
                    smsCount += this.sendSms(numberTo, message);
                }
            } finally {
                if (smsCount > 0) {
                    apiUsageClient.report(tenantId, customerId, ApiUsageRecordKey.SMS_EXEC_COUNT, smsCount);
                }
            }
        } else {
            throw new RuntimeException("SMS sending is disabled due to API limits!");
        }
    }

    @Override
    public void sendTestSms(TestSmsRequest testSmsRequest) throws JnksIotException {
        SmsSender testSmsSender;
        try {
            testSmsSender = this.smsSenderFactory.createSmsSender(testSmsRequest.getProviderConfiguration());
        } catch (Exception e) {
            throw handleException(e);
        }
        this.sendSms(testSmsSender, testSmsRequest.getNumberTo(), testSmsRequest.getMessage());
        testSmsSender.destroy();
    }

    @Override
    public boolean isConfigured(TenantId tenantId) {
        return smsSender != null;
    }

    private int sendSms(SmsSender smsSender, String numberTo, String message) throws JnksIotException {
        try {
            int sentSms = smsSender.sendSms(numberTo, message);
            log.trace("Successfully sent sms to number: {}", numberTo);
            return sentSms;
        } catch (Exception e) {
            throw handleException(e);
        }
    }

    private JnksIotException handleException(Exception exception) {
        String message;
        if (exception instanceof NestedRuntimeException) {
            message = ((NestedRuntimeException) exception).getMostSpecificCause().getMessage();
        } else {
            message = exception.getMessage();
        }
        log.warn("Unable to send SMS: {}", message);
        return new JnksIotException(String.format("Unable to send SMS: %s", message),
                JnksIotErrorCode.GENERAL);
    }
}

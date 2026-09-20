package com.jnks.iot.rule.engine.api;

import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.data.id.CustomerId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.data.sms.config.TestSmsRequest;

/**
 * 默认的短信服务
 */
public interface SmsService {

    void updateSmsConfiguration();

    void sendSms(TenantId tenantId, CustomerId customerId, String[] numbersTo, String message) throws JnksIotException;;

    void sendTestSms(TestSmsRequest testSmsRequest) throws JnksIotException;

    boolean isConfigured(TenantId tenantId);

}

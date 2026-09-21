package com.jnks.iot.rule.engine.sms;

import lombok.Data;
import com.jnks.iot.rule.engine.api.NodeConfiguration;
import com.jnks.iot.server.common.data.sms.config.SmsProviderConfiguration;

@Data
public class JnksIotSendSmsNodeConfiguration implements NodeConfiguration {

    private String numbersToTemplate;
    private String smsMessageTemplate;
    private boolean useSystemSmsSettings;
    private SmsProviderConfiguration smsProviderConfiguration;

    @Override
    public NodeConfiguration defaultConfiguration() {
        JnksIotSendSmsNodeConfiguration configuration = new JnksIotSendSmsNodeConfiguration();
        configuration.numbersToTemplate = "${userPhone}";
        configuration.smsMessageTemplate = "Device ${deviceName} has high temperature ${temp}";
        configuration.setUseSystemSmsSettings(true);
        return configuration;
    }
}

package com.jnks.iot.rule.engine.api.sms;

import com.jnks.iot.server.common.data.sms.config.SmsProviderConfiguration;

public interface SmsSenderFactory {

    SmsSender createSmsSender(SmsProviderConfiguration config);

}

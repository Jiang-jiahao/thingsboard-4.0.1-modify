package com.jnks.iot.server.service.sms;

import org.springframework.stereotype.Component;
import com.jnks.iot.rule.engine.api.sms.SmsSender;
import com.jnks.iot.rule.engine.api.sms.SmsSenderFactory;
import com.jnks.iot.server.common.data.sms.config.AwsSnsSmsProviderConfiguration;
import com.jnks.iot.server.common.data.sms.config.SmppSmsProviderConfiguration;
import com.jnks.iot.server.common.data.sms.config.SmsProviderConfiguration;
import com.jnks.iot.server.common.data.sms.config.TwilioSmsProviderConfiguration;
import com.jnks.iot.server.service.sms.aws.AwsSmsSender;
import com.jnks.iot.server.service.sms.smpp.SmppSmsSender;
import com.jnks.iot.server.service.sms.twilio.TwilioSmsSender;

@Component
public class DefaultSmsSenderFactory implements SmsSenderFactory {

    @Override
    public SmsSender createSmsSender(SmsProviderConfiguration config) {
        switch (config.getType()) {
            case AWS_SNS:
                return new AwsSmsSender((AwsSnsSmsProviderConfiguration)config);
            case TWILIO:
                return new TwilioSmsSender((TwilioSmsProviderConfiguration)config);
            case SMPP:
                return new SmppSmsSender((SmppSmsProviderConfiguration) config);
            default:
                throw new RuntimeException("Unknown SMS provider type " + config.getType());
        }
    }

}

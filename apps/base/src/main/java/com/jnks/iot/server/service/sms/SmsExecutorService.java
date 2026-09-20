package com.jnks.iot.server.service.sms;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.AbstractListeningExecutor;

@Component
public class SmsExecutorService extends AbstractListeningExecutor {

    @Value("${actors.rule.sms_thread_pool_size}")
    private int smsExecutorThreadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return smsExecutorThreadPoolSize;
    }

}

package com.jnks.iot.server.service.executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.AbstractListeningExecutor;

@Component
public class NotificationExecutorService extends AbstractListeningExecutor {

    @Value("${notification_system.thread_pool_size:10}")
    private int threadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return threadPoolSize;
    }

}

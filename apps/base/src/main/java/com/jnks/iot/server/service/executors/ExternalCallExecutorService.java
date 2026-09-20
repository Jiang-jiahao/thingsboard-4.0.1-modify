package com.jnks.iot.server.service.executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.AbstractListeningExecutor;

@Component
public class ExternalCallExecutorService extends AbstractListeningExecutor {

    @Value("${actors.rule.external_call_thread_pool_size}")
    private int externalCallExecutorThreadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return externalCallExecutorThreadPoolSize;
    }

}


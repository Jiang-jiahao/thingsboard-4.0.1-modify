package com.jnks.iot.server.service.executors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.AbstractListeningExecutor;

@Component
public class VersionControlExecutor extends AbstractListeningExecutor {

    @Value("${vc.thread_pool_size:6}")
    private int threadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return threadPoolSize;
    }
}

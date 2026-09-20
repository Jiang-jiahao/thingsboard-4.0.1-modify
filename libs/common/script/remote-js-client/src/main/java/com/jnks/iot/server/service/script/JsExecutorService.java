package com.jnks.iot.server.service.script;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.AbstractListeningExecutor;

@Component
public class JsExecutorService extends AbstractListeningExecutor {

    @Value("${js.remote.js_thread_pool_size:50}")
    private int jsExecutorThreadPoolSize;

    @Override
    protected int getThreadPollSize() {
        return Math.max(jsExecutorThreadPoolSize, 1);
    }

}

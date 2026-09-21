package com.jnks.iot.server.service.executors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.ExecutorProvider;
import com.jnks.iot.common.util.JnksIotExecutors;
import java.util.concurrent.ScheduledExecutorService;

@Lazy
@Component
public class PubSubRuleNodeExecutorProvider implements ExecutorProvider {

    @Value("${service.rule_engine.pubsub.executor_thread_pool_size}")
    private Integer threadPoolSize;

    /**
    * Refers to com.google.cloud.pubsub.v1.Publisher default executor configuration
    */
    private static final int THREADS_PER_CPU = 5;
    private ScheduledExecutorService executor;

    @PostConstruct
    public void init() {
        if (threadPoolSize == null) {
            threadPoolSize = THREADS_PER_CPU * Runtime.getRuntime().availableProcessors();
        }
        executor = JnksIotExecutors.newScheduledThreadPool(threadPoolSize, "pubsub-rule-nodes");
    }

    @Override
    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    @PreDestroy
    private void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}

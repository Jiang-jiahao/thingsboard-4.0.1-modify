package com.jnks.iot.server.service.subscription;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JnksIotExecutors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
public class SubscriptionSchedulerComponent {

    @Getter
    private ScheduledExecutorService scheduler;

    @PostConstruct
    public void initExecutor() {
        scheduler = JnksIotExecutors.newSingleThreadScheduledExecutor("subscription-scheduler");
    }

    @PreDestroy
    public void shutdownExecutor() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return scheduler.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
}

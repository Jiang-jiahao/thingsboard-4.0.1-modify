package com.jnks.iot.server.dao.sql;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import com.jnks.iot.common.util.JnksIotExecutors;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class ScheduledLogExecutorComponent {

    private ScheduledExecutorService schedulerLogExecutor;

    @PostConstruct
    public void init() {
        schedulerLogExecutor = JnksIotExecutors.newSingleThreadScheduledExecutor("sql-log");
    }

    @PreDestroy
    public void stop() {
        if (schedulerLogExecutor != null) {
            schedulerLogExecutor.shutdownNow();
        }
    }

    public void scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        schedulerLogExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }
}

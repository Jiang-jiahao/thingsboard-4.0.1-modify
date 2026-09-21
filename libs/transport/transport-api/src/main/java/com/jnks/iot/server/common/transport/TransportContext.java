package com.jnks.iot.server.common.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.server.cache.ota.OtaPackageDataCache;
import com.jnks.iot.server.common.transport.limits.TransportRateLimitService;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.server.common.scheduler.SchedulerComponent;

import java.util.concurrent.ExecutorService;

/**
 * Created by ashvayka on 15.10.18.
 */
@Slf4j
@Data
public abstract class TransportContext {

    protected final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    protected TransportService transportService;

    @Autowired
    private JnksIotServiceInfoProvider serviceInfoProvider;

    @Autowired
    private SchedulerComponent scheduler;

    @Getter
    private ExecutorService executor;

    @Getter
    @Autowired
    private OtaPackageDataCache otaPackageDataCache;

    @Autowired
    private TransportResourceCache transportResourceCache;

    @Autowired
    protected TransportRateLimitService rateLimitService;

    @PostConstruct
    public void init() {
        executor = JnksIotExecutors.newWorkStealingPool(50, getClass());
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    public String getNodeId() {
        return serviceInfoProvider.getServiceId();
    }



}

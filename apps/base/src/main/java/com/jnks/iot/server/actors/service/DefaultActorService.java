package com.jnks.iot.server.actors.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Service;
import com.jnks.iot.common.util.JnksIotExecutors;
import com.jnks.iot.common.util.JnksIotThreadFactory;
import com.jnks.iot.server.actors.ActorSystemContext;
import com.jnks.iot.server.actors.DefaultJnksIotActorSystem;
import com.jnks.iot.server.actors.JnksIotActorRef;
import com.jnks.iot.server.actors.JnksIotActorSystem;
import com.jnks.iot.server.actors.JnksIotActorSystemSettings;
import com.jnks.iot.server.actors.app.AppActor;
import com.jnks.iot.server.actors.app.AppInitMsg;
import com.jnks.iot.server.actors.stats.StatsActor;
import com.jnks.iot.server.common.msg.queue.PartitionChangeMsg;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.queue.discovery.JnksIotApplicationEventListener;
import com.jnks.iot.server.queue.discovery.event.PartitionChangeEvent;
import com.jnks.iot.common.util.AfterStartUp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
public class DefaultActorService extends JnksIotApplicationEventListener<PartitionChangeEvent> implements ActorService {

    public static final String APP_DISPATCHER_NAME = "app-dispatcher";
    public static final String TENANT_DISPATCHER_NAME = "tenant-dispatcher";
    public static final String DEVICE_DISPATCHER_NAME = "device-dispatcher";
    public static final String RULE_DISPATCHER_NAME = "rule-dispatcher";
    public static final String CF_MANAGER_DISPATCHER_NAME = "cf-manager-dispatcher";
    public static final String CF_ENTITY_DISPATCHER_NAME = "cf-entity-dispatcher";

    @Autowired
    private ActorSystemContext actorContext;

    private JnksIotActorSystem system;

    private JnksIotActorRef appActor;

    @Value("${actors.system.throughput:5}")
    private int actorThroughput;

    @Value("${actors.system.max_actor_init_attempts:10}")
    private int maxActorInitAttempts;

    @Value("${actors.system.scheduler_pool_size:1}")
    private int schedulerPoolSize;

    @Value("${actors.system.app_dispatcher_pool_size:1}")
    private int appDispatcherSize;

    @Value("${actors.system.tenant_dispatcher_pool_size:2}")
    private int tenantDispatcherSize;

    @Value("${actors.system.device_dispatcher_pool_size:4}")
    private int deviceDispatcherSize;

    @Value("${actors.system.rule_dispatcher_pool_size:8}")
    private int ruleDispatcherSize;

    @Value("${actors.system.cfm_dispatcher_pool_size:2}")
    private int calculatedFieldManagerDispatcherSize;

    @Value("${actors.system.cfe_dispatcher_pool_size:8}")
    private int calculatedFieldEntityDispatcherSize;


    @PostConstruct
    public void initActorSystem() {
        log.info("Initializing actor system.");
        actorContext.setActorService(this);
        JnksIotActorSystemSettings settings = new JnksIotActorSystemSettings(actorThroughput, schedulerPoolSize, maxActorInitAttempts);
        system = new DefaultJnksIotActorSystem(settings);

        system.createDispatcher(APP_DISPATCHER_NAME, initDispatcherExecutor(APP_DISPATCHER_NAME, appDispatcherSize));
        system.createDispatcher(TENANT_DISPATCHER_NAME, initDispatcherExecutor(TENANT_DISPATCHER_NAME, tenantDispatcherSize));
        system.createDispatcher(DEVICE_DISPATCHER_NAME, initDispatcherExecutor(DEVICE_DISPATCHER_NAME, deviceDispatcherSize));
        system.createDispatcher(RULE_DISPATCHER_NAME, initDispatcherExecutor(RULE_DISPATCHER_NAME, ruleDispatcherSize));
        system.createDispatcher(CF_MANAGER_DISPATCHER_NAME, initDispatcherExecutor(CF_MANAGER_DISPATCHER_NAME, calculatedFieldManagerDispatcherSize));
        system.createDispatcher(CF_ENTITY_DISPATCHER_NAME, initDispatcherExecutor(CF_ENTITY_DISPATCHER_NAME, calculatedFieldEntityDispatcherSize));

        actorContext.setActorSystem(system);
        // 创建appActor
        appActor = system.createRootActor(APP_DISPATCHER_NAME, new AppActor.ActorCreator(actorContext));
        actorContext.setAppActor(appActor);
        // 创建stateActor（主要用于收集和聚合性能指标）
        JnksIotActorRef statsActor = system.createRootActor(TENANT_DISPATCHER_NAME, new StatsActor.ActorCreator(actorContext, "StatsActor"));
        actorContext.setStatsActor(statsActor);

        log.info("Actor system initialized.");
    }

    private ExecutorService initDispatcherExecutor(String dispatcherName, int poolSize) {
        if (poolSize == 0) {
            int cores = Runtime.getRuntime().availableProcessors();
            poolSize = Math.max(1, cores / 2);
        }
        if (poolSize == 1) {
            return Executors.newSingleThreadExecutor(JnksIotThreadFactory.forName(dispatcherName));
        } else {
            return JnksIotExecutors.newWorkStealingPool(poolSize, dispatcherName);
        }
    }

    @AfterStartUp(order = AfterStartUp.ACTOR_SYSTEM)
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        log.info("Received application ready event. Sending application init message to actor system");
        appActor.tellWithHighPriority(new AppInitMsg());
    }

    @Override
    protected void onJnksIotApplicationEvent(PartitionChangeEvent event) {
        log.info("Received partition change event.");
        appActor.tellWithHighPriority(new PartitionChangeMsg(event.getServiceType()));
    }

    @Override
    protected boolean filterJnksIotApplicationEvent(PartitionChangeEvent event) {
        return event.getServiceType() == ServiceType.JNKS_IOT_RULE_ENGINE || event.getServiceType() == ServiceType.JNKS_IOT_CORE;
    }

    @PreDestroy
    public void stopActorSystem() {
        if (system != null) {
            log.info("Stopping actor system.");
            system.stop();
            log.info("Actor system stopped.");
        }
    }

}

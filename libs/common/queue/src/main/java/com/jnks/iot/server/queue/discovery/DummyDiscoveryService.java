package com.jnks.iot.server.queue.discovery;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.common.util.AfterStartUp;

import java.util.Collections;
import java.util.List;

/**
 * 假的服务发现，没有相关触发分区再分配的事件，只会在开始的时候执行一次
 * 用于在不使用zk的时候，单体模式能正常启动
 */
@Service
@ConditionalOnProperty(prefix = "zk", value = "enabled", havingValue = "false", matchIfMissing = true)
@Slf4j
@DependsOn("environmentLogService")
public class DummyDiscoveryService implements DiscoveryService {

    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final PartitionService partitionService;


    public DummyDiscoveryService(JnksIotServiceInfoProvider serviceInfoProvider, PartitionService partitionService) {
        this.serviceInfoProvider = serviceInfoProvider;
        this.partitionService = partitionService;
    }

    @AfterStartUp(order = AfterStartUp.DISCOVERY_SERVICE)
    public void onApplicationEvent(ApplicationReadyEvent event) {
        partitionService.recalculatePartitions(serviceInfoProvider.getServiceInfo(), Collections.emptyList());
    }

    @Override
    public List<TransportProtos.ServiceInfo> getOtherServers() {
        return Collections.emptyList();
    }

    @Override
    public boolean isMonolith() {
        return true;
    }
}

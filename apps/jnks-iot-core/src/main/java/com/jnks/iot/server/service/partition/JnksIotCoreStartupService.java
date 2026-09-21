package com.jnks.iot.server.service.partition;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.cluster.JnksIotClusterService;
import com.jnks.iot.server.common.msg.queue.ServiceType;
import com.jnks.iot.server.gen.transport.TransportProtos;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.QueueKey;
import com.jnks.iot.server.queue.discovery.JnksIotServiceInfoProvider;
import com.jnks.iot.common.util.AfterStartUp;
/**
 * jnks-iot-core 启动广播服务：本节点就绪后，向集群其他 Core 广播自身负责的主队列分区。
 * <p>
 * <b>职责：</b>读取本实例 JNKS_IOT_CORE 分区，封装 {@code CoreStartupMsg} 并 {@code broadcastToCore}。
 * <p>
 * <b>触发方式：</b>应用启动（{@code @AfterStartUp} / {@link ApplicationReadyEvent}）。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class JnksIotCoreStartupService {

    private final PartitionService partitionService;
    private final JnksIotServiceInfoProvider serviceInfoProvider;
    private final JnksIotClusterService clusterService;

    /** 启动完成后向其他 Core 广播本节点分区。 */
    @AfterStartUp(order = AfterStartUp.STARTUP_SERVICE)
    public void onApplicationEvent(ApplicationReadyEvent event) {
        // 获取当前服务实例负责的核心主队列分区
        var myPartitions = partitionService.getMyPartitions(new QueueKey(ServiceType.JNKS_IOT_CORE));
        if (myPartitions != null && !myPartitions.isEmpty()) {
            TransportProtos.ToCoreNotificationMsg toCoreMsg = TransportProtos.ToCoreNotificationMsg.newBuilder()
                    .setCoreStartupMsg(TransportProtos.CoreStartupMsg.newBuilder()
                            .setServiceId(serviceInfoProvider.getServiceId()).addAllPartitions(myPartitions).build()).build();
            clusterService.broadcastToCore(toCoreMsg);
        }
    }

}

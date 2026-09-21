package com.jnks.iot.transport.base;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import com.jnks.iot.server.common.transport.TransportService;
import com.jnks.iot.server.gen.transport.TransportProtos.GetAllQueueRoutingInfoRequestMsg;
import com.jnks.iot.server.queue.discovery.QueueRoutingInfo;
import com.jnks.iot.server.queue.discovery.QueueRoutingInfoService;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 在传输服务启动时初始化队列路由信息
 */
@Slf4j
@Service
public class TransportQueueRoutingInfoService implements QueueRoutingInfoService {

    private final TransportService transportService;

    public TransportQueueRoutingInfoService(@Lazy TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public List<QueueRoutingInfo> getAllQueuesRoutingInfo() {
        GetAllQueueRoutingInfoRequestMsg msg = GetAllQueueRoutingInfoRequestMsg.newBuilder().build();
        return transportService.getQueueRoutingInfo(msg).stream().map(QueueRoutingInfo::new).collect(Collectors.toList());
    }
}

package com.jnks.iot.server.transport.tcp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.gen.transport.TransportProtos.ServiceInfo;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;
import com.jnks.iot.server.transport.tcp.event.TcpTransportListChangedEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled:true}'=='true'")
@Service
@Slf4j
public class TcpTransportBalancingService {
    private final PartitionService partitionService;
    private final ApplicationEventPublisher eventPublisher;

    private final TcpTransportService tcpTransportService;
    private int tcpTransportsCount = 1;
    private Integer currentTransportPartitionIndex = 0;
    private List<String> lastTcpServiceIds = List.of();

    public TcpTransportBalancingService(PartitionService partitionService,
                                        ApplicationEventPublisher eventPublisher,
                                        @Lazy TcpTransportService tcpTransportService) {
        this.partitionService = partitionService;
        this.eventPublisher = eventPublisher;
        this.tcpTransportService = tcpTransportService;
    }
    public void onServiceListChanged(ServiceListChangedEvent event) {
        log.trace("Got service list changed event: {}", event);
        recalculatePartitions(event.getOtherServices(), event.getCurrentService());
    }
    public boolean isManagedByCurrentTransport(UUID entityId) {
        boolean isManaged = resolvePartitionIndexForEntity(entityId) == currentTransportPartitionIndex;
        if (!isManaged) {
            log.trace("Entity {} is not managed by current TCP transport node", entityId);
        }
        return isManaged;
    }
    private int resolvePartitionIndexForEntity(UUID entityId) {
        return partitionService.resolvePartitionIndex(entityId, tcpTransportsCount);
    }
    private void recalculatePartitions(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        List<ServiceInfo> tcpTransports = uniqueTcpTransports(otherServices, currentService);
        log.trace("Found TCP transports: {}", tcpTransports);
        int previousCurrentTransportPartitionIndex = currentTransportPartitionIndex;
        int previousTcpTransportsCount = tcpTransportsCount;
        List<String> previousServiceIds = lastTcpServiceIds;
        currentTransportPartitionIndex = 0;
        for (int i = 0; i < tcpTransports.size(); i++) {
            if (tcpTransports.get(i).getServiceId().equals(currentService.getServiceId())) {
                currentTransportPartitionIndex = i;
                break;
            }
        }
        tcpTransportsCount = Math.max(1, tcpTransports.size());
        lastTcpServiceIds = tcpTransports.stream().map(ServiceInfo::getServiceId).collect(Collectors.toList());
        if (tcpTransportsCount != previousTcpTransportsCount
                || currentTransportPartitionIndex != previousCurrentTransportPartitionIndex
                || !lastTcpServiceIds.equals(previousServiceIds)) {
            log.info("TCP transports partitions have changed: transports count = {}, current transport partition index = {}",
                    tcpTransportsCount, currentTransportPartitionIndex);
            eventPublisher.publishEvent(new TcpTransportListChangedEvent());
        } else {
            log.info("TCP transports partitions have not changed");
        }
    }

    /**
     * 只按 serviceId 计 TCP 节点。ZK 里可能残留同 ID 的旧 ephemeral（会话异常时未及时清除），
     * 或对端刚注册时还没有 transports。按条目计数会多出没人认领的分区索引，
     * 落到该索引的设备不会被任何实例建立会话，RPC 也无人处理。
     */
    List<ServiceInfo> uniqueTcpTransports(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        String tcpName = tcpTransportService.getName();
        Map<String, ServiceInfo> byId = Stream.concat(otherServices.stream(), Stream.of(currentService))
                .filter(service -> isTcpTransportNode(service, currentService, tcpName))
                .collect(Collectors.toMap(
                        ServiceInfo::getServiceId,
                        service -> service,
                        (left, right) -> left.getTransportsCount() >= right.getTransportsCount() ? left : right,
                        LinkedHashMap::new));
        if (!byId.containsKey(currentService.getServiceId())) {
            byId.put(currentService.getServiceId(), currentService);
        }
        List<ServiceInfo> tcpTransports = new ArrayList<>(byId.values());
        tcpTransports.sort(Comparator.comparing(ServiceInfo::getServiceId));
        return tcpTransports;
    }

    static boolean isTcpTransportNode(ServiceInfo service, ServiceInfo currentService, String tcpName) {
        if (service.getServiceId().equals(currentService.getServiceId())) {
            return true;
        }
        if (service.getTransportsList().contains(tcpName)) {
            return true;
        }
        return sameReplicaFamily(currentService.getServiceId(), service.getServiceId());
    }

    static boolean sameReplicaFamily(String left, String right) {
        if (left == null || right == null) {
            return false;
        }
        String leftFamily = left.replaceFirst("\\d+$", "");
        String rightFamily = right.replaceFirst("\\d+$", "");
        return !leftFamily.isEmpty() && leftFamily.equals(rightFamily);
    }
}

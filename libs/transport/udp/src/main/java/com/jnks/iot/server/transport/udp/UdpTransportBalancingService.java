package com.jnks.iot.server.transport.udp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import com.jnks.iot.server.gen.transport.TransportProtos.ServiceInfo;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;
import com.jnks.iot.server.transport.udp.event.UdpTransportListChangedEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.udp.enabled:true}'=='true'")
@Service
@Slf4j
public class UdpTransportBalancingService {
    private final PartitionService partitionService;
    private final ApplicationEventPublisher eventPublisher;

    private final UdpTransportService udpTransportService;
    private int udpTransportsCount = 1;
    private Integer currentTransportPartitionIndex = 0;
    private List<String> lastUdpServiceIds = List.of();

    public UdpTransportBalancingService(PartitionService partitionService,
                                        ApplicationEventPublisher eventPublisher,
                                        @Lazy UdpTransportService udpTransportService) {
        this.partitionService = partitionService;
        this.eventPublisher = eventPublisher;
        this.udpTransportService = udpTransportService;
    }
    public void onServiceListChanged(ServiceListChangedEvent event) {
        log.trace("Got service list changed event: {}", event);
        recalculatePartitions(event.getOtherServices(), event.getCurrentService());
    }
    public boolean isManagedByCurrentTransport(UUID entityId) {
        boolean isManaged = resolvePartitionIndexForEntity(entityId) == currentTransportPartitionIndex;
        if (!isManaged) {
            log.trace("Entity {} is not managed by current UDP transport node", entityId);
        }
        return isManaged;
    }
    private int resolvePartitionIndexForEntity(UUID entityId) {
        return partitionService.resolvePartitionIndex(entityId, udpTransportsCount);
    }
    private void recalculatePartitions(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        List<ServiceInfo> udpTransports = uniqueUdpTransports(otherServices, currentService);
        log.trace("Found UDP transports: {}", udpTransports);
        int previousCurrentTransportPartitionIndex = currentTransportPartitionIndex;
        int previousUdpTransportsCount = udpTransportsCount;
        List<String> previousServiceIds = lastUdpServiceIds;
        currentTransportPartitionIndex = 0;
        for (int i = 0; i < udpTransports.size(); i++) {
            if (udpTransports.get(i).getServiceId().equals(currentService.getServiceId())) {
                currentTransportPartitionIndex = i;
                break;
            }
        }
        udpTransportsCount = Math.max(1, udpTransports.size());
        lastUdpServiceIds = udpTransports.stream().map(ServiceInfo::getServiceId).collect(Collectors.toList());
        if (udpTransportsCount != previousUdpTransportsCount
                || currentTransportPartitionIndex != previousCurrentTransportPartitionIndex
                || !lastUdpServiceIds.equals(previousServiceIds)) {
            log.info("UDP transports partitions have changed: transports count = {}, current transport partition index = {}",
                    udpTransportsCount, currentTransportPartitionIndex);
            eventPublisher.publishEvent(new UdpTransportListChangedEvent());
        } else {
            log.info("UDP transports partitions have not changed");
        }
    }

    /**
     * 只按 serviceId 计 UDP 节点。ZK 里可能残留同 ID 的旧 ephemeral（会话异常时未及时清除），
     * 或对端刚注册时还没有 transports。按条目计数会多出没人认领的分区索引，
     * 落到该索引的设备既不会被任何实例建出站会话，RPC 也会静默丢弃。
     */
    List<ServiceInfo> uniqueUdpTransports(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        String udpName = udpTransportService.getName();
        Map<String, ServiceInfo> byId = Stream.concat(otherServices.stream(), Stream.of(currentService))
                .filter(service -> isUdpTransportNode(service, currentService, udpName))
                .collect(Collectors.toMap(
                        ServiceInfo::getServiceId,
                        service -> service,
                        (left, right) -> left.getTransportsCount() >= right.getTransportsCount() ? left : right,
                        LinkedHashMap::new));
        if (!byId.containsKey(currentService.getServiceId())) {
            byId.put(currentService.getServiceId(), currentService);
        }
        List<ServiceInfo> udpTransports = new ArrayList<>(byId.values());
        udpTransports.sort(Comparator.comparing(ServiceInfo::getServiceId));
        return udpTransports;
    }

    static boolean isUdpTransportNode(ServiceInfo service, ServiceInfo currentService, String udpName) {
        if (service.getServiceId().equals(currentService.getServiceId())) {
            return true;
        }
        if (service.getTransportsList().contains(udpName)) {
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

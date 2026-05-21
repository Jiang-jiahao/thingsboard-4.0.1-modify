/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.transport.udp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.queue.util.TbUdpTransportComponent;
import org.thingsboard.server.transport.udp.event.UdpTransportListChangedEvent;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
@TbUdpTransportComponent
@Service
@Slf4j
public class UdpTransportBalancingService {
    private final PartitionService partitionService;
    private final ApplicationEventPublisher eventPublisher;

    private final UdpTransportService udpTransportService;
    private int tcpTransportsCount = 1;
    private Integer currentTransportPartitionIndex = 0;


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
        return partitionService.resolvePartitionIndex(entityId, tcpTransportsCount);
    }
    private void recalculatePartitions(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        log.info("Recalculating partitions for UDP transports");
        List<ServiceInfo> tcpTransports = Stream.concat(otherServices.stream(), Stream.of(currentService))
                .filter(service -> service.getTransportsList().contains(udpTransportService.getName()))
                .sorted(Comparator.comparing(ServiceInfo::getServiceId))
                .collect(Collectors.toList());
        log.trace("Found UDP transports: {}", tcpTransports);
        int previousCurrentTransportPartitionIndex = currentTransportPartitionIndex;
        int previousTcpTransportsCount = tcpTransportsCount;
        if (!tcpTransports.isEmpty()) {
            for (int i = 0; i < tcpTransports.size(); i++) {
                if (tcpTransports.get(i).equals(currentService)) {
                    currentTransportPartitionIndex = i;
                    break;
                }
            }
            tcpTransportsCount = tcpTransports.size();
        }
        if (tcpTransportsCount != previousTcpTransportsCount || currentTransportPartitionIndex != previousCurrentTransportPartitionIndex) {
            log.info("UDP transports partitions have changed: transports count = {}, current transport partition index = {}", tcpTransportsCount, currentTransportPartitionIndex);
            eventPublisher.publishEvent(new UdpTransportListChangedEvent());
        } else {
            log.info("UDP transports partitions have not changed");
        }
    }
}

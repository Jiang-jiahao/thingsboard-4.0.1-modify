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
package org.thingsboard.server.transport.http;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.transport.http.event.HttpTransportListChangedEvent;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.http.enabled:true}'=='true'")
@Service
@Slf4j
public class HttpTransportBalancingService {

    private final PartitionService partitionService;
    private final ApplicationEventPublisher eventPublisher;

    private int httpTransportsCount = 1;
    private int currentTransportPartitionIndex = 0;

    public HttpTransportBalancingService(PartitionService partitionService,
                                         ApplicationEventPublisher eventPublisher) {
        this.partitionService = partitionService;
        this.eventPublisher = eventPublisher;
    }

    public void onServiceListChanged(ServiceListChangedEvent event) {
        recalculatePartitions(event.getOtherServices(), event.getCurrentService());
    }

    public boolean isManagedByCurrentTransport(UUID entityId) {
        return resolvePartitionIndexForEntity(entityId) == currentTransportPartitionIndex;
    }

    private int resolvePartitionIndexForEntity(UUID entityId) {
        return partitionService.resolvePartitionIndex(entityId, httpTransportsCount);
    }

    private void recalculatePartitions(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        log.info("Recalculating partitions for HTTP transports");
        List<ServiceInfo> httpTransports = Stream.concat(otherServices.stream(), Stream.of(currentService))
                .filter(service -> service.getTransportsList().contains(DataConstants.HTTP_TRANSPORT_NAME))
                .sorted(Comparator.comparing(ServiceInfo::getServiceId))
                .collect(Collectors.toList());
        int previousIndex = currentTransportPartitionIndex;
        int previousCount = httpTransportsCount;
        if (!httpTransports.isEmpty()) {
            for (int i = 0; i < httpTransports.size(); i++) {
                if (httpTransports.get(i).getServiceId().equals(currentService.getServiceId())) {
                    currentTransportPartitionIndex = i;
                    break;
                }
            }
            httpTransportsCount = httpTransports.size();
        }
        if (httpTransportsCount != previousCount || currentTransportPartitionIndex != previousIndex) {
            log.info("HTTP transports partitions have changed: transports count = {}, current transport partition index = {}",
                    httpTransportsCount, currentTransportPartitionIndex);
            eventPublisher.publishEvent(new HttpTransportListChangedEvent());
        } else {
            log.info("HTTP transports partitions have not changed");
        }
    }
}

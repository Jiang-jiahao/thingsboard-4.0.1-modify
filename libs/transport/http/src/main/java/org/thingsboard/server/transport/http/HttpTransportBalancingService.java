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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.http.enabled:true}'=='true'")
@Service
@Slf4j
public class HttpTransportBalancingService {

    private final PartitionService partitionService;
    private final ApplicationEventPublisher eventPublisher;

    private volatile int httpTransportsCount = 1;
    private volatile int currentTransportPartitionIndex = 0;
    private volatile List<String> lastHttpServiceIds = List.of();

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
        return partitionService.resolvePartitionIndex(entityId, Math.max(1, httpTransportsCount));
    }

    private void recalculatePartitions(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        List<ServiceInfo> httpTransports = uniqueHttpTransports(otherServices, currentService);
        log.info("Recalculating partitions for HTTP transports: {}",
                httpTransports.stream()
                        .map(service -> service.getServiceId() + service.getTransportsList())
                        .collect(Collectors.toList()));
        int previousIndex = currentTransportPartitionIndex;
        int previousCount = httpTransportsCount;
        List<String> previousIds = lastHttpServiceIds;
        currentTransportPartitionIndex = 0;
        for (int i = 0; i < httpTransports.size(); i++) {
            if (httpTransports.get(i).getServiceId().equals(currentService.getServiceId())) {
                currentTransportPartitionIndex = i;
                break;
            }
        }
        httpTransportsCount = Math.max(1, httpTransports.size());
        lastHttpServiceIds = httpTransports.stream().map(ServiceInfo::getServiceId).collect(Collectors.toList());
        if (httpTransportsCount != previousCount
                || currentTransportPartitionIndex != previousIndex
                || !lastHttpServiceIds.equals(previousIds)) {
            log.info("HTTP transports partitions have changed: transports count = {}, current transport partition index = {}",
                    httpTransportsCount, currentTransportPartitionIndex);
            eventPublisher.publishEvent(new HttpTransportListChangedEvent());
        } else {
            log.info("HTTP transports partitions have not changed");
        }
    }

    List<ServiceInfo> uniqueHttpTransports(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        Map<String, ServiceInfo> byId = Stream.concat(otherServices.stream(), Stream.of(currentService))
                .filter(service -> isHttpTransportNode(service, currentService))
                .collect(Collectors.toMap(
                        ServiceInfo::getServiceId,
                        service -> service,
                        (left, right) -> left.getTransportsCount() >= right.getTransportsCount() ? left : right,
                        LinkedHashMap::new));
        if (!byId.containsKey(currentService.getServiceId())) {
            byId.put(currentService.getServiceId(), currentService);
        }
        List<ServiceInfo> httpTransports = new ArrayList<>(byId.values());
        httpTransports.sort(Comparator.comparing(ServiceInfo::getServiceId));
        return httpTransports;
    }

    static boolean isHttpTransportNode(ServiceInfo service, ServiceInfo currentService) {
        if (service.getServiceId().equals(currentService.getServiceId())) {
            return true;
        }
        if (service.getTransportsList().contains(DataConstants.HTTP_TRANSPORT_NAME)) {
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

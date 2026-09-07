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
package org.thingsboard.server.transport.mqtt;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;
import org.thingsboard.server.transport.mqtt.event.MqttTransportListChangedEvent;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ConditionalOnExpression("'${transport.api_enabled:true}'=='true' && '${transport.mqtt.enabled:true}'=='true'")
@Service
@Slf4j
public class MqttTransportBalancingService {

    private final PartitionService partitionService;
    private final ApplicationEventPublisher eventPublisher;
    private final MqttTransportService mqttTransportService;

    private int mqttTransportsCount = 1;
    private int currentTransportPartitionIndex = 0;

    public MqttTransportBalancingService(PartitionService partitionService,
                                         ApplicationEventPublisher eventPublisher,
                                         @Lazy MqttTransportService mqttTransportService) {
        this.partitionService = partitionService;
        this.eventPublisher = eventPublisher;
        this.mqttTransportService = mqttTransportService;
    }

    public void onServiceListChanged(ServiceListChangedEvent event) {
        recalculatePartitions(event.getOtherServices(), event.getCurrentService());
    }

    public boolean isManagedByCurrentTransport(UUID entityId) {
        return resolvePartitionIndexForEntity(entityId) == currentTransportPartitionIndex;
    }

    private int resolvePartitionIndexForEntity(UUID entityId) {
        return partitionService.resolvePartitionIndex(entityId, mqttTransportsCount);
    }

    private void recalculatePartitions(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        log.info("Recalculating partitions for MQTT transports");
        List<ServiceInfo> mqttTransports = Stream.concat(otherServices.stream(), Stream.of(currentService))
                .filter(service -> service.getTransportsList().contains(mqttTransportService.getName()))
                .sorted(Comparator.comparing(ServiceInfo::getServiceId))
                .collect(Collectors.toList());
        int previousIndex = currentTransportPartitionIndex;
        int previousCount = mqttTransportsCount;
        if (!mqttTransports.isEmpty()) {
            for (int i = 0; i < mqttTransports.size(); i++) {
                if (mqttTransports.get(i).getServiceId().equals(currentService.getServiceId())) {
                    currentTransportPartitionIndex = i;
                    break;
                }
            }
            mqttTransportsCount = mqttTransports.size();
        }
        if (mqttTransportsCount != previousCount || currentTransportPartitionIndex != previousIndex) {
            log.info("MQTT transports partitions have changed: transports count = {}, current transport partition index = {}",
                    mqttTransportsCount, currentTransportPartitionIndex);
            eventPublisher.publishEvent(new MqttTransportListChangedEvent());
        } else {
            log.info("MQTT transports partitions have not changed");
        }
    }
}

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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private volatile int mqttTransportsCount = 1;
    private volatile int currentTransportPartitionIndex = 0;
    private volatile List<String> lastMqttServiceIds = List.of();

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
        return partitionService.resolvePartitionIndex(entityId, Math.max(1, mqttTransportsCount));
    }

    private void recalculatePartitions(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        List<ServiceInfo> mqttTransports = uniqueMqttTransports(otherServices, currentService);
        log.info("Recalculating partitions for MQTT transports: {}",
                mqttTransports.stream()
                        .map(service -> service.getServiceId() + service.getTransportsList())
                        .collect(Collectors.toList()));
        int previousIndex = currentTransportPartitionIndex;
        int previousCount = mqttTransportsCount;
        List<String> previousIds = lastMqttServiceIds;
        currentTransportPartitionIndex = 0;
        for (int i = 0; i < mqttTransports.size(); i++) {
            if (mqttTransports.get(i).getServiceId().equals(currentService.getServiceId())) {
                currentTransportPartitionIndex = i;
                break;
            }
        }
        mqttTransportsCount = Math.max(1, mqttTransports.size());
        lastMqttServiceIds = mqttTransports.stream().map(ServiceInfo::getServiceId).collect(Collectors.toList());
        if (mqttTransportsCount != previousCount
                || currentTransportPartitionIndex != previousIndex
                || !lastMqttServiceIds.equals(previousIds)) {
            log.info("MQTT transports partitions have changed: transports count = {}, current transport partition index = {}",
                    mqttTransportsCount, currentTransportPartitionIndex);
            eventPublisher.publishEvent(new MqttTransportListChangedEvent());
        } else {
            log.info("MQTT transports partitions have not changed");
        }
    }

    /**
     * 只按 serviceId 计 MQTT 节点。ZK 里可能残留同 ID 的旧 ephemeral，
     * 或对端刚注册时还没有 transports，都会让本机仍按 count=1 独占全部设备。
     */
    List<ServiceInfo> uniqueMqttTransports(List<ServiceInfo> otherServices, ServiceInfo currentService) {
        String mqttName = mqttTransportService.getName();
        Map<String, ServiceInfo> byId = Stream.concat(otherServices.stream(), Stream.of(currentService))
                .filter(service -> isMqttTransportNode(service, currentService, mqttName))
                .collect(Collectors.toMap(
                        ServiceInfo::getServiceId,
                        service -> service,
                        (left, right) -> left.getTransportsCount() >= right.getTransportsCount() ? left : right,
                        LinkedHashMap::new));
        if (!byId.containsKey(currentService.getServiceId())) {
            byId.put(currentService.getServiceId(), currentService);
        }
        List<ServiceInfo> mqttTransports = new ArrayList<>(byId.values());
        mqttTransports.sort(Comparator.comparing(ServiceInfo::getServiceId));
        return mqttTransports;
    }

    static boolean isMqttTransportNode(ServiceInfo service, ServiceInfo currentService, String mqttName) {
        if (service.getServiceId().equals(currentService.getServiceId())) {
            return true;
        }
        if (service.getTransportsList().contains(mqttName)) {
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

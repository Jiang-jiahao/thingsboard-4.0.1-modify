/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.gen.transport.TransportProtos.ServiceInfo;
import org.thingsboard.server.queue.discovery.PartitionService;
import org.thingsboard.server.queue.discovery.event.ServiceListChangedEvent;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MqttTransportBalancingServiceTest {

    private static final UUID DEVICE_A = UUID.fromString("67b80140-a7fe-11f1-a933-b9755a0ddd74");
    private static final UUID DEVICE_B = UUID.fromString("875eb610-ab5f-11f1-a6db-ddefc5643bf3");

    @Mock
    private PartitionService partitionService;
    @Mock
    private ApplicationEventPublisher eventPublisher;
    @Mock
    private MqttTransportService mqttTransportService;

    private MqttTransportBalancingService balancing;

    @BeforeEach
    void setUp() {
        when(mqttTransportService.getName()).thenReturn(DataConstants.MQTT_TRANSPORT_NAME);
        when(partitionService.resolvePartitionIndex(any(UUID.class), anyInt())).thenAnswer(invocation -> {
            UUID id = invocation.getArgument(0);
            int partitions = invocation.getArgument(1);
            if (partitions <= 1) {
                return 0;
            }
            return DEVICE_B.equals(id) ? 1 : 0;
        });
        balancing = new MqttTransportBalancingService(partitionService, eventPublisher, mqttTransportService);
    }

    @Test
    void soleMqttNodeOwnsEveryDevice() {
        ServiceInfo mqtt1 = mqttNode("tb-mqtt-transport1");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), mqtt1));

        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    @Test
    void twoMqttNodesSplitDevices() {
        ServiceInfo mqtt1 = mqttNode("tb-mqtt-transport1");
        ServiceInfo mqtt2 = mqttNode("tb-mqtt-transport2");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(mqtt2), mqtt1));

        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isFalse();
    }

    @Test
    void duplicateZkEntriesForSameServiceIdCountAsOneNode() {
        ServiceInfo mqtt1 = mqttNode("tb-mqtt-transport1");
        ServiceInfo ghost = mqttNode("tb-mqtt-transport1");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(ghost), mqtt1));

        assertThat(balancing.uniqueMqttTransports(List.of(ghost), mqtt1)).hasSize(1);
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    @Test
    void currentNodeWithoutTransportsStillOwnsEveryDevice() {
        ServiceInfo mqtt1 = ServiceInfo.newBuilder().setServiceId("tb-mqtt-transport1").build();
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), mqtt1));

        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    @Test
    void peerJoiningSplitsDevicesEvenWithoutTransports() {
        ServiceInfo mqtt1 = mqttNode("tb-mqtt-transport1");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), mqtt1));
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();

        ServiceInfo mqtt2 = ServiceInfo.newBuilder().setServiceId("tb-mqtt-transport2").build();
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(mqtt2), mqtt1));
        assertThat(balancing.uniqueMqttTransports(List.of(mqtt2), mqtt1)).hasSize(2);
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isFalse();
    }

    @Test
    void droppingPeerMqttNodeTakesOverTheOtherHalf() {
        ServiceInfo mqtt1 = mqttNode("tb-mqtt-transport1");
        ServiceInfo mqtt2 = mqttNode("tb-mqtt-transport2");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(mqtt2), mqtt1));
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isFalse();

        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), mqtt1));
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    private static ServiceInfo mqttNode(String serviceId) {
        return ServiceInfo.newBuilder()
                .setServiceId(serviceId)
                .addTransports(DataConstants.MQTT_TRANSPORT_NAME)
                .build();
    }
}

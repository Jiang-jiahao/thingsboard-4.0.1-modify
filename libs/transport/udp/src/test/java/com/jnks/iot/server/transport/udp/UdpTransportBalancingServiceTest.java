package com.jnks.iot.server.transport.udp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import com.jnks.iot.server.common.data.DataConstants;
import com.jnks.iot.server.gen.transport.TransportProtos.ServiceInfo;
import com.jnks.iot.server.queue.discovery.PartitionService;
import com.jnks.iot.server.queue.discovery.event.ServiceListChangedEvent;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UdpTransportBalancingServiceTest {

    private static final UUID DEVICE_A = UUID.fromString("67b80140-a7fe-11f1-a933-b9755a0ddd74");
    private static final UUID DEVICE_B = UUID.fromString("875eb610-ab5f-11f1-a6db-ddefc5643bf3");

    @Mock
    private PartitionService partitionService;
    @Mock
    private ApplicationEventPublisher eventPublisher;
    @Mock
    private UdpTransportService udpTransportService;

    private UdpTransportBalancingService balancing;

    @BeforeEach
    void setUp() {
        when(udpTransportService.getName()).thenReturn(DataConstants.UDP_TRANSPORT_NAME);
        when(partitionService.resolvePartitionIndex(any(UUID.class), anyInt())).thenAnswer(invocation -> {
            UUID id = invocation.getArgument(0);
            int partitions = invocation.getArgument(1);
            if (partitions <= 1) {
                return 0;
            }
            return DEVICE_B.equals(id) ? 1 : 0;
        });
        balancing = new UdpTransportBalancingService(partitionService, eventPublisher, udpTransportService);
    }

    @Test
    void soleUdpNodeOwnsEveryDevice() {
        ServiceInfo udp1 = udpNode("jnks-iot-udp-transport1");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), udp1));

        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    @Test
    void twoUdpNodesSplitDevices() {
        ServiceInfo udp1 = udpNode("jnks-iot-udp-transport1");
        ServiceInfo udp2 = udpNode("jnks-iot-udp-transport2");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(udp2), udp1));

        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isFalse();
    }

    /** 回归用例：ZK 残留同 serviceId 的旧节点时，不能多出一个没人认领的分区。 */
    @Test
    void duplicateZkEntriesForSameServiceIdCountAsOneNode() {
        ServiceInfo udp1 = udpNode("jnks-iot-udp-transport1");
        ServiceInfo ghost = udpNode("jnks-iot-udp-transport1");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(ghost), udp1));

        assertThat(balancing.uniqueUdpTransports(List.of(ghost), udp1)).hasSize(1);
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    @Test
    void threeEntriesWithOneDuplicateStillSplitIntoTwoPartitions() {
        ServiceInfo udp1 = udpNode("jnks-iot-udp-transport1");
        ServiceInfo udp2 = udpNode("jnks-iot-udp-transport2");
        ServiceInfo udp2Ghost = udpNode("jnks-iot-udp-transport2");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(udp2, udp2Ghost), udp1));

        assertThat(balancing.uniqueUdpTransports(List.of(udp2, udp2Ghost), udp1)).hasSize(2);
        // 两个分区都有归属：A 归 udp1，B 归 udp2
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isFalse();
    }

    @Test
    void currentNodeWithoutTransportsStillOwnsEveryDevice() {
        ServiceInfo udp1 = ServiceInfo.newBuilder().setServiceId("jnks-iot-udp-transport1").build();
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), udp1));

        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    @Test
    void peerJoiningSplitsDevicesEvenWithoutTransports() {
        ServiceInfo udp1 = udpNode("jnks-iot-udp-transport1");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), udp1));
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();

        ServiceInfo udp2 = ServiceInfo.newBuilder().setServiceId("jnks-iot-udp-transport2").build();
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(udp2), udp1));
        assertThat(balancing.uniqueUdpTransports(List.of(udp2), udp1)).hasSize(2);
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isFalse();
    }

    @Test
    void droppingPeerUdpNodeTakesOverTheOtherHalf() {
        ServiceInfo udp1 = udpNode("jnks-iot-udp-transport1");
        ServiceInfo udp2 = udpNode("jnks-iot-udp-transport2");
        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(udp2), udp1));
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isFalse();

        balancing.onServiceListChanged(new ServiceListChangedEvent(List.of(), udp1));
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_A)).isTrue();
        assertThat(balancing.isManagedByCurrentTransport(DEVICE_B)).isTrue();
    }

    private static ServiceInfo udpNode(String serviceId) {
        return ServiceInfo.newBuilder()
                .setServiceId(serviceId)
                .addTransports(DataConstants.UDP_TRANSPORT_NAME)
                .build();
    }
}

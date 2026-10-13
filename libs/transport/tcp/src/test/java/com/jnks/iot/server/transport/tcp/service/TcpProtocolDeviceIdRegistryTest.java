package com.jnks.iot.server.transport.tcp.service;

import org.junit.jupiter.api.Test;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.device.data.DeviceData;
import com.jnks.iot.server.common.data.device.data.TcpDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.transport.DeviceUpdatedEvent;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 协议设备 ID 的解析按**档案**分域：同一串值可被不同档案的不同设备复用。
 */
class TcpProtocolDeviceIdRegistryTest {

    private static Device device(DeviceProfileId profileId, String protocolDeviceId) {
        Device d = new Device(new DeviceId(UUID.randomUUID()));
        d.setDeviceProfileId(profileId);
        TcpDeviceTransportConfiguration tc = new TcpDeviceTransportConfiguration();
        tc.setTcpWireAuthPayloadDeviceId(protocolDeviceId);
        DeviceData data = new DeviceData();
        data.setTransportConfiguration(tc);
        d.setDeviceData(data);
        return d;
    }

    @Test
    void sameProtocolDeviceIdInTwoProfilesResolvesWithinItsOwnProfile() throws Exception {
        DeviceProfileId pa = new DeviceProfileId(UUID.randomUUID());
        DeviceProfileId pb = new DeviceProfileId(UUID.randomUUID());
        Device a1 = device(pa, "dupA");
        Device b1 = device(pb, "dupA");   // 与 a1 同值，不同档案

        TcpProtocolDeviceIdRegistry registry = new TcpProtocolDeviceIdRegistry(null);
        registry.onDeviceUpdated(new DeviceUpdatedEvent(a1));
        registry.onDeviceUpdated(new DeviceUpdatedEvent(b1));

        Optional<DeviceId> fromA = Optional.empty();
        for (int i = 0; i < 100 && fromA.isEmpty(); i++) {
            fromA = registry.findByProtocolDeviceId(pa, "dupA");
            if (fromA.isEmpty()) {
                Thread.sleep(20);
            }
        }

        assertEquals(a1.getId(), fromA.orElseThrow(), "档案 A 里 dupA 应解析到 a1");
        assertEquals(b1.getId(), registry.findByProtocolDeviceId(pb, "dupA").orElseThrow(),
                "档案 B 里同值 dupA 应解析到 b1");
        assertTrue(registry.findByProtocolDeviceId(new DeviceProfileId(UUID.randomUUID()), "dupA").isEmpty(),
                "别的档案里没有这个值");
    }
}

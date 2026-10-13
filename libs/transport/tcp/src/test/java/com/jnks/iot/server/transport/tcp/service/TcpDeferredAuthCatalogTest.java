package com.jnks.iot.server.transport.tcp.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.DeviceTransportType;
import com.jnks.iot.server.common.data.device.profile.DeviceProfileData;
import com.jnks.iot.server.common.data.device.profile.TcpDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.TcpTransportConnectMode;
import com.jnks.iot.server.common.data.device.profile.TcpWireAuthenticationMode;
import com.jnks.iot.server.common.data.id.DeviceProfileId;
import com.jnks.iot.server.common.transport.DeviceProfileUpdatedEvent;
import com.jnks.iot.server.common.transport.TransportDeviceProfileCache;
import com.jnks.iot.server.gen.transport.TransportProtos;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 两个档案共用同一个负载键名时，按**本会话监听端口**落回唯一档案。
 * （键名不必租户内唯一 —— 这是本次改动的核心。）
 */
class TcpDeferredAuthCatalogTest {

    private static final String KEY = "deviceId";

    private static DeviceProfile profile(String name, int port) {
        DeviceProfile p = new DeviceProfile(new DeviceProfileId(UUID.randomUUID()));
        p.setName(name);
        p.setTransportType(DeviceTransportType.TCP);
        TcpDeviceProfileTransportConfiguration tc = new TcpDeviceProfileTransportConfiguration();
        tc.setTcpTransportConnectMode(TcpTransportConnectMode.SERVER);
        tc.setTcpWireAuthenticationMode(TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID);
        tc.setTcpProfileServerBindPort(port);
        tc.setTcpDeferredWireAuthTokenJsonKey(KEY);
        DeviceProfileData data = new DeviceProfileData();
        data.setTransportConfiguration(tc);
        p.setProfileData(data);
        return p;
    }

    private static TransportDeviceProfileCache cacheOf(Map<DeviceProfileId, DeviceProfile> byId) {
        return new TransportDeviceProfileCache() {
            @Override
            public DeviceProfile getOrCreate(DeviceProfileId id, TransportProtos.DeviceProfileProto proto) {
                return byId.get(id);
            }

            @Override
            public DeviceProfile get(DeviceProfileId id) {
                return byId.get(id);
            }

            @Override
            public void put(DeviceProfile profile) {
                byId.put(profile.getId(), profile);
            }

            @Override
            public DeviceProfile put(TransportProtos.DeviceProfileProto proto) {
                return null;
            }

            @Override
            public void evict(DeviceProfileId id) {
                byId.remove(id);
            }
        };
    }

    @Test
    void match_resolvesProfileByListenPortNotByKeyAlone() throws Exception {
        DeviceProfile a = profile("dup-p1", 15921);
        DeviceProfile b = profile("dup-p2", 15922);
        Map<DeviceProfileId, DeviceProfile> byId = new HashMap<>();
        byId.put(a.getId(), a);
        byId.put(b.getId(), b);
        TcpDeferredAuthCatalog catalog = new TcpDeferredAuthCatalog(null, cacheOf(byId));

        catalog.onDeviceProfileUpdated(new DeviceProfileUpdatedEvent(a));
        catalog.onDeviceProfileUpdated(new DeviceProfileUpdatedEvent(b));

        JsonObject frame = JsonParser.parseString("{\"deviceId\":\"v1\",\"k\":1}").getAsJsonObject();

        // 目录异步装载：轮询到两个档案都进去为止
        TcpDeferredAuthCatalog.Match onA = null;
        for (int i = 0; i < 100 && onA == null; i++) {
            onA = catalog.match(frame, 15921).orElse(null);
            if (onA == null) {
                Thread.sleep(20);
            }
        }

        assertEquals(a.getId().getId(), onA.profile().getId().getId(), "端口 15921 应落到 dup-p1");
        assertEquals(KEY, onA.key());
        assertEquals("v1", onA.value());

        assertEquals(b.getId().getId(), catalog.match(frame, 15922).orElseThrow().profile().getId().getId(),
                "端口 15922 应落到 dup-p2（同键名）");
        assertTrue(catalog.match(frame, 15999).isEmpty(), "没有档案绑定该端口时不应匹配");
    }
}

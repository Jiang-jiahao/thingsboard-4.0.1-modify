package com.jnks.iot.server.transport.http.push.session;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.device.data.DefaultDeviceTransportConfiguration;
import com.jnks.iot.server.common.data.device.profile.DefaultDeviceProfileTransportConfiguration;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;
import com.jnks.iot.server.transport.http.push.HttpPushRoutingService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
public class HttpPushGatewaySessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private SessionInfoProto gatewaySessionInfo;
    private DefaultDeviceProfileTransportConfiguration profileTransportConfiguration;
    private DefaultDeviceTransportConfiguration deviceTransportConfiguration;
    private HttpPushRoutingService routingService;

    @Builder.Default
    private final Map<String, HttpPushTargetSession> activeTargets = new ConcurrentHashMap<>();

    public DeviceId getDeviceId() {
        return device.getId();
    }

    public void clearTargets() {
        activeTargets.clear();
    }
}

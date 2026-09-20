package com.jnks.iot.server.transport.http.outbound;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.Device;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.gen.transport.TransportProtos.SessionInfoProto;
import com.jnks.iot.server.transport.http.outbound.HttpOutboundTransportContext;

@Data
@Builder
public class HttpOutboundSessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private String token;
    private SessionInfoProto sessionInfo;
    private HttpOutboundTransportContext transportContext;

    public DeviceId getDeviceId() {
        return device.getId();
    }
}

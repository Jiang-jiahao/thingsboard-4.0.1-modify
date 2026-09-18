package org.thingsboard.server.transport.http.outbound;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.transport.http.outbound.HttpOutboundTransportContext;

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

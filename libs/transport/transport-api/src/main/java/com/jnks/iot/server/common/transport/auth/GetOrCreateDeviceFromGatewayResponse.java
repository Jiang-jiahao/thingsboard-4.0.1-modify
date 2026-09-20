package com.jnks.iot.server.common.transport.auth;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.DeviceProfile;

@Data
@Builder
public class GetOrCreateDeviceFromGatewayResponse implements DeviceProfileAware {

    private TransportDeviceInfo deviceInfo;
    private DeviceProfile deviceProfile;

}

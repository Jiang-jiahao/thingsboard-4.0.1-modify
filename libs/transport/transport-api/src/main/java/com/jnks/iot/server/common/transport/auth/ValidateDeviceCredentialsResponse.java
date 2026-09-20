package com.jnks.iot.server.common.transport.auth;

import lombok.Builder;
import lombok.Data;
import com.jnks.iot.server.common.data.DeviceProfile;

import java.io.Serializable;

@Data
@Builder
public class ValidateDeviceCredentialsResponse implements DeviceProfileAware, Serializable {

    private final TransportDeviceInfo deviceInfo;
    private final DeviceProfile deviceProfile;
    private final String credentials;

    public boolean hasDeviceInfo() {
        return deviceInfo != null;
    }
}

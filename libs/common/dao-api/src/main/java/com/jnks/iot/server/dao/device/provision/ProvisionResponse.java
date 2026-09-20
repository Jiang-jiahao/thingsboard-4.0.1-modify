package com.jnks.iot.server.dao.device.provision;

import lombok.Data;
import com.jnks.iot.server.common.data.security.DeviceCredentials;

@Data
public class ProvisionResponse {
    private final DeviceCredentials deviceCredentials;
    private final ProvisionResponseStatus responseStatus;
}

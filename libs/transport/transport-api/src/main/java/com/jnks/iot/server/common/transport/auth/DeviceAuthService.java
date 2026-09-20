package com.jnks.iot.server.common.transport.auth;

import com.jnks.iot.server.common.data.security.DeviceCredentialsFilter;

public interface DeviceAuthService {

    DeviceAuthResult process(DeviceCredentialsFilter credentials);

}

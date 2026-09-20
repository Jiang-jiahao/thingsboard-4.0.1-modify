package com.jnks.iot.server.dao.device;

import lombok.Data;

@Data
public class DeviceConnectivityInfo {
    private boolean enabled;
    private String host;
    private String port;
}

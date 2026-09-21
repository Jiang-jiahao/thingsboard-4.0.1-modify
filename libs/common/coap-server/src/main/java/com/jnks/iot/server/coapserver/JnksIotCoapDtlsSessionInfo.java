package com.jnks.iot.server.coapserver;

import lombok.Data;
import com.jnks.iot.server.common.data.DeviceProfile;
import com.jnks.iot.server.common.transport.auth.ValidateDeviceCredentialsResponse;

@Data
public class JnksIotCoapDtlsSessionInfo {

    private ValidateDeviceCredentialsResponse msg;
    private DeviceProfile deviceProfile;
    private long lastActivityTime;


    public JnksIotCoapDtlsSessionInfo(ValidateDeviceCredentialsResponse msg, DeviceProfile deviceProfile) {
        this.msg = msg;
        this.deviceProfile = deviceProfile;
        this.lastActivityTime = System.currentTimeMillis();
    }
}
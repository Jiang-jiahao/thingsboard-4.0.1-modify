package com.jnks.iot.server.common.data.device.profile;

import lombok.Data;
import com.jnks.iot.server.common.data.DeviceProfileType;

@Data
public class DefaultDeviceProfileConfiguration implements DeviceProfileConfiguration {

    @Override
    public DeviceProfileType getType() {
        return DeviceProfileType.DEFAULT;
    }

}

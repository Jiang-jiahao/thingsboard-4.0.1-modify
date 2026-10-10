package com.jnks.iot.server.common.data.device.data;

import lombok.Data;
import com.jnks.iot.server.common.data.DeviceTransportType;

/**
 * HTTP Push（设备主动上报）设备级配置：设备凭访问令牌上报自己的数据，无可配置项。
 */
@Data
public class DefaultDeviceTransportConfiguration implements DeviceTransportConfiguration {

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.DEFAULT;
    }

    @Override
    public void validate() {
    }

}

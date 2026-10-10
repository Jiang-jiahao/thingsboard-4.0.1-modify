package com.jnks.iot.server.common.data.device.profile;

import lombok.Data;
import com.jnks.iot.server.common.data.DeviceTransportType;

@Data
public class DefaultDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /**
     * UI 工作模式标记：PASSIVE=被动上报，PULL=主动拉取（仅前端写入，便于保存后正确回显）。
     */
    private String httpTransportMode;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.DEFAULT;
    }

    @Override
    public void validate() {
    }

}

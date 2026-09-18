package org.thingsboard.server.common.data.device.profile;

import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;

@Data
public class DefaultDeviceProfileTransportConfiguration implements DeviceProfileTransportConfiguration {

    /**
     * UI 工作模式标记：PASSIVE=被动上报，PULL=主动拉取（仅前端写入，便于保存后正确回显）。
     */
    private String httpTransportMode;

    /**
     * HTTP Push（设备主动上报）数据路由；未配置时为 null，走标准遥测解析。
     */
    private HttpPullDeviceRoutingConfiguration routing;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.DEFAULT;
    }

    @Override
    public void validate() {
        if (routing != null) {
            routing.validate();
        }
    }

}

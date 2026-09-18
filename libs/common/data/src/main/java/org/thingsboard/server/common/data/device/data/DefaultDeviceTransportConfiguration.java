package org.thingsboard.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;

/**
 * HTTP Push 等多设备路由场景下的设备级配置。
 */
@Data
public class DefaultDeviceTransportConfiguration implements DeviceTransportConfiguration {

    /**
     * 是否为网关设备（使用访问令牌接收批量上报并路由）。默认 true。
     */
    private Boolean gateway = true;

    /**
     * 多设备路由时与请求体中设备 ID 字段匹配（见档案 routing）。
     */
    private String externalDeviceId;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.DEFAULT;
    }

    @Override
    public void validate() {
        if (gateway == null) {
            gateway = true;
        }
    }

    @JsonIgnore
    public boolean isGateway() {
        return gateway == null || gateway;
    }

}

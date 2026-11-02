package com.jnks.iot.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import com.jnks.iot.server.common.data.DeviceTransportType;

/**
 * 设备级 HTTP Pull 配置：每个设备独立作为 HTTP 客户端轮询，数据只写入本设备。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpPullDeviceTransportConfiguration implements DeviceTransportConfiguration {

    /**
     * 可选：覆盖设备档案中的 pollUrl（主机:端口或完整 URL）。
     */
    private String pollUrlOverride;

    /**
     * 可选：设备 IP（或主机）覆盖，只替换档案 URL 中的 host。
     * <p>
     * 协议、端口与路径全部沿用档案配置，登录地址与所有拉取地址共用该 IP；
     * 同一档案即可服务多台同款设备（端口/路径相同、IP 不同）。
     */
    private String ipOverride;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.HTTP_PULL;
    }

    @Override
    public void validate() {
    }
}

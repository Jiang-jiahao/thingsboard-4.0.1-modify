/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;

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

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.HTTP_PULL;
    }

    @Override
    public void validate() {
    }
}

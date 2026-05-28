/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.thingsboard.server.common.data.DeviceTransportType;
import org.thingsboard.server.common.data.StringUtils;

/**
 * 设备级 HTTP Pull 配置。
 * <ul>
 *   <li>{@link #collector}：为 true 时该设备作为采集器，按档案周期发起 HTTP 请求。</li>
 *   <li>{@link #externalDeviceId}：多设备路由时，与响应中设备 ID 字段匹配（见档案 routing）。</li>
 *   <li>{@link #pollUrlOverride}：覆盖档案中的 pollUrl。</li>
 * </ul>
 */
@Data
public class HttpPullDeviceTransportConfiguration implements DeviceTransportConfiguration {

    /**
     * 是否作为 HTTP 采集器（发起轮询）。默认 true。
     */
    private Boolean collector = true;

    /**
     * 第三方平台设备 ID，用于 MULTI_DEVICE 路由匹配。
     */
    private String externalDeviceId;

    /**
     * 可选：覆盖设备档案中的 pollUrl。
     */
    private String pollUrlOverride;

    @Override
    public DeviceTransportType getType() {
        return DeviceTransportType.HTTP_PULL;
    }

    @Override
    public void validate() {
        if (collector == null) {
            collector = true;
        }
    }

    @JsonIgnore
    public boolean isCollector() {
        return collector == null || collector;
    }
}

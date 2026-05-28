/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.server.common.data.StringUtils;

import java.io.Serializable;
import java.util.UUID;

/**
 * 多设备路由：从响应 JSON 中读取设备标识，映射到租户内已注册且具备会话的目标设备。
 * <p>
 * 目标设备须在 {@link #targetDeviceProfileId} 对应档案下，且设备传输配置中填写
 * {@link org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration#getExternalDeviceId()}
 *（{@link HttpPullDeviceIdMatchStrategy#EXTERNAL_DEVICE_ID}）或与 name/label 一致。
 * 仅对已成功建立异步会话的「活跃」目标设备上报遥测。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class HttpPullDeviceRoutingConfiguration implements Serializable {

    private HttpPullRoutingMode routingMode = HttpPullRoutingMode.SINGLE_DEVICE;

    /**
     * 响应体中数组的 JSONPath，如 {@code $.data.list}；空表示根节点即为数组或单对象。
     */
    private String responseArrayJsonPath;

    /**
     * 每个数组元素内设备 ID 的 JSONPath（相对元素），如 {@code deviceId} 或 {@code id}。
     * {@link HttpPullRoutingMode#MULTI_DEVICE} 时必填。
     */
    private String deviceIdJsonPath;

    private HttpPullDeviceIdMatchStrategy deviceIdMatchStrategy = HttpPullDeviceIdMatchStrategy.DEVICE_NAME;

    /**
     * 仅路由到该设备档案下的目标设备；为空则允许匹配租户内任意档案（仍须能解析到设备）。
     */
    private UUID targetDeviceProfileId;

    /**
     * 整包 JSON 写入遥测的键名。
     */
    private String telemetryPayloadKey = "httpPullPayload";

    public void validate() {
        if (routingMode == null) {
            routingMode = HttpPullRoutingMode.SINGLE_DEVICE;
        }
        if (deviceIdMatchStrategy == null) {
            deviceIdMatchStrategy = HttpPullDeviceIdMatchStrategy.DEVICE_NAME;
        }
        if (StringUtils.isBlank(telemetryPayloadKey)) {
            telemetryPayloadKey = "httpPullPayload";
        }
        if (routingMode == HttpPullRoutingMode.MULTI_DEVICE && StringUtils.isBlank(deviceIdJsonPath)) {
            throw new IllegalArgumentException("HTTP pull multi-device routing requires deviceIdJsonPath");
        }
    }
}

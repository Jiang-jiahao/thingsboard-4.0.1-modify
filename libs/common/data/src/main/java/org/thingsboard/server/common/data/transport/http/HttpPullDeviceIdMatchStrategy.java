/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

/**
 * 将 HTTP 响应中的设备标识映射到 ThingsBoard 设备。
 */
public enum HttpPullDeviceIdMatchStrategy {
    /** 与设备 {@code name} 匹配（默认） */
    DEVICE_NAME,
    /** 与设备 {@code label} 匹配 */
    DEVICE_LABEL,
    /** 与设备传输配置中的 {@link org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration#getExternalDeviceId()} 匹配 */
    EXTERNAL_DEVICE_ID
}

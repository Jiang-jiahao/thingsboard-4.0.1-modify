/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

public enum HttpPullRoutingMode {
    /** 整段响应 JSON 写入采集器设备遥测 */
    SINGLE_DEVICE,
    /** 按数组元素中的设备 ID 字段路由到租户内活跃目标设备 */
    MULTI_DEVICE
}

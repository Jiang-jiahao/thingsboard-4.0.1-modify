/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

public enum HttpPullRoutingMode {
    /** 整包写入采集器 */
    SINGLE_DEVICE,
    /**
     * 每条消息对应一个设备（适用于 MQTT 通配订阅：多设备分时上报，payload 为单条 JSON）。
     */
    PER_MESSAGE,
    /** 按本请求配置的路径拆分并路由（单条消息内 JSON 数组批量设备） */
    MULTI_DEVICE,
    /**
     * 自动：响应为多条记录（数组或列表）时按设备 ID 路由，否则整包写入采集器。
     */
    AUTO
}

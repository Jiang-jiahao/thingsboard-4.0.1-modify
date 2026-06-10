/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

public enum HttpPullRoutingMode {
    /** 整包写入采集器 */
    SINGLE_DEVICE,
    /** 按本请求配置的路径拆分并路由 */
    MULTI_DEVICE,
    /**
     * 自动：响应为多条记录（数组或列表）时按设备 ID 路由，否则整包写入采集器。
     */
    AUTO
}

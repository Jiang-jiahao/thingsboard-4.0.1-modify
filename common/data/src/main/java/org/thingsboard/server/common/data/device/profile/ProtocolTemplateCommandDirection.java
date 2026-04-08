/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

/**
 * 协议模板负载下命令与传输方向（上行解析仅使用 {@link #UPLINK} 与 {@link #BOTH}）。
 */
public enum ProtocolTemplateCommandDirection {
    /** 设备→平台，参与 HEX 遥测解析 */
    UPLINK,
    /** 平台→设备，仅记录在配置中供运维/规则参考，不参与上行解析展开 */
    DOWNLINK,
    /** 双向 */
    BOTH
}

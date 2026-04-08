/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.service.protocoltemplate;

import lombok.Data;

/**
 * 协议模板包在线解析测试请求（与 TCP 上行 {@code {"hex":"..."}} 行为一致）。
 */
@Data
public class ProtocolTemplateHexParseRequest {

    /** 租户库中协议模板包 UUID */
    private String bundleId;
    /** 连续十六进制串，可含空格；将规范化为小写无空格 */
    private String hex;
}

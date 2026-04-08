/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.service.protocoltemplate;

import lombok.Data;

import java.util.Map;

/**
 * 协议模板 HEX 解析测试结果（与 {@link org.thingsboard.server.transport.tcp.util.TcpHexProtocolParser} 一致）。
 */
@Data
public class ProtocolTemplateHexParseResult {

    private boolean success;
    /** 失败时说明（未找到包、非法 hex、校验失败、无匹配命令且无默认字段等） */
    private String errorMessage;
    /** 解析得到的扁平遥测键值（成功时） */
    private Map<String, Object> telemetry;
    /** 命中命令规则名称时与遥测中 hexCmdProfile 一致 */
    private String matchedHexCommandProfile;
}

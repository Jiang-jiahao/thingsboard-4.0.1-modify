/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.service.protocoltemplate;

import lombok.Data;

import java.util.Map;

/**
 * 按协议模板包中的下行（或双向）命令契约组 HEX 帧。
 */
@Data
public class ProtocolTemplateHexBuildRequest {

    private String bundleId;
    /**
     * 与 {@link org.thingsboard.server.common.data.device.profile.ProtocolTemplateCommandDefinition#getCommandValue()} 一致。
     */
    private Long commandValue;
    /**
     * 当同一 commandValue 在包内出现多次时必填，用于唯一定位命令行。
     */
    private String templateId;
    /**
     * 字段键 → 数值或 BYTES_AS_HEX 的连续十六进制字符串（与解析侧遥测语义一致：整数/浮点已乘 scale）。
     */
    private Map<String, Object> values;
}

/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.common.data.device.profile;

/**
 * LTV 中未在 {@link TcpHexLtvTagMapping} 中出现的 Tag 的处理方式。
 */
public enum TcpHexUnknownTagMode {
    SKIP,
    /**
     * 将整段 Value 以小写十六进制字符串写入遥测；
     * 键名为 {@code prefix + "_unk_" + 序号 + "_t" + tag}，其中 {@code tag} 为十进制或 {@code 0x} 十六进制（按 Tag 字段类型线宽补零），
     * 由 {@link TcpHexLtvRepeatingConfig#getUnknownTagTelemetryKeyHexLiteral()} 与
     * {@link TcpHexLtvTagMapping#getTagValueLiterallyHex()} 等配置共同决定。
     */
    EMIT_HEX
}

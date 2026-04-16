/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;

/**
 * LTV/TLV 中某一 Tag 与遥测键、Value 解析方式的映射。
 */
@Data
public class TcpHexLtvTagMapping implements Serializable {

    /** Tag 的整型值（与读出的无符号/有符号一致，如 0x0A 填 10） */
    private long tagValue;
    private String telemetryKey;
    private TcpHexValueType valueType;
    /**
     * 已废弃：LTV 段 Value 字节数由报文 Length 字段决定，{@link TcpHexValueType#BYTES_AS_HEX} 亦取整段 Value。
     * 保留仅用于反序列化旧配置。
     */
    private Integer byteLength;
    private Double scale;
    private Long bitMask;

    @JsonIgnore
    public double getEffectiveScale() {
        if (scale == null || scale == 0.0) {
            return 1.0;
        }
        return scale;
    }

    public void validate() {
        if (telemetryKey == null || telemetryKey.isBlank()) {
            throw new IllegalArgumentException("LTV tag mapping telemetryKey must not be blank");
        }
        if (valueType == null) {
            throw new IllegalArgumentException("LTV tag mapping valueType is required");
        }
        if (valueType == TcpHexValueType.UINT64_LE || valueType == TcpHexValueType.UINT64_BE
                || valueType == TcpHexValueType.INT64_LE || valueType == TcpHexValueType.INT64_BE) {
            throw new IllegalArgumentException(
                    "LTV tag mapping must not use UINT64_*/INT64_*; use UINT_AUTO_* / INT_AUTO_* (8-byte values are decoded as 64-bit automatically)");
        }
        // BYTES_AS_HEX：长度以 LTV Length 切出的 Value 为准，不再要求 byteLength
    }
}

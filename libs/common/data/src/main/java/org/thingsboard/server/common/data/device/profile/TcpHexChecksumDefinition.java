/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import lombok.Data;

import java.io.Serializable;

/**
 * 可选整帧校验：算法与参与字节范围与规则引擎 {@code HexChecksumDefinition} 一致。
 * 负下标为相对帧尾（如 -1 为最后一字节）。
 */
@Data
public class TcpHexChecksumDefinition implements Serializable {

    /**
     * NONE / SUM8 / CRC16_MODBUS / CRC16_CCITT / CRC32（大小写不敏感）。
     */
    private String type;

    private int fromByte;
    /**
     * 不包含；若为负则解析为 {@code buffer.length + toExclusive}。
     */
    private int toExclusive;
    /**
     * 校验值所在首字节下标；负数为相对帧尾。
     */
    private int checksumByteIndex;
}

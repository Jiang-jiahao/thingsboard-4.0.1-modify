/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.common.data.device.profile;

/**
 * 二进制字段类型（用于 TCP HEX 一帧原始字节的按偏移解析）。
 * 后缀 _BE 为大端，_LE 为小端（与 {@link java.nio.ByteOrder} 一致）。
 */
public enum TcpHexValueType {

    UINT8(1),
    INT8(1),
    UINT16_BE(2),
    UINT16_LE(2),
    INT16_BE(2),
    INT16_LE(2),
    UINT32_BE(4),
    UINT32_LE(4),
    INT32_BE(4),
    INT32_LE(4),
    /**
     * 仅用于 LTV Tag→遥测映射：无符号整数，小端；实际宽度由本段 Value 字节数在 1/2/4 中择一（与 {@link TcpHexValueType#UINT8}/{@link #UINT16_LE}/{@link #UINT32_LE} 等价）。
     */
    UINT_AUTO_LE(0),
    /**
     * 仅用于 LTV Tag→遥测映射：无符号整数，大端；宽度 1/2/4 字节。
     */
    UINT_AUTO_BE(0),
    /**
     * 仅用于 LTV Tag→遥测映射：有符号整数，小端；宽度 1/2/4 字节。
     */
    INT_AUTO_LE(0),
    /**
     * 仅用于 LTV Tag→遥测映射：有符号整数，大端；宽度 1/2/4 字节。
     */
    INT_AUTO_BE(0),
    FLOAT_BE(4),
    FLOAT_LE(4),
    DOUBLE_BE(8),
    DOUBLE_LE(8),
    /**
     * 原始字节切片格式化为连续十六进制字符串（小写），长度由 {@link TcpHexFieldDefinition#getByteLength()} 指定。
     */
    BYTES_AS_HEX(0);

    private final int fixedByteLength;

    TcpHexValueType(int fixedByteLength) {
        this.fixedByteLength = fixedByteLength;
    }

    /**
     * 固定宽度类型的字节数；{@link #BYTES_AS_HEX} 为 0，须使用字段上的 byteLength。
     */
    public int getFixedByteLength() {
        return fixedByteLength;
    }

    public boolean isBytesAsHex() {
        return this == BYTES_AS_HEX;
    }

    /** 是否仅用于 LTV 映射的「按本段字节数」整型（非固定宽度枚举）。 */
    public boolean isLtvAutoWidthIntegral() {
        return this == UINT_AUTO_LE || this == UINT_AUTO_BE || this == INT_AUTO_LE || this == INT_AUTO_BE;
    }
}

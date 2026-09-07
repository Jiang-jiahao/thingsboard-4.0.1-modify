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
    /** 无符号 64 位小端（8 字节）。解析器内部由 {@link #UINT_AUTO_LE} 在 Value 为 8 字节时选用；勿写入 LTV Tag 映射。 */
    UINT64_LE(8),
    /** 无符号 64 位大端（8 字节）。内部由 {@link #UINT_AUTO_BE} 选用；勿写入 LTV Tag 映射。 */
    UINT64_BE(8),
    /** 有符号 64 位小端（8 字节）。内部由 {@link #INT_AUTO_LE} 选用；勿写入 LTV Tag 映射。 */
    INT64_LE(8),
    /** 有符号 64 位大端（8 字节）。内部由 {@link #INT_AUTO_BE} 选用；勿写入 LTV Tag 映射。 */
    INT64_BE(8),
    /**
     * 仅用于 LTV Tag→遥测映射：无符号整数，小端；本段 Value 为 1/2/4 字节时择宽；8 字节映射为 {@link #UINT64_LE}；其它长度退化为 {@link #BYTES_AS_HEX}。
     */
    UINT_AUTO_LE(0),
    /**
     * 仅用于 LTV Tag→遥测映射：无符号整数，大端；1/2/4 字节择宽；8 字节映射为 {@link #UINT64_BE}；其它长度退化为 {@link #BYTES_AS_HEX}。
     */
    UINT_AUTO_BE(0),
    /**
     * 仅用于 LTV Tag→遥测映射：有符号整数，小端；1/2/4 字节择宽；8 字节映射为 {@link #INT64_LE}；其它长度退化为 {@link #BYTES_AS_HEX}。
     */
    INT_AUTO_LE(0),
    /**
     * 仅用于 LTV Tag→遥测映射：有符号整数，大端；1/2/4 字节择宽；8 字节映射为 {@link #INT64_BE}；其它长度退化为 {@link #BYTES_AS_HEX}。
     */
    INT_AUTO_BE(0),
    FLOAT_BE(4),
    FLOAT_LE(4),
    DOUBLE_BE(8),
    DOUBLE_LE(8),
    /**
     * 原始字节切片按 UTF-8 解码为字符串写入遥测；线宽与 {@link #BYTES_AS_HEX} 相同（{@link TcpHexFieldDefinition#getByteLength()}
     * 或从帧内偏移读长度）。非法序列按 Unicode 替换字符解码。
     */
    BYTES_AS_UTF8(0),
    /**
     * 原始字节切片格式化为连续十六进制字符串（小写），长度由 {@link TcpHexFieldDefinition#getByteLength()} 指定。
     */
    BYTES_AS_HEX(0);

    private final int fixedByteLength;

    TcpHexValueType(int fixedByteLength) {
        this.fixedByteLength = fixedByteLength;
    }

    /**
     * 固定宽度类型的字节数；{@link #BYTES_AS_HEX}、{@link #BYTES_AS_UTF8} 为 0，须使用字段上的 byteLength 或动态长度配置。
     */
    public int getFixedByteLength() {
        return fixedByteLength;
    }

    public boolean isBytesAsHex() {
        return this == BYTES_AS_HEX;
    }

    /**
     * 变长字节切片：连续 hex 或 UTF-8 文本（共享 {@link TcpHexFieldDefinition} 的 byteLength / byteLengthFromByteOffset 语义）。
     */
    public boolean isVariableByteSlice() {
        return this == BYTES_AS_HEX || this == BYTES_AS_UTF8;
    }

    /** 是否仅用于 LTV 映射的「按本段字节数」整型（非固定宽度枚举）。 */
    public boolean isLtvAutoWidthIntegral() {
        return this == UINT_AUTO_LE || this == UINT_AUTO_BE || this == INT_AUTO_LE || this == INT_AUTO_BE;
    }
}

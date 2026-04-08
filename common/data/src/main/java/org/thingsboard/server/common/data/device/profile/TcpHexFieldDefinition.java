/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;

/**
 * TCP HEX 帧内单字段映射：从帧字节数组的指定偏移按类型读出，写入遥测键名。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TcpHexFieldDefinition implements Serializable {

    /**
     * 遥测键名（扁平入库）。
     */
    private String key;
    /**
     * 相对帧起始的字节偏移（0 起）。
     */
    private int byteOffset;
    /**
     * 解析类型。
     */
    private TcpHexValueType valueType;
    /**
     * 仅 {@link TcpHexValueType#BYTES_AS_HEX}：固定截取的字节数（与 {@link #byteLengthFromByteOffset} 二选一）。
     */
    private Integer byteLength;
    /**
     * 仅 {@link TcpHexValueType#BYTES_AS_HEX}：从帧内该偏移按 {@link #byteLengthFromValueType} 读出长度，再截取相应字节。
     */
    private Integer byteLengthFromByteOffset;
    /**
     * 读出动态长度时使用的整型类型；默认 {@link TcpHexValueType#UINT8}。
     */
    private TcpHexValueType byteLengthFromValueType;
    /**
     * 数值比例系数，默认 1；解析结果为 Number 时乘以该系数再入库。
     */
    private Double scale;
    /**
     * 可选：按位与（仅对整数类型有效；浮点与 BYTES_AS_HEX 忽略）。
     */
    private Long bitMask;

    /** 不参与 JSON：否则入库再读出时反序列化无对应字段会失败 */
    @JsonIgnore
    public double getEffectiveScale() {
        if (scale == null || scale == 0.0) {
            return 1.0;
        }
        return scale;
    }

    public void validate() {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("TCP hex protocol field key must not be blank");
        }
        if (byteOffset < 0) {
            throw new IllegalArgumentException("TCP hex protocol field byteOffset must be >= 0");
        }
        if (valueType == null) {
            throw new IllegalArgumentException("TCP hex protocol field valueType is required");
        }
        if (valueType.isBytesAsHex()) {
            boolean fixed = byteLength != null && byteLength > 0;
            boolean fromFrame = byteLengthFromByteOffset != null && byteLengthFromByteOffset >= 0;
            if (fixed == fromFrame) {
                throw new IllegalArgumentException(
                        "TCP hex BYTES_AS_HEX requires exactly one of: byteLength>0, or byteLengthFromByteOffset with integral byteLengthFromValueType");
            }
            if (fromFrame) {
                TcpHexValueType vt = byteLengthFromValueType != null ? byteLengthFromValueType : TcpHexValueType.UINT8;
                if (!TcpHexCommandProfile.isIntegralMatchType(vt)) {
                    throw new IllegalArgumentException("byteLengthFromValueType must be integral");
                }
            }
        }
    }
}

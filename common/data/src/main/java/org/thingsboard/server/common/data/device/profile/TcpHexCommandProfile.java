/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.common.data.device.profile;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 按帧内某偏移处的<strong>命令字</strong>（整数）匹配成功后，使用该规则下的字段列表解析遥测。
 * <p>
 * 典型用法（如华诺协议）：起始码后第 4 字节为命令，则 {@code matchByteOffset=4}、{@code matchValueType=UINT8}、
 * {@code matchValue=0xA4}（十进制 164）等；参数字段偏移从帧头 0 起算（例如参数从字节 7 开始则 offset=7）。
 */
@Data
public class TcpHexCommandProfile implements Serializable {

    /**
     * 可选说明，便于在界面区分多条规则。
     */
    private String name;
    /**
     * 读取命令字时的字节偏移（相对帧头，从 0 开始）。
     */
    private int matchByteOffset;
    /**
     * 命令字的二进制类型（仅允许整型，不允许 FLOAT/DOUBLE/BYTES_AS_HEX）。
     */
    private TcpHexValueType matchValueType;
    /**
     * 期望的命令值；无符号类型按无符号比较（例如 UINT8 的 255 即 255L）。
     */
    private long matchValue;
    /**
     * 可选第二匹配：主匹配成功后，再在该偏移读取整型并与 {@link #secondaryMatchValue} 相等（如华诺应答帧命令 0xA2，
     * 第 7 字节为回显的原命令）。未设置 {@link #secondaryMatchByteOffset} 时仅主匹配。
     */
    private Integer secondaryMatchByteOffset;
    private TcpHexValueType secondaryMatchValueType;
    private Long secondaryMatchValue;
    /**
     * 匹配成功后用于解析的字段列表（可与 {@link #ltvRepeating} 二选一或同时使用）。
     */
    private List<TcpHexFieldDefinition> fields;
    /**
     * 可选：从指定偏移起按 LTV/TLV 重复解析（适用于参数字段内为 Tag-Length-Value 列表的协议）。
     */
    private TcpHexLtvRepeatingConfig ltvRepeating;

    public void validate() {
        if (matchByteOffset < 0) {
            throw new IllegalArgumentException("TCP hex command profile matchByteOffset must be >= 0");
        }
        if (matchValueType == null) {
            throw new IllegalArgumentException("TCP hex command profile matchValueType is required");
        }
        if (!TcpHexCommandProfile.isIntegralMatchType(matchValueType)) {
            throw new IllegalArgumentException("TCP hex command matchValueType must be an integral type, got " + matchValueType);
        }
        boolean hasFields = fields != null && !fields.isEmpty();
        boolean hasLtv = ltvRepeating != null;
        if (!hasFields && !hasLtv) {
            throw new IllegalArgumentException("TCP hex command profile requires non-empty fields and/or ltvRepeating");
        }
        if (hasFields) {
            for (TcpHexFieldDefinition f : new ArrayList<>(fields)) {
                if (f != null) {
                    f.validate();
                }
            }
        }
        if (hasLtv) {
            ltvRepeating.validate();
        }
        if (secondaryMatchByteOffset != null) {
            if (secondaryMatchByteOffset < 0) {
                throw new IllegalArgumentException("TCP hex command profile secondaryMatchByteOffset must be >= 0");
            }
            TcpHexValueType st = secondaryMatchValueType != null ? secondaryMatchValueType : TcpHexValueType.UINT8;
            if (!isIntegralMatchType(st)) {
                throw new IllegalArgumentException("TCP hex command secondaryMatchValueType must be integral");
            }
            if (secondaryMatchValue == null) {
                throw new IllegalArgumentException("TCP hex command profile secondaryMatchValue is required when secondaryMatchByteOffset is set");
            }
        }
    }

    public static boolean isIntegralMatchType(TcpHexValueType t) {
        if (t == null) {
            return false;
        }
        return switch (t) {
            case FLOAT_BE, FLOAT_LE, DOUBLE_BE, DOUBLE_LE, BYTES_AS_HEX -> false;
            default -> true;
        };
    }
}

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
import java.util.HexFormat;
import java.util.List;

/**
 * TCP HEX 帧内单字段映射：从帧字节数组的指定偏移按类型读出，写入遥测键名。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TcpHexFieldDefinition implements Serializable {

    private String key;
    private int byteOffset;
    private TcpHexValueType valueType;
    private Integer byteLength;
    private Integer byteLengthFromByteOffset;
    private TcpHexValueType byteLengthFromValueType;
    private Double scale;
    private Long bitMask;
    /**
     * 仅用于<strong>命令覆盖字段</strong>：为 true 时参与该命令「下行自动参长」的字节统计（须与
     * {@link ProtocolTemplateCommandDefinition#getDownlinkPayloadLengthFieldKey()} 等配合）。
     */
    private Boolean includeInDownlinkPayloadLength;
    /**
     * @deprecated 旧版：参长字段上直接列成员键；请改用命令级 {@link ProtocolTemplateCommandDefinition#getDownlinkPayloadLengthAuto()}。
     */
    private List<String> downlinkPayloadLengthMemberKeys;
    /**
     * @deprecated 旧版自动参长勾选。
     */
    private Boolean autoDownlinkPayloadLength;
    /**
     * 仅旧版<strong>字段级</strong>自动参长（{@link #downlinkPayloadLengthMemberKeys} / {@link #autoDownlinkPayloadLength}）使用。
     * 命令级 {@link ProtocolTemplateCommandDefinition#getDownlinkPayloadLengthAuto()} 已改为勾选字段宽度之和，不再读取本属性。
     */
    private Integer downlinkPayloadStartByteOffset;
    /**
     * @deprecated 旧版区间结束。
     */
    private Integer downlinkPayloadEndExclusiveByteOffset;
    /**
     * 非空且 {@link #valueType} 为整型时：上行解析要求线型整数码与该值一致（经与 {@code writeIntegralAt}/{@code readIntegralAt} 相同的宽度/端序归一化后比较）；
     * 下行组帧直接写入该线型值，调用方 JSON 可省略此键（与 scale 无关，即为线上原始整数码，如 UINT8 的 0xA5 填 165）。
     */
    private Long fixedWireIntegralValue;
    /**
     * 非空且 {@link #valueType} 为 {@link TcpHexValueType#BYTES_AS_HEX}（固定 {@link #byteLength}）时：上行要求该段原始字节与解析后的 hex 完全一致（忽略空白、大小写）；
     * 下行组帧直接写入该 hex，JSON 可省略此键。
     */
    private String fixedBytesHex;

    @JsonIgnore
    public double getEffectiveScale() {
        if (scale == null || scale == 0.0) {
            return 1.0;
        }
        return scale;
    }

    @JsonIgnore
    public boolean hasDownlinkPayloadLengthMemberKeys() {
        return downlinkPayloadLengthMemberKeys != null && !downlinkPayloadLengthMemberKeys.isEmpty();
    }

    /**
     * 整型固定线值与 BYTES_AS_HEX 固定内容互斥。历史 JSON / 未走 UI 互斥保存时可能两者同在，按 {@link #valueType} 保留其一并清空另一项，避免校验失败。
     */
    public void normalizeMutuallyExclusiveFixedFields() {
        if (fixedWireIntegralValue == null) {
            return;
        }
        if (fixedBytesHex == null || fixedBytesHex.isBlank()) {
            return;
        }
        if (valueType != null && valueType.isBytesAsHex()) {
            fixedWireIntegralValue = null;
        } else {
            fixedBytesHex = null;
        }
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
        if (valueType.isLtvAutoWidthIntegral()) {
            throw new IllegalArgumentException(
                    "TCP hex field valueType must not use LTV-only auto integral types: " + valueType);
        }
        normalizeMutuallyExclusiveFixedFields();
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
        if (hasDownlinkPayloadLengthMemberKeys()) {
            if (valueType == null || valueType.isBytesAsHex()) {
                throw new IllegalArgumentException("downlinkPayloadLengthMemberKeys requires an integral valueType");
            }
            if (!TcpHexCommandProfile.isIntegralMatchType(valueType)) {
                throw new IllegalArgumentException("downlinkPayloadLengthMemberKeys requires integral valueType (not float/double)");
            }
            if (downlinkPayloadLengthMemberKeys.size() > 64) {
                throw new IllegalArgumentException("downlinkPayloadLengthMemberKeys must have at most 64 entries");
            }
            for (String mk : downlinkPayloadLengthMemberKeys) {
                if (mk == null || mk.isBlank()) {
                    throw new IllegalArgumentException("downlinkPayloadLengthMemberKeys entries must be non-blank");
                }
                if (mk.length() > 255) {
                    throw new IllegalArgumentException("downlinkPayloadLengthMemberKeys entry too long: " + mk.length());
                }
            }
            if (downlinkPayloadStartByteOffset != null && downlinkPayloadStartByteOffset < 0) {
                throw new IllegalArgumentException("downlinkPayloadStartByteOffset must be >= 0");
            }
        } else if (Boolean.TRUE.equals(autoDownlinkPayloadLength)) {
            if (valueType == null || valueType.isBytesAsHex()) {
                throw new IllegalArgumentException("autoDownlinkPayloadLength requires an integral valueType");
            }
            if (!TcpHexCommandProfile.isIntegralMatchType(valueType)) {
                throw new IllegalArgumentException("autoDownlinkPayloadLength requires integral valueType (not float/double)");
            }
            if (downlinkPayloadStartByteOffset != null && downlinkPayloadStartByteOffset < 0) {
                throw new IllegalArgumentException("downlinkPayloadStartByteOffset must be >= 0");
            }
            if (downlinkPayloadEndExclusiveByteOffset != null) {
                int impliedStart = byteOffset + valueType.getFixedByteLength();
                int start = downlinkPayloadStartByteOffset != null ? downlinkPayloadStartByteOffset : impliedStart;
                if (downlinkPayloadEndExclusiveByteOffset <= start) {
                    throw new IllegalArgumentException(
                            "downlinkPayloadEndExclusiveByteOffset must be greater than effective payload start (" + start + ")");
                }
            }
        }
        if (fixedWireIntegralValue != null) {
            if (valueType == null || valueType.isBytesAsHex() || !TcpHexCommandProfile.isIntegralMatchType(valueType)) {
                throw new IllegalArgumentException("fixedWireIntegralValue requires an integral valueType (not BYTES_AS_HEX/float/double)");
            }
        }
        if (fixedBytesHex != null && !fixedBytesHex.isBlank()) {
            if (valueType == null || !valueType.isBytesAsHex()) {
                throw new IllegalArgumentException("fixedBytesHex requires BYTES_AS_HEX valueType");
            }
            if (byteLength == null || byteLength <= 0) {
                throw new IllegalArgumentException("fixedBytesHex requires fixed byteLength > 0 on BYTES_AS_HEX");
            }
            String clean = fixedBytesHex.replaceAll("\\s+", "");
            if (clean.isEmpty() || (clean.length() & 1) == 1) {
                throw new IllegalArgumentException("fixedBytesHex must be non-empty even-length hex");
            }
            try {
                byte[] parsed = HexFormat.of().parseHex(clean);
                if (parsed.length != byteLength) {
                    throw new IllegalArgumentException(
                            "fixedBytesHex decodes to " + parsed.length + " bytes but byteLength is " + byteLength);
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("fixedBytesHex: invalid hex: " + e.getMessage());
            }
        }
    }
}

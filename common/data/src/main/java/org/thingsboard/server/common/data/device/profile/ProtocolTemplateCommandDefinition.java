/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0, the "License";
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 在某一帧模板下，按命令字配置上行/下行及可选字段覆盖。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProtocolTemplateCommandDefinition implements Serializable {

    private String templateId;
    /**
     * 可选；匹配成功后写入遥测键 hexCmdProfile。
     */
    private String name;
    /**
     * 整型匹配：与模板 commandByteOffset 处按 {@link #matchValueType} 读出的整型命令值比较。
     * 字节切片匹配（BYTES_AS_HEX / BYTES_AS_UTF8）时可为 0，期望值见 {@link #commandMatchBytesHex}。
     */
    private long commandValue;
    /**
     * 当 {@link #matchValueType} 为 BYTES_AS_HEX / BYTES_AS_UTF8 时：期望线字节（十六进制串），
     * 位数须与帧模板 {@link ProtocolTemplateDefinition#getCommandMatchWidth()}（1 或 4 字节）一致。
     */
    private String commandMatchBytesHex;
    /**
     * 默认 UINT32_LE；整型或 BYTES_AS_HEX / BYTES_AS_UTF8（定长原始字节，与帧模板一致）。
     */
    private TcpHexValueType matchValueType;
    /**
     * 可选：第二匹配字节偏移（整帧 0 起），与 {@link TcpHexCommandProfile#getSecondaryMatchByteOffset()} 一致。
     */
    private Integer secondaryMatchByteOffset;
    private TcpHexValueType secondaryMatchValueType;
    private Long secondaryMatchValue;
    private ProtocolTemplateCommandDirection direction;
    /**
     * 语义随 {@link #direction} 不同：
     * <ul>
     *   <li><b>上行 / 双向</b>：与帧模板字段按字节区间合并后，供 TCP HEX <b>解析</b> 使用（从设备上报帧中按偏移读出并写入遥测键）。</li>
     *   <li><b>下行</b>：表示平台下发时需 <b>填入参区</b> 的各参数含义与字节布局（组帧/编码契约）；当前 TCP 传输层不对下行帧做此类解析，供 RPC、规则链或外部组帧对照。</li>
     * </ul>
     * 合并规则：先保留与命令字段字节范围不重叠的模板字段，再追加命令字段；重叠区间以命令字段为准。为空则完全沿用模板字段。
     */
    private List<TcpHexFieldDefinition> fields;
    /**
     * 非空时覆盖模板中的 hexLtvRepeating。
     */
    private TcpHexLtvRepeatingConfig ltvRepeating;
    /**
     * 为 true 且方向为下行/双向时：下行组帧将 {@link #downlinkPayloadLengthFieldKey} 指明的整型字段写为参长，
     * 值为模板与命令合并后，各字段上 {@link TcpHexFieldDefinition#getIncludeInDownlinkPayloadLength()} 为 true 的<strong>线宽字节数之和</strong>（见 {@link #resolveDownlinkPayloadContributorKeys(java.util.List)}）。
     * 无固定 {@code byteLength} 的 BYTES 切片按组帧 {@code values} 中该键的实际字节数计入；若参长字段本身也勾选「参与参长」，则含本字段线宽。
     */
    private Boolean downlinkPayloadLengthAuto;
    /**
     * 作为参长写入的遥测键名，须与模板+命令合并后的某整型字段 key 一致。
     */
    private String downlinkPayloadLengthFieldKey;

    public void validate() {
        if (templateId == null || templateId.isBlank()) {
            throw new IllegalArgumentException("protocol template command templateId is required");
        }
        if (direction == null) {
            throw new IllegalArgumentException("protocol template command direction is required");
        }
        TcpHexValueType t = matchValueType != null ? matchValueType : TcpHexValueType.UINT32_LE;
        if (TcpHexCommandProfile.isByteSliceCommandMatchType(t)) {
            if (commandMatchBytesHex == null || commandMatchBytesHex.isBlank()) {
                throw new IllegalArgumentException("protocol template command commandMatchBytesHex is required for BYTES_AS_HEX/BYTES_AS_UTF8 match");
            }
            try {
                String c = commandMatchBytesHex.replaceAll("\\s+", "");
                if (c.regionMatches(true, 0, "0x", 0, 2)) {
                    c = c.substring(2);
                }
                if (c.length() % 2 != 0) {
                    throw new IllegalArgumentException("commandMatchBytesHex must have an even number of hex digits");
                }
                HexFormat.of().parseHex(c);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("protocol template command commandMatchBytesHex: " + e.getMessage());
            }
        } else if (!TcpHexCommandProfile.isIntegralMatchType(t)) {
            throw new IllegalArgumentException("protocol template command matchValueType must be integral or BYTES_AS_HEX/BYTES_AS_UTF8");
        }
        if (fields != null) {
            int autoTotalFrame = 0;
            for (TcpHexFieldDefinition f : new ArrayList<>(fields)) {
                if (f != null) {
                    f.validate();
                    if (Boolean.TRUE.equals(f.getAutoDownlinkTotalFrameLength())) {
                        autoTotalFrame++;
                    }
                }
            }
            if (autoTotalFrame > 1) {
                throw new IllegalArgumentException("at most one command override field may set autoDownlinkTotalFrameLength");
            }
        }
        if (ltvRepeating != null) {
            ltvRepeating.validate();
        }
        if (secondaryMatchByteOffset != null) {
            if (secondaryMatchByteOffset < 0) {
                throw new IllegalArgumentException("protocol template command secondaryMatchByteOffset must be >= 0");
            }
            TcpHexValueType st = secondaryMatchValueType != null ? secondaryMatchValueType : TcpHexValueType.UINT8;
            if (!TcpHexCommandProfile.isIntegralMatchType(st)) {
                throw new IllegalArgumentException("protocol template command secondaryMatchValueType must be integral");
            }
            if (secondaryMatchValue == null) {
                throw new IllegalArgumentException("protocol template command secondaryMatchValue is required when secondaryMatchByteOffset is set");
            }
        }
        if (Boolean.TRUE.equals(downlinkPayloadLengthAuto)) {
            if (direction != ProtocolTemplateCommandDirection.DOWNLINK && direction != ProtocolTemplateCommandDirection.BOTH) {
                throw new IllegalArgumentException("downlinkPayloadLengthAuto requires DOWNLINK or BOTH direction");
            }
            if (downlinkPayloadLengthFieldKey == null || downlinkPayloadLengthFieldKey.isBlank()) {
                throw new IllegalArgumentException("downlinkPayloadLengthFieldKey is required when downlinkPayloadLengthAuto is true");
            }
            String lk = downlinkPayloadLengthFieldKey.trim();
            for (TcpHexFieldDefinition f : fields != null ? fields : List.<TcpHexFieldDefinition>of()) {
                if (f != null && f.getKey() != null && Objects.equals(f.getKey().trim(), lk)
                        && Boolean.TRUE.equals(f.getAutoDownlinkTotalFrameLength())) {
                    throw new IllegalArgumentException(
                            "downlinkPayloadLengthFieldKey must not refer to a field with autoDownlinkTotalFrameLength");
                }
            }
        }
    }

    /**
     * 下行自动参长：参与字节统计的键名集合（取模板与命令合并后的字段上 {@link TcpHexFieldDefinition#getIncludeInDownlinkPayloadLength()}）。
     * 命令覆盖与模板同区间时以命令字段为准，请在覆盖行上保留需要的勾选。
     */
    public static Set<String> resolveDownlinkPayloadContributorKeys(List<TcpHexFieldDefinition> merged) {
        Set<String> s = new HashSet<>();
        if (merged == null) {
            return s;
        }
        for (TcpHexFieldDefinition f : merged) {
            if (f != null && Boolean.TRUE.equals(f.getIncludeInDownlinkPayloadLength()) && f.getKey() != null) {
                String k = f.getKey().trim();
                if (!k.isEmpty()) {
                    s.add(k);
                }
            }
        }
        return s;
    }
}

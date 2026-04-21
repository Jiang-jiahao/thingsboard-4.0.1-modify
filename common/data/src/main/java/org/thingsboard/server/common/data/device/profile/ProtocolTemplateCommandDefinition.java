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
import java.util.List;
import java.util.Objects;

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
     * 与模板 commandByteOffset 处读出的命令值比较（十进制；UINT32_LE 时常用命令号如 3）。
     */
    private long commandValue;
    /**
     * 默认 UINT32_LE；须为整型类型。
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
     * 值为命令覆盖字段中 {@link TcpHexFieldDefinition#getIncludeInDownlinkPayloadLength()} 为 true 的各字段在合并结果中的<strong>字节宽度之和</strong>（须在合并结果中存在）。
     * 若参长字段本身也勾选「参与参长」，则其线宽一并计入（适用于「长度 = 本字段 + 后续数据」等语义）。
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
        if (!TcpHexCommandProfile.isIntegralMatchType(t)) {
            throw new IllegalArgumentException("protocol template command matchValueType must be integral");
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
            boolean anyContributor = false;
            if (fields != null) {
                for (TcpHexFieldDefinition f : fields) {
                    if (f != null && Boolean.TRUE.equals(f.getIncludeInDownlinkPayloadLength())) {
                        anyContributor = true;
                        break;
                    }
                }
            }
            if (!anyContributor) {
                throw new IllegalArgumentException(
                        "downlinkPayloadLengthAuto requires at least one command field with includeInDownlinkPayloadLength");
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
}

/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0, the "License";
 */
package org.thingsboard.server.common.data.device.profile;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 在某一帧模板下，按命令字配置上行/下行及可选字段覆盖。
 */
@Data
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
            for (TcpHexFieldDefinition f : new ArrayList<>(fields)) {
                if (f != null) {
                    f.validate();
                }
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
    }
}

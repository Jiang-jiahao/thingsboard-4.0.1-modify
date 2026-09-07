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

/**
 * 协议模板中的「帧模板」：同一模板下共享命令字节偏移、默认字段与可选 LTV；具体命令号在 {@link ProtocolTemplateCommandDefinition} 中配置。
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProtocolTemplateDefinition implements Serializable {

    /**
     * 模板标识，在同一设备配置内唯一；命令行通过 templateId 引用。
     */
    private String id;
    /**
     * 界面展示用名称。
     */
    private String name;
    /**
     * 命令字起始字节偏移（相对整帧字节 0），UDP 场景常见为 12。
     */
    private Integer commandByteOffset;
    /**
     * 1 或 4；4 表示 uint32 小端与命令值比较。
     */
    private Integer commandMatchWidth;
    /**
     * 可选：整帧校验和（先于字段解析执行）。
     */
    private TcpHexChecksumDefinition checksum;
    /**
     * 未在命令规则中覆盖时使用的默认解析字段（整帧偏移）。
     */
    private List<TcpHexFieldDefinition> hexProtocolFields;
    /**
     * 可选默认 LTV/TLV 重复段。
     */
    private TcpHexLtvRepeatingConfig hexLtvRepeating;

    public void validate() {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("protocol template id is required");
        }
        if (commandByteOffset != null && commandByteOffset < 0) {
            throw new IllegalArgumentException("protocol template commandByteOffset must be >= 0");
        }
        if (commandMatchWidth != null && commandMatchWidth != 1 && commandMatchWidth != 4) {
            throw new IllegalArgumentException("protocol template commandMatchWidth must be 1 or 4");
        }
        if (hexProtocolFields != null) {
            int autoTotalFrame = 0;
            for (TcpHexFieldDefinition f : new ArrayList<>(hexProtocolFields)) {
                if (f != null) {
                    f.validate();
                    if (Boolean.TRUE.equals(f.getAutoDownlinkTotalFrameLength())) {
                        autoTotalFrame++;
                    }
                }
            }
            if (autoTotalFrame > 1) {
                throw new IllegalArgumentException("at most one template field may set autoDownlinkTotalFrameLength");
            }
        }
        if (hexLtvRepeating != null) {
            hexLtvRepeating.validate();
        }
    }
}

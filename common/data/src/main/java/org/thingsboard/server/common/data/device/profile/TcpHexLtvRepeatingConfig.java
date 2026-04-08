/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 从帧内某偏移起，按 LTV 或 TLV 重复解析多段；每段 Value 长度由 Length 字段给出（仅指负载，不含 L/T）。
 */
@Data
public class TcpHexLtvRepeatingConfig implements Serializable {

    private int startByteOffset;
    private TcpHexValueType lengthFieldType;
    private TcpHexValueType tagFieldType;
    private TcpHexLtvChunkOrder chunkOrder;
    /**
     * 最多解析条数；{@code null} 或 {@code 0} 表示使用内置上限（256）。
     */
    private Integer maxItems;
    /**
     * 遥测键前缀，单条键名为 {@code prefix + "_" + 序号 + "_" + telemetryKey}。
     */
    private String keyPrefix;
    private TcpHexUnknownTagMode unknownTagMode;
    private List<TcpHexLtvTagMapping> tagMappings;

    public TcpHexLtvChunkOrder getChunkOrder() {
        return Objects.requireNonNullElse(chunkOrder, TcpHexLtvChunkOrder.LTV);
    }

    @JsonIgnore
    public String getEffectiveKeyPrefix() {
        if (keyPrefix == null || keyPrefix.isBlank()) {
            return "ltv";
        }
        return keyPrefix;
    }

    public TcpHexUnknownTagMode getUnknownTagMode() {
        return Objects.requireNonNullElse(unknownTagMode, TcpHexUnknownTagMode.SKIP);
    }

    @JsonIgnore
    public int getEffectiveMaxItems() {
        if (maxItems == null || maxItems <= 0) {
            return 256;
        }
        return Math.min(maxItems, 4096);
    }

    public void validate() {
        if (startByteOffset < 0) {
            throw new IllegalArgumentException("LTV startByteOffset must be >= 0");
        }
        if (lengthFieldType == null || tagFieldType == null) {
            throw new IllegalArgumentException("LTV lengthFieldType and tagFieldType are required");
        }
        if (!TcpHexCommandProfile.isIntegralMatchType(lengthFieldType) || !TcpHexCommandProfile.isIntegralMatchType(tagFieldType)) {
            throw new IllegalArgumentException("LTV length/tag field types must be integral");
        }
        if (tagMappings != null) {
            for (TcpHexLtvTagMapping m : new ArrayList<>(tagMappings)) {
                if (m != null) {
                    m.validate();
                }
            }
        }
    }
}

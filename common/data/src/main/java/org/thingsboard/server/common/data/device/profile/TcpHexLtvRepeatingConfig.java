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
 * 从帧内某偏移起，按 LTV 或 TLV 重复解析多段。
 * <p>
 * 默认（{@link #lengthIncludesTag} 为 false）：Length 数值仅表示 <b>Value</b> 的字节数；Length 字段自身<strong>从不</strong>计入该数值。
 * {@link #lengthIncludesTag} 为 {@code true} 时：Length 数值仍<strong>不含</strong> Length 字段自身，仅表示其后紧跟的 <b>Tag + Value</b> 总字节数，
 * Value 长度 = {@code lengthField - tagWidth}。
 * <p>
 * {@link #lengthIncludesLengthField} 为少数线型：Length 数值把 <strong>Length 字段自身也计入</strong>（整段 L+T+V 的总字节数），
 * 此时 Value 长度 = {@code length - lenWidth - tagWidth}。解析时优先于 {@link #lengthIncludesTag}。
 */
@Data
public class TcpHexLtvRepeatingConfig implements Serializable {

    private int startByteOffset;
    private TcpHexValueType lengthFieldType;
    private TcpHexValueType tagFieldType;
    private TcpHexLtvChunkOrder chunkOrder;
    /**
     * 为 {@code true} 时，Length 数值（仍不含 Length 字段自身）= Tag 线宽 + Value 线宽。
     */
    private Boolean lengthIncludesTag;
    /**
     * 为 {@code true} 时，Length 数值 = Length 字段 + Tag + Value（整段总字节数，少数设备/帧格式）。
     */
    private Boolean lengthIncludesLengthField;
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

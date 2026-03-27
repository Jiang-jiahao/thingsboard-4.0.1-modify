/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import lombok.Data;

import java.util.List;

/**
 * One extracted field. Types:
 * <ul>
 *   <li>UINT8, UINT16_LE, UINT16_BE, UINT32_LE, UINT32_BE</li>
 *   <li>FLOAT32_LE, FLOAT32_BE, FLOAT64_LE, FLOAT64_BE — IEEE 754</li>
 *   <li>HEX_SLICE — optional {@code length}: if null or &lt;= 0, reads to buffer end</li>
 *   <li>HEX_SLICE_LEN_U16LE — parameter bytes: read uint16 LE at {@code lengthFieldOffset} (default 5) as N, then
 *       hex slice [{@code offset}, {@code offset}+N) (typically offset 7)</li>
 *   <li>BOOL_BIT — uses {@code offset} as byte index and {@code bitIndex} 0..7</li>
 *   <li>TLV_LIST — from {@code offset} to end (or {@code toOffsetExclusive}): repeated paramId (uint16 BE/LE) + len (uint8) + value.
 *       If {@link #listItemFields} is set, every value is parsed with the same inner fields (offsets 0-based inside value).
 *       Otherwise optional {@link #tlvNestedRules}: per {@code paramId}, parse value with nested fields (legacy).</li>
 *   <li>UNIT_LIST — repeating information units: uint32 LE {@code unitPayloadLength} at {@code unitLengthFieldOffset} (default 0) from each unit start
 *       means byte count of (编号 + 数据内容) following that length field; then {@code unitIdByteLength} (default 4) bytes for 编号 parsed with
 *       {@link #listItemFields} (offsets 0-based inside 编号); remaining bytes in the unit are data content (hex string under {@code unitDataOutputName}).</li>
 *   <li>STRUCT — nested layout: {@link #nestedFields} use offsets relative to this field's {@code offset}. Span is {@code length} bytes if set &gt; 0,
 *       else exclusive end {@code toOffsetExclusive} (same rules as TLV_LIST), else to buffer end.</li>
 *   <li>GENERIC_LIST — repeating records inside [{@code offset},{@code toOffsetExclusive}): {@code listCountMode} FIXED (repeat {@code listCount} times),
 *       FROM_FIELD (read count at {@code listCountFieldOffset} with {@code listCountFieldType}), or UNTIL_END (consume region).
 *       Each element size via {@code listItemLengthMode}: FIXED ({@code listItemFixedLength} bytes) or PREFIX_UINT8 / PREFIX_UINT16_LE|BE / PREFIX_UINT32_LE
 *       (length prefix then payload). Parse each payload with {@link #listItemFields} (offsets 0-based in payload); if empty, output {@code itemRawHex} only.</li>
 * </ul>
 */
@Data
public class HexFieldDefinition {

    private String name;
    private int offset;
    private String type;
    /** HEX_SLICE length in bytes; optional */
    private Integer length;
    /** BOOL_BIT: bit index within byte */
    private Integer bitIndex;
    /** TLV_LIST: exclusive end offset; 0 or negative = buffer.length + value */
    private Integer toOffsetExclusive;
    /** TLV_LIST param id size (default 2) */
    private Integer tlvIdSize;
    /** TLV_LIST: BE or LE for param id (default BE) */
    private String tlvIdEndian;

    /**
     * HEX_SLICE_LEN_U16LE: byte offset of uint16 little-endian length N (default 5).
     */
    private Integer lengthFieldOffset;

    /** TLV_LIST: optional inner templates keyed by param id (used when {@link #listItemFields} is empty) */
    private List<TlvNestedRule> tlvNestedRules;

    /** TLV_LIST: parse every item's value bytes with these fields (same layout for all entries); offsets relative to value start */
    private List<HexFieldDefinition> listItemFields;

    /** UNIT_LIST: byte offset from each unit start where uint32 LE payload length is read (default 0) */
    private Integer unitLengthFieldOffset;
    /** UNIT_LIST: 编号 length in bytes (default 4); payload after length field must be &gt;= this */
    private Integer unitIdByteLength;
    /** UNIT_LIST: JSON key for data content hex (default dataContentHex) */
    private String unitDataOutputName;
    /** UNIT_LIST: JSON key for parsed 编号 object (default number); if no listItemFields, outputs idHex under same parent */
    private String unitNumberOutputName;

    /** STRUCT: child fields; offsets relative to this field's byte range start */
    private List<HexFieldDefinition> nestedFields;

    /** GENERIC_LIST: FIXED | FROM_FIELD | UNTIL_END */
    private String listCountMode;
    /** GENERIC_LIST when listCountMode=FIXED */
    private Integer listCount;
    /** GENERIC_LIST when listCountMode=FROM_FIELD: absolute index in current buffer */
    private Integer listCountFieldOffset;
    /** GENERIC_LIST: UINT8, UINT16_LE, UINT16_BE, UINT32_LE, UINT32_BE */
    private String listCountFieldType;

    /** GENERIC_LIST: FIXED | PREFIX_UINT8 | PREFIX_UINT16_LE | PREFIX_UINT16_BE | PREFIX_UINT32_LE */
    private String listItemLengthMode;
    /** GENERIC_LIST when listItemLengthMode=FIXED */
    private Integer listItemFixedLength;

}

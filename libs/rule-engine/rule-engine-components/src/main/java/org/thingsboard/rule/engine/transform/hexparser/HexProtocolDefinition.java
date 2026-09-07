/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import lombok.Data;

import java.util.List;

/**
 * One protocol layout. Match either by {@link #getId()} equals incoming {@code protocolId} message key,
 * or by {@link #getSyncHex()} prefix at {@link #getSyncOffset()}.
 * <p>
 * Optional {@link #commandByteOffset} + {@link #commandValue}: when both set, bytes at that index must match.
 * Default match is 1 byte (0–255). Set {@link #commandMatchWidth} to {@code 4} for uint32 little-endian
 * (e.g. monitoring UDP command at offset 12).
 * <p>
 * Optional {@link #templateId}: merge with {@link HexFrameTemplate} — header and shared payload layout come from the
 * template ({@link HexFrameTemplate#getPayloadFields()}); this row adds command matching and optional
 * {@link #getFields()} for command-specific extras. Offsets in {@code fields} are relative to template
 * {@code paramStartOffset} when {@link #payloadOffsetsRelative} is true.
 * <p>
 * Headless frames (no {@code syncHex}): if {@link #commandValue} is {@code null}, any command dword matches
 * (monitoring UDP datagram style: same layout for all command numbers).
 */
@Data
public class HexProtocolDefinition {

    private String id;
    /** References {@link HexFrameTemplate#getId()}; when set, sync/checksum/header come from template */
    private String templateId;
    /** When using template: if true (default), field offsets are relative to param area start */
    private Boolean payloadOffsetsRelative;
    /** Hex string e.g. A55A — matched at syncOffset */
    private String syncHex;
    private int syncOffset;
    private int minBytes;
    /** If set with {@link #commandValue}, bytes at this index must match (see {@link #commandMatchWidth}) */
    private Integer commandByteOffset;
    /**
     * Expected value: single byte 0–255, or uint32 LE when {@link #commandMatchWidth} is 4.
     * When {@code null} on a headless (no sync) definition, any command is accepted.
     */
    private Integer commandValue;
    /** 1 (default) = compare one byte; 4 = compare uint32 LE at commandByteOffset */
    private Integer commandMatchWidth;
    private HexChecksumDefinition checksum;
    private List<HexFieldDefinition> fields;

}

/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import lombok.Data;

import java.util.List;

/**
 * Frame template: sync, optional checksum, {@link #headerFields} (only source of header layout),
 * and layout hints for payload ({@link #paramStartOffset}, {@link #paramLenFieldOffset}).
 * No fixed field names are generated in code — define everything in {@code headerFields}.
 */
@Data
public class HexFrameTemplate {

    private String id;
    /** e.g. A55A */
    private String syncHex;
    private int syncOffset;
    /** Minimum frame length hint; 0 = ignore */
    private int minBytes;
    /**
     * Byte index used with {@link HexProtocolDefinition#getCommandValue()} when the protocol omits
     * {@code commandByteOffset}. Optional.
     */
    private Integer commandMatchOffset;
    /** Reference byte index for variable payload length (default 5) — used when payload fields use HEX_SLICE_LEN_U16LE without lengthFieldOffset */
    private Integer paramLenFieldOffset;
    /** First payload byte index (default 7) — base for relative payload offsets */
    private Integer paramStartOffset;
    /** Optional default checksum when variant does not define one */
    private HexChecksumDefinition checksum;
    /** Header fields: names, offsets, types — required for any header bytes before payload */
    private List<HexFieldDefinition> headerFields;

}

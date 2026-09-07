/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import lombok.Data;

/**
 * Optional integrity check. Negative byte indices are relative to buffer end (e.g. -1 = last byte).
 */
@Data
public class HexChecksumDefinition {

    /**
     * SUM8: sum bytes in [fromByte, toExclusive) masked to 8 bits; CRC16_MODBUS: poly 0xA001 init 0xFFFF, 2 bytes LE at index;
     * CRC16_CCITT: CRC-16/CCITT-FALSE poly 0x1021 init 0xFFFF, 2 bytes BE at index.
     */
    private String type;

    private int fromByte;
    /**
     * Exclusive end; if negative, resolved as {@code buffer.length + toExclusive} (e.g. -1 = up to last byte excluded when checksum follows).
     */
    private int toExclusive;
    /**
     * Byte index of checksum field; negative = length + value.
     */
    private int checksumByteIndex;

}

/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import lombok.Data;

import java.util.List;

/**
 * For {@link HexFieldDefinition#getType()} {@code TLV_LIST}: optional inner layout for a given
 * parameter id. Value bytes are parsed with {@link #fields} using offsets relative to the start of that value.
 */
@Data
public class TlvNestedRule {

    private int paramId;
    private List<HexFieldDefinition> fields;
}

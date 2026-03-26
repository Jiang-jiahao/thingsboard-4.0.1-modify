/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import java.util.ArrayList;
import java.util.List;

/**
 * Merges {@link HexFrameTemplate} with a variant {@link HexProtocolDefinition} that lists only payload fields.
 */
public final class HexProtocolExpander {

    private HexProtocolExpander() {
    }

    public static HexFrameTemplate findTemplate(List<HexFrameTemplate> templates, String id) {
        if (templates == null || id == null || id.isEmpty()) {
            return null;
        }
        for (HexFrameTemplate t : templates) {
            if (id.equals(t.getId())) {
                return t;
            }
        }
        return null;
    }

    /**
     * If {@code raw.getTemplateId()} is set, builds a full definition: header fields from template +
     * payload fields (offsets optionally shifted by {@code paramStartOffset}).
     * Otherwise returns {@code raw} unchanged.
     */
    public static HexProtocolDefinition expand(HexProtocolDefinition raw, List<HexFrameTemplate> templates) {
        if (raw.getTemplateId() == null || raw.getTemplateId().isEmpty()) {
            return raw;
        }
        HexFrameTemplate t = findTemplate(templates, raw.getTemplateId());
        if (t == null) {
            throw new IllegalArgumentException("Unknown frame template id: " + raw.getTemplateId());
        }
        boolean relative = raw.getPayloadOffsetsRelative() == null || Boolean.TRUE.equals(raw.getPayloadOffsetsRelative());
        int base = intOrDefault(t.getParamStartOffset(), 7);
        int lenOff = intOrDefault(t.getParamLenFieldOffset(), 5);

        HexProtocolDefinition out = new HexProtocolDefinition();
        out.setId(raw.getId());
        out.setSyncHex(t.getSyncHex());
        out.setSyncOffset(t.getSyncOffset());
        out.setChecksum(resolveChecksum(raw, t));
        out.setCommandByteOffset(raw.getCommandByteOffset());
        out.setCommandValue(raw.getCommandValue());
        out.setCommandMatchWidth(raw.getCommandMatchWidth());

        int min = Math.max(raw.getMinBytes(), t.getMinBytes());
        out.setMinBytes(min);

        List<HexFieldDefinition> merged = new ArrayList<>();
        if (t.getHeaderFields() != null) {
            for (HexFieldDefinition hf : t.getHeaderFields()) {
                merged.add(copyField(hf));
            }
        }

        if (raw.getFields() != null) {
            for (HexFieldDefinition pf : raw.getFields()) {
                merged.add(shiftPayloadField(pf, t, relative, base, lenOff));
            }
        }
        out.setFields(merged);
        return out;
    }

    private static HexChecksumDefinition resolveChecksum(HexProtocolDefinition raw, HexFrameTemplate t) {
        HexChecksumDefinition r = raw.getChecksum();
        if (r != null && r.getType() != null) {
            if ("NONE".equalsIgnoreCase(r.getType())) {
                return null;
            }
            return r;
        }
        return t != null ? t.getChecksum() : null;
    }

    private static int intOrDefault(Integer v, int def) {
        return v != null ? v : def;
    }

    private static HexFieldDefinition shiftPayloadField(HexFieldDefinition pf, HexFrameTemplate t,
                                                      boolean relative, int base, int lenOff) {
        HexFieldDefinition c = copyField(pf);
        if (relative) {
            c.setOffset(pf.getOffset() + base);
            if ("HEX_SLICE_LEN_U16LE".equalsIgnoreCase(pf.getType())) {
                if (c.getLengthFieldOffset() == null) {
                    c.setLengthFieldOffset(lenOff);
                }
            }
            if ("TLV_LIST".equalsIgnoreCase(pf.getType()) && pf.getToOffsetExclusive() != null && pf.getToOffsetExclusive() > 0) {
                c.setToOffsetExclusive(pf.getToOffsetExclusive() + base);
            }
            if ("UNIT_LIST".equalsIgnoreCase(pf.getType()) && pf.getToOffsetExclusive() != null && pf.getToOffsetExclusive() > 0) {
                c.setToOffsetExclusive(pf.getToOffsetExclusive() + base);
            }
        } else if ("HEX_SLICE_LEN_U16LE".equalsIgnoreCase(pf.getType()) && c.getLengthFieldOffset() == null) {
            c.setLengthFieldOffset(lenOff);
        }
        return c;
    }

    private static HexFieldDefinition copyField(HexFieldDefinition pf) {
        HexFieldDefinition c = new HexFieldDefinition();
        c.setName(pf.getName());
        c.setOffset(pf.getOffset());
        c.setType(pf.getType());
        c.setLength(pf.getLength());
        c.setBitIndex(pf.getBitIndex());
        c.setToOffsetExclusive(pf.getToOffsetExclusive());
        c.setTlvIdSize(pf.getTlvIdSize());
        c.setTlvIdEndian(pf.getTlvIdEndian());
        c.setLengthFieldOffset(pf.getLengthFieldOffset());
        if (pf.getTlvNestedRules() != null) {
            List<TlvNestedRule> rules = new ArrayList<>();
            for (TlvNestedRule r : pf.getTlvNestedRules()) {
                TlvNestedRule nr = new TlvNestedRule();
                nr.setParamId(r.getParamId());
                if (r.getFields() != null) {
                    List<HexFieldDefinition> nfs = new ArrayList<>();
                    for (HexFieldDefinition hf : r.getFields()) {
                        nfs.add(copyField(hf));
                    }
                    nr.setFields(nfs);
                }
                rules.add(nr);
            }
            c.setTlvNestedRules(rules);
        }
        if (pf.getListItemFields() != null) {
            List<HexFieldDefinition> inner = new ArrayList<>();
            for (HexFieldDefinition lf : pf.getListItemFields()) {
                inner.add(copyField(lf));
            }
            c.setListItemFields(inner);
        }
        c.setUnitLengthFieldOffset(pf.getUnitLengthFieldOffset());
        c.setUnitIdByteLength(pf.getUnitIdByteLength());
        c.setUnitDataOutputName(pf.getUnitDataOutputName());
        c.setUnitNumberOutputName(pf.getUnitNumberOutputName());
        return c;
    }

}

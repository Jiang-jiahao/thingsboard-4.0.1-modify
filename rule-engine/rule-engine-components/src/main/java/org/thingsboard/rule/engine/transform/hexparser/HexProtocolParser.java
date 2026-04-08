/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.rule.engine.transform.hexparser;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.thingsboard.common.util.JacksonUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Config-driven binary parser for hex string payloads.
 */
public final class HexProtocolParser {

    private HexProtocolParser() {
    }

    public static byte[] parseHexString(String hex) {
        if (hex == null) {
            return null;
        }
        String s = hex.replaceAll("\\s+", "");
        if ((s.length() & 1) != 0) {
            throw new IllegalArgumentException("Hex length must be even");
        }
        int n = s.length() / 2;
        byte[] buf = new byte[n];
        for (int i = 0; i < n; i++) {
            buf[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return buf;
    }

    public static int resolveIndex(byte[] buf, int idx) {
        if (idx >= 0) {
            return idx;
        }
        return buf.length + idx;
    }

    public static ObjectNode parse(HexProtocolDefinition def, byte[] buf) {
        if (def.getMinBytes() > 0 && buf.length < def.getMinBytes()) {
            throw new IllegalArgumentException("Buffer too short: " + buf.length + " < " + def.getMinBytes());
        }
        if (Boolean.TRUE.equals(def.getValidateTotalLengthU32Le())) {
            if (buf.length < 4) {
                throw new IllegalArgumentException("Need 4 bytes for total length field");
            }
            int declared = readU32LE(buf, 0);
            if (declared != buf.length) {
                throw new IllegalArgumentException("Packet length field " + declared + " != buffer length " + buf.length);
            }
        }
        if (def.getSyncHex() != null && !def.getSyncHex().isEmpty()) {
            byte[] sync = parseHexString(def.getSyncHex());
            int off = def.getSyncOffset();
            if (off + sync.length > buf.length) {
                throw new IllegalArgumentException("Sync past end");
            }
            for (int i = 0; i < sync.length; i++) {
                if (buf[off + i] != sync[i]) {
                    throw new IllegalArgumentException("Sync mismatch");
                }
            }
        }
        validateChecksum(def.getChecksum(), buf);

        ObjectNode out = JacksonUtil.newObjectNode();
        out.put("protocolId", def.getId());
        out.put("parseOk", true);
        if (def.getFields() != null) {
            for (HexFieldDefinition f : def.getFields()) {
                putField(out, f, buf);
            }
        }
        return out;
    }

    private static void validateChecksum(HexChecksumDefinition cs, byte[] buf) {
        if (cs == null || cs.getType() == null || "NONE".equalsIgnoreCase(cs.getType())) {
            return;
        }
        int from = resolveIndex(buf, cs.getFromByte());
        int toEx = resolveIndex(buf, cs.getToExclusive());
        int cksAt = resolveIndex(buf, cs.getChecksumByteIndex());
        if (from < 0 || toEx > buf.length || from > toEx) {
            throw new IllegalArgumentException("Checksum range invalid");
        }
        if (cksAt < 0 || cksAt >= buf.length) {
            throw new IllegalArgumentException("Checksum index invalid");
        }
        String t = cs.getType().toUpperCase();
        switch (t) {
            case "SUM8":
                int sum = 0;
                for (int i = from; i < toEx; i++) {
                    sum = (sum + (buf[i] & 0xFF)) & 0xFF;
                }
                if (sum != (buf[cksAt] & 0xFF)) {
                    throw new IllegalArgumentException("SUM8 mismatch");
                }
                break;
            case "CRC16_MODBUS":
                if (cksAt + 1 >= buf.length) {
                    throw new IllegalArgumentException("CRC16_MODBUS: need 2 bytes at checksum index");
                }
                int crc = crc16Modbus(buf, from, toEx);
                int lo = buf[cksAt] & 0xFF;
                int hi = buf[cksAt + 1] & 0xFF;
                int actual = lo | (hi << 8);
                if (crc != actual) {
                    throw new IllegalArgumentException("CRC16_MODBUS mismatch expected " + crc + " got " + actual);
                }
                break;
            case "CRC16_CCITT":
                if (cksAt + 1 >= buf.length) {
                    throw new IllegalArgumentException("CRC16_CCITT: need 2 bytes at checksum index");
                }
                int cc = crc16Ccitt(buf, from, toEx);
                int ccHi = buf[cksAt] & 0xFF;
                int ccLo = buf[cksAt + 1] & 0xFF;
                int ccActual = (ccHi << 8) | ccLo;
                if (cc != ccActual) {
                    throw new IllegalArgumentException("CRC16_CCITT mismatch expected " + cc + " got " + ccActual);
                }
                break;
            case "CRC32":
                if (cksAt + 3 >= buf.length) {
                    throw new IllegalArgumentException("CRC32: need 4 bytes at checksum index");
                }
                long crc32 = crc32IEEE(buf, from, toEx);
                long a = readU32LE(buf, cksAt) & 0xFFFFFFFFL;
                if ((crc32 & 0xFFFFFFFFL) != a) {
                    throw new IllegalArgumentException("CRC32 mismatch");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown checksum type: " + cs.getType());
        }
    }

    private static int readU32LE(byte[] b, int i) {
        return (b[i] & 0xFF) | ((b[i + 1] & 0xFF) << 8) | ((b[i + 2] & 0xFF) << 16) | ((b[i + 3] & 0xFF) << 24);
    }

    private static int readS32BE(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24)
                | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8)
                | (b[off + 3] & 0xFF);
    }

    private static long readU64LE(byte[] b, int off) {
        return (b[off] & 0xFFL)
                | ((b[off + 1] & 0xFFL) << 8)
                | ((b[off + 2] & 0xFFL) << 16)
                | ((b[off + 3] & 0xFFL) << 24)
                | ((b[off + 4] & 0xFFL) << 32)
                | ((b[off + 5] & 0xFFL) << 40)
                | ((b[off + 6] & 0xFFL) << 48)
                | ((long) (b[off + 7] & 0xFF) << 56);
    }

    private static long readU64BE(byte[] b, int off) {
        return ((long) (b[off] & 0xFF) << 56)
                | ((long) (b[off + 1] & 0xFF) << 48)
                | ((long) (b[off + 2] & 0xFF) << 40)
                | ((long) (b[off + 3] & 0xFF) << 32)
                | ((long) (b[off + 4] & 0xFF) << 24)
                | ((b[off + 5] & 0xFFL) << 16)
                | ((b[off + 6] & 0xFFL) << 8)
                | (b[off + 7] & 0xFFL);
    }

    /**
     * CRC-32 IEEE 802.3 (Ethernet)
     */
    private static long crc32IEEE(byte[] data, int from, int to) {
        long crc = 0xFFFFFFFFL;
        for (int i = from; i < to; i++) {
            crc ^= (data[i] & 0xFFL);
            for (int k = 0; k < 8; k++) {
                if ((crc & 1) != 0) {
                    crc = (crc >>> 1) ^ 0xEDB88320L;
                } else {
                    crc = crc >>> 1;
                }
            }
        }
        return (~crc) & 0xFFFFFFFFL;
    }

    public static int crc16Modbus(byte[] buf, int from, int to) {
        int crc = 0xFFFF;
        for (int i = from; i < to; i++) {
            crc ^= (buf[i] & 0xFF);
            for (int j = 0; j < 8; j++) {
                if ((crc & 1) != 0) {
                    crc = (crc >>> 1) ^ 0xA001;
                } else {
                    crc = crc >>> 1;
                }
            }
        }
        return crc & 0xFFFF;
    }

    /**
     * CRC-16/CCITT-FALSE: poly 0x1021, init 0xFFFF; checksum stored big-endian at {@code checksumByteIndex}.
     */
    public static int crc16Ccitt(byte[] buf, int from, int to) {
        int crc = 0xFFFF;
        for (int pos = from; pos < to; pos++) {
            crc ^= (buf[pos] & 0xFF) << 8;
            for (int i = 0; i < 8; i++) {
                if ((crc & 0x8000) != 0) {
                    crc = (crc << 1) ^ 0x1021;
                } else {
                    crc <<= 1;
                }
                crc &= 0xFFFF;
            }
        }
        return crc & 0xFFFF;
    }

    private static void putField(ObjectNode out, HexFieldDefinition f, byte[] buf) {
        String t = f.getType() != null ? f.getType().toUpperCase() : "UINT8";
        String name = f.getName();
        switch (t) {
            case "UINT8":
                checkBounds(buf, f.getOffset(), 1);
                out.put(name, buf[f.getOffset()] & 0xFF);
                break;
            case "UINT16_LE":
                checkBounds(buf, f.getOffset(), 2);
                out.put(name, (buf[f.getOffset()] & 0xFF) | ((buf[f.getOffset() + 1] & 0xFF) << 8));
                break;
            case "UINT16_BE":
                checkBounds(buf, f.getOffset(), 2);
                out.put(name, ((buf[f.getOffset()] & 0xFF) << 8) | (buf[f.getOffset() + 1] & 0xFF));
                break;
            case "UINT32_LE":
                checkBounds(buf, f.getOffset(), 4);
                long v = (buf[f.getOffset()] & 0xFFL)
                        | ((buf[f.getOffset() + 1] & 0xFFL) << 8)
                        | ((buf[f.getOffset() + 2] & 0xFFL) << 16)
                        | ((buf[f.getOffset() + 3] & 0xFFL) << 24);
                out.put(name, v & 0xFFFFFFFFL);
                break;
            case "UINT32_BE":
                checkBounds(buf, f.getOffset(), 4);
                long vb = ((buf[f.getOffset()] & 0xFFL) << 24)
                        | ((buf[f.getOffset() + 1] & 0xFFL) << 16)
                        | ((buf[f.getOffset() + 2] & 0xFFL) << 8)
                        | (buf[f.getOffset() + 3] & 0xFFL);
                out.put(name, vb & 0xFFFFFFFFL);
                break;
            case "FLOAT32_LE":
                checkBounds(buf, f.getOffset(), 4);
                out.put(name, Float.intBitsToFloat(readU32LE(buf, f.getOffset())));
                break;
            case "FLOAT32_BE":
                checkBounds(buf, f.getOffset(), 4);
                out.put(name, Float.intBitsToFloat(readS32BE(buf, f.getOffset())));
                break;
            case "FLOAT64_LE":
                checkBounds(buf, f.getOffset(), 8);
                out.put(name, Double.longBitsToDouble(readU64LE(buf, f.getOffset())));
                break;
            case "FLOAT64_BE":
                checkBounds(buf, f.getOffset(), 8);
                out.put(name, Double.longBitsToDouble(readU64BE(buf, f.getOffset())));
                break;
            case "HEX_SLICE":
                int len = f.getLength() != null ? f.getLength() : 0;
                int start = f.getOffset();
                int end = len > 0 ? start + len : buf.length;
                if (start > buf.length || end > buf.length || start > end) {
                    throw new IllegalArgumentException("HEX_SLICE bounds");
                }
                out.put(name, hexSlice(buf, start, end));
                break;
            case "HEX_SLICE_LEN_U16LE":
                int lfOff = f.getLengthFieldOffset() != null ? f.getLengthFieldOffset() : 5;
                int payloadOff = f.getOffset();
                checkBounds(buf, lfOff, 2);
                int n = (buf[lfOff] & 0xFF) | ((buf[lfOff + 1] & 0xFF) << 8);
                if (payloadOff + n > buf.length) {
                    throw new IllegalArgumentException("HEX_SLICE_LEN_U16LE: payload past buffer");
                }
                out.put(name, hexSlice(buf, payloadOff, payloadOff + n));
                break;
            case "BOOL_BIT":
                int bi = f.getBitIndex() != null ? f.getBitIndex() : 0;
                checkBounds(buf, f.getOffset(), 1);
                out.put(name, ((buf[f.getOffset()] >> bi) & 1) == 1);
                break;
            case "STRUCT":
                putStruct(out, f, buf);
                break;
            case "GENERIC_LIST":
                putGenericList(out, f, buf);
                break;
            case "TLV_LIST":
                int tlvStart = f.getOffset();
                int tlvEnd = f.getToOffsetExclusive() != null && f.getToOffsetExclusive() != 0
                        ? resolveIndex(buf, f.getToOffsetExclusive())
                        : buf.length;
                int idSize = f.getTlvIdSize() != null ? f.getTlvIdSize() : 2;
                boolean idBe = f.getTlvIdEndian() == null || "BE".equalsIgnoreCase(f.getTlvIdEndian());
                ArrayNode arr = JacksonUtil.newArrayNode();
                int p = tlvStart;
                while (p + idSize + 1 <= tlvEnd) {
                    int paramId = idSize == 2
                            ? (idBe ? ((buf[p] & 0xFF) << 8 | (buf[p + 1] & 0xFF)) : ((buf[p + 1] & 0xFF) << 8 | (buf[p] & 0xFF)))
                            : (buf[p] & 0xFF);
                    p += idSize;
                    int plen = buf[p] & 0xFF;
                    p++;
                    if (p + plen > tlvEnd) {
                        break;
                    }
                    ObjectNode item = JacksonUtil.newObjectNode();
                    item.put("paramId", paramId);
                    item.put("len", plen);
                    item.put("valueHex", hexSlice(buf, p, p + plen));
                    byte[] valueSlice = Arrays.copyOfRange(buf, p, p + plen);
                    List<HexFieldDefinition> listItemFields = f.getListItemFields();
                    if (listItemFields != null && !listItemFields.isEmpty()) {
                        ObjectNode nested = JacksonUtil.newObjectNode();
                        for (HexFieldDefinition nf : listItemFields) {
                            putField(nested, nf, valueSlice);
                        }
                        item.set("nested", nested);
                    } else {
                        List<TlvNestedRule> nestedRules = f.getTlvNestedRules();
                        if (nestedRules != null) {
                            for (TlvNestedRule rule : nestedRules) {
                                if (rule.getParamId() == paramId && rule.getFields() != null && !rule.getFields().isEmpty()) {
                                    ObjectNode nested = JacksonUtil.newObjectNode();
                                    for (HexFieldDefinition nf : rule.getFields()) {
                                        putField(nested, nf, valueSlice);
                                    }
                                    item.set("nested", nested);
                                    break;
                                }
                            }
                        }
                    }
                    arr.add(item);
                    p += plen;
                }
                out.set(name, arr);
                break;
            case "UNIT_LIST":
                int unitStart = f.getOffset();
                int unitEnd = f.getToOffsetExclusive() != null && f.getToOffsetExclusive() != 0
                        ? resolveIndex(buf, f.getToOffsetExclusive())
                        : buf.length;
                int lenOffInUnit = f.getUnitLengthFieldOffset() != null ? f.getUnitLengthFieldOffset() : 0;
                int idBytes = f.getUnitIdByteLength() != null ? f.getUnitIdByteLength() : 4;
                String dataKey = f.getUnitDataOutputName() != null && !f.getUnitDataOutputName().isEmpty()
                        ? f.getUnitDataOutputName() : "dataContentHex";
                String numKey = f.getUnitNumberOutputName() != null && !f.getUnitNumberOutputName().isEmpty()
                        ? f.getUnitNumberOutputName() : "number";
                ArrayNode units = JacksonUtil.newArrayNode();
                int up = unitStart;
                while (up < unitEnd) {
                    int lenFieldAt = up + lenOffInUnit;
                    if (lenFieldAt + 4 > unitEnd) {
                        break;
                    }
                    int payloadLen = readU32LE(buf, lenFieldAt);
                    if (payloadLen < 0) {
                        break;
                    }
                    int bodyStart = up + lenOffInUnit + 4;
                    if (bodyStart + payloadLen > unitEnd) {
                        break;
                    }
                    if (payloadLen < idBytes) {
                        break;
                    }
                    ObjectNode unitItem = JacksonUtil.newObjectNode();
                    unitItem.put("payloadLength", payloadLen);
                    byte[] idSlice = Arrays.copyOfRange(buf, bodyStart, bodyStart + idBytes);
                    int dataLen = payloadLen - idBytes;
                    int dataStart = bodyStart + idBytes;
                    byte[] dataSlice = Arrays.copyOfRange(buf, dataStart, dataStart + dataLen);
                    List<HexFieldDefinition> idFields = f.getListItemFields();
                    if (idFields != null && !idFields.isEmpty()) {
                        ObjectNode numberObj = JacksonUtil.newObjectNode();
                        for (HexFieldDefinition nf : idFields) {
                            putField(numberObj, nf, idSlice);
                        }
                        unitItem.set(numKey, numberObj);
                    } else {
                        unitItem.put("idHex", hexSlice(idSlice, 0, idSlice.length));
                    }
                    unitItem.put(dataKey, hexSlice(dataSlice, 0, dataSlice.length));
                    units.add(unitItem);
                    up = bodyStart + payloadLen;
                }
                out.set(name, units);
                break;
            default:
                throw new IllegalArgumentException("Unknown field type: " + f.getType());
        }
    }

    private static void putStruct(ObjectNode out, HexFieldDefinition f, byte[] buf) {
        int start = f.getOffset();
        if (start < 0 || start > buf.length) {
            throw new IllegalArgumentException("STRUCT: offset out of range");
        }
        int end;
        if (f.getLength() != null && f.getLength() > 0) {
            end = Math.min(buf.length, start + f.getLength());
        } else if (f.getToOffsetExclusive() != null && f.getToOffsetExclusive() != 0) {
            end = resolveIndex(buf, f.getToOffsetExclusive());
        } else {
            end = buf.length;
        }
        if (end < start || end > buf.length) {
            throw new IllegalArgumentException("STRUCT: invalid byte range");
        }
        byte[] sub = Arrays.copyOfRange(buf, start, end);
        ObjectNode structNode = JacksonUtil.newObjectNode();
        List<HexFieldDefinition> nested = f.getNestedFields();
        if (nested != null) {
            for (HexFieldDefinition nf : nested) {
                putField(structNode, nf, sub);
            }
        }
        out.set(f.getName(), structNode);
    }

    private static final class GenericElement {
        final int payloadStart;
        final int payloadLen;
        final int nextPos;

        GenericElement(int payloadStart, int payloadLen, int nextPos) {
            this.payloadStart = payloadStart;
            this.payloadLen = payloadLen;
            this.nextPos = nextPos;
        }
    }

    private static void putGenericList(ObjectNode out, HexFieldDefinition f, byte[] buf) {
        int regionStart = f.getOffset();
        int regionEnd = f.getToOffsetExclusive() != null && f.getToOffsetExclusive() != 0
                ? resolveIndex(buf, f.getToOffsetExclusive())
                : buf.length;
        if (regionStart < 0 || regionStart > regionEnd || regionEnd > buf.length) {
            throw new IllegalArgumentException("GENERIC_LIST: invalid region");
        }
        String countMode = f.getListCountMode() != null ? f.getListCountMode().toUpperCase() : "UNTIL_END";
        String lenMode = f.getListItemLengthMode() != null ? f.getListItemLengthMode().toUpperCase() : "PREFIX_UINT8";

        int count = -1;
        switch (countMode) {
            case "FIXED":
                if (f.getListCount() == null || f.getListCount() < 0) {
                    throw new IllegalArgumentException("GENERIC_LIST FIXED requires listCount >= 0");
                }
                count = f.getListCount();
                break;
            case "FROM_FIELD":
                if (f.getListCountFieldOffset() == null) {
                    throw new IllegalArgumentException("GENERIC_LIST FROM_FIELD requires listCountFieldOffset");
                }
                count = readUIntAt(buf, f.getListCountFieldOffset(), f.getListCountFieldType());
                break;
            case "UNTIL_END":
                count = -1;
                break;
            default:
                throw new IllegalArgumentException("GENERIC_LIST unknown listCountMode: " + countMode);
        }

        ArrayNode arr = JacksonUtil.newArrayNode();
        int pos = regionStart;
        if (count >= 0) {
            for (int i = 0; i < count; i++) {
                GenericElement el = readGenericElement(buf, pos, regionEnd, lenMode, f.getListItemFixedLength());
                if (el == null) {
                    throw new IllegalArgumentException("GENERIC_LIST: truncated item " + i);
                }
                arr.add(buildGenericListItem(buf, el, f));
                pos = el.nextPos;
            }
        } else {
            while (pos < regionEnd) {
                GenericElement el = readGenericElement(buf, pos, regionEnd, lenMode, f.getListItemFixedLength());
                if (el == null) {
                    break;
                }
                arr.add(buildGenericListItem(buf, el, f));
                pos = el.nextPos;
            }
        }
        out.set(f.getName(), arr);
    }

    private static GenericElement readGenericElement(byte[] buf, int pos, int regionEnd, String lenMode, Integer fixedLen) {
        if (pos > regionEnd) {
            return null;
        }
        if (pos == regionEnd) {
            return null;
        }
        if ("FIXED".equals(lenMode)) {
            int fl = fixedLen != null ? fixedLen : 0;
            if (fl <= 0) {
                throw new IllegalArgumentException("GENERIC_LIST listItemLengthMode FIXED requires listItemFixedLength > 0");
            }
            if (pos + fl > regionEnd) {
                return null;
            }
            return new GenericElement(pos, fl, pos + fl);
        }
        int bodyStart;
        long lenLong;
        switch (lenMode) {
            case "PREFIX_UINT8":
                if (pos + 1 > regionEnd) {
                    return null;
                }
                lenLong = buf[pos] & 0xFFL;
                bodyStart = pos + 1;
                break;
            case "PREFIX_UINT16_LE":
                if (pos + 2 > regionEnd) {
                    return null;
                }
                lenLong = (buf[pos] & 0xFFL) | ((buf[pos + 1] & 0xFFL) << 8);
                bodyStart = pos + 2;
                break;
            case "PREFIX_UINT16_BE":
                if (pos + 2 > regionEnd) {
                    return null;
                }
                lenLong = ((buf[pos] & 0xFFL) << 8) | (buf[pos + 1] & 0xFFL);
                bodyStart = pos + 2;
                break;
            case "PREFIX_UINT32_LE":
                if (pos + 4 > regionEnd) {
                    return null;
                }
                lenLong = readU32LE(buf, pos) & 0xFFFFFFFFL;
                bodyStart = pos + 4;
                break;
            default:
                throw new IllegalArgumentException("GENERIC_LIST unknown listItemLengthMode: " + lenMode);
        }
        if (lenLong < 0 || lenLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("GENERIC_LIST invalid item length");
        }
        int L = (int) lenLong;
        if (bodyStart + L > regionEnd) {
            return null;
        }
        return new GenericElement(bodyStart, L, bodyStart + L);
    }

    private static ObjectNode buildGenericListItem(byte[] buf, GenericElement el, HexFieldDefinition f) {
        byte[] payload = Arrays.copyOfRange(buf, el.payloadStart, el.payloadStart + el.payloadLen);
        ObjectNode item = JacksonUtil.newObjectNode();
        List<HexFieldDefinition> itemFields = f.getListItemFields();
        if (itemFields != null && !itemFields.isEmpty()) {
            for (HexFieldDefinition nf : itemFields) {
                putField(item, nf, payload);
            }
        } else {
            item.put("itemRawHex", hexSlice(payload, 0, payload.length));
        }
        return item;
    }

    private static int readUIntAt(byte[] buf, int off, String type) {
        String t = type != null ? type.toUpperCase() : "UINT8";
        switch (t) {
            case "UINT8":
                checkBounds(buf, off, 1);
                return buf[off] & 0xFF;
            case "UINT16_LE":
                checkBounds(buf, off, 2);
                return (buf[off] & 0xFF) | ((buf[off + 1] & 0xFF) << 8);
            case "UINT16_BE":
                checkBounds(buf, off, 2);
                return ((buf[off] & 0xFF) << 8) | (buf[off + 1] & 0xFF);
            case "UINT32_LE":
                checkBounds(buf, off, 4);
                long v = readU32LE(buf, off) & 0xFFFFFFFFL;
                if (v > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("GENERIC_LIST count exceeds int range");
                }
                return (int) v;
            case "UINT32_BE":
                checkBounds(buf, off, 4);
                long vb = ((long) (buf[off] & 0xFF) << 24)
                        | ((buf[off + 1] & 0xFFL) << 16)
                        | ((buf[off + 2] & 0xFFL) << 8)
                        | (buf[off + 3] & 0xFFL);
                if (vb > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("GENERIC_LIST count exceeds int range");
                }
                return (int) vb;
            default:
                throw new IllegalArgumentException("Unknown listCountFieldType: " + t);
        }
    }

    private static void checkBounds(byte[] buf, int off, int len) {
        if (off < 0 || off + len > buf.length) {
            throw new IllegalArgumentException("Field out of bounds at " + off);
        }
    }

    private static String hexSlice(byte[] buf, int from, int to) {
        StringBuilder sb = new StringBuilder((to - from) * 2);
        for (int i = from; i < to; i++) {
            sb.append(String.format("%02X", buf[i] & 0xFF));
        }
        return sb.toString();
    }

    public static HexProtocolDefinition findProtocol(List<HexProtocolDefinition> protocols, String protocolId,
                                                     byte[] buf) {
        return findProtocol(protocols, protocolId, buf, null);
    }

    /**
     * @param templates optional; used when a protocol has {@code templateId} but empty {@code syncHex}
     */
    public static HexProtocolDefinition findProtocol(List<HexProtocolDefinition> protocols, String protocolId,
                                                     byte[] buf, List<HexFrameTemplate> templates) {
        if (protocols == null || protocols.isEmpty()) {
            throw new IllegalArgumentException("No protocols configured");
        }
        // 仅当「id 一致且当前 hex 确实满足该协议的同步头/命令匹配」时才按 id 选用；否则继续自动匹配（避免错误 protocolId 导致乱解析）
        if (protocolId != null && !protocolId.isEmpty()) {
            for (HexProtocolDefinition d : protocols) {
                if (protocolId.equals(d.getId()) && protocolMatchesBuffer(d, buf, templates)) {
                    return d;
                }
            }
        }
        for (HexProtocolDefinition d : protocols) {
            String syncHex = effectiveSyncHex(d, templates);
            if (syncHex == null || syncHex.isEmpty()) {
                continue;
            }
            if (protocolMatchesBuffer(d, buf, templates)) {
                return d;
            }
        }
        for (HexProtocolDefinition d : protocols) {
            String syncHex = effectiveSyncHex(d, templates);
            if (syncHex != null && !syncHex.isEmpty()) {
                continue;
            }
            if (protocolMatchesBuffer(d, buf, templates)) {
                return d;
            }
        }
        throw new IllegalArgumentException("No matching protocol definition");
    }

    /**
     * True if buffer matches this definition: either sync (from protocol or template) + optional command match,
     * or headless (minBytes + command at template/command offset).
     */
    static boolean protocolMatchesBuffer(HexProtocolDefinition d, byte[] buf, List<HexFrameTemplate> templates) {
        String syncHex = effectiveSyncHex(d, templates);
        if (syncHex != null && !syncHex.isEmpty()) {
            try {
                byte[] sync = parseHexString(syncHex);
                int off = d.getSyncOffset();
                if ((d.getSyncHex() == null || d.getSyncHex().isEmpty()) && templates != null) {
                    HexFrameTemplate t = HexProtocolExpander.findTemplate(templates, d.getTemplateId());
                    if (t != null) {
                        off = t.getSyncOffset();
                    }
                }
                if (off + sync.length > buf.length) {
                    return false;
                }
                for (int i = 0; i < sync.length; i++) {
                    if (buf[off + i] != sync[i]) {
                        return false;
                    }
                }
                return commandMatches(d, buf, templates);
            } catch (Exception e) {
                return false;
            }
        }
        if (d.getMinBytes() > 0 && buf.length < d.getMinBytes()) {
            return false;
        }
        return commandMatchesHeadless(d, buf, templates);
    }

    /**
     * For frames without sync: require explicit command + value (protocol or template commandMatchOffset).
     */
    private static boolean commandMatchesHeadless(HexProtocolDefinition d, byte[] buf, List<HexFrameTemplate> templates) {
        if (d.getCommandValue() == null) {
            // Same layout for every command (e.g. monitoring UDP datagram)
            return true;
        }
        Integer cOff = resolveCommandOffset(d, templates);
        if (cOff == null) {
            return false;
        }
        return compareCommandAt(buf, cOff, d.getCommandMatchWidth(), d.getCommandValue());
    }

    private static Integer resolveCommandOffset(HexProtocolDefinition d, List<HexFrameTemplate> templates) {
        Integer cOff = d.getCommandByteOffset();
        if (cOff == null && templates != null && d.getTemplateId() != null) {
            HexFrameTemplate t = HexProtocolExpander.findTemplate(templates, d.getTemplateId());
            if (t != null && t.getCommandMatchOffset() != null) {
                cOff = t.getCommandMatchOffset();
            }
        }
        return cOff;
    }

    private static boolean compareCommandAt(byte[] buf, int cOff, Integer matchWidth, int expected) {
        int o = resolveIndex(buf, cOff);
        boolean wide = matchWidth != null && matchWidth == 4;
        if (wide) {
            if (o < 0 || o + 4 > buf.length) {
                return false;
            }
            return readU32LE(buf, o) == expected;
        }
        if (o < 0 || o >= buf.length) {
            return false;
        }
        return (buf[o] & 0xFF) == (expected & 0xFF);
    }

    private static String effectiveSyncHex(HexProtocolDefinition d, List<HexFrameTemplate> templates) {
        if (d.getSyncHex() != null && !d.getSyncHex().isEmpty()) {
            return d.getSyncHex();
        }
        if (templates != null && d.getTemplateId() != null) {
            HexFrameTemplate t = HexProtocolExpander.findTemplate(templates, d.getTemplateId());
            if (t != null && t.getSyncHex() != null && !t.getSyncHex().isEmpty()) {
                return t.getSyncHex();
            }
        }
        return null;
    }

    /**
     * When {@link HexProtocolDefinition#getCommandValue()} is set, compares buf at command index
     * (1 byte or uint32 LE per {@link HexProtocolDefinition#getCommandMatchWidth()}).
     */
    static boolean commandMatches(HexProtocolDefinition d, byte[] buf, List<HexFrameTemplate> templates) {
        Integer cOff = resolveCommandOffset(d, templates);
        Integer cVal = d.getCommandValue();
        if (cOff == null || cVal == null) {
            return true;
        }
        return compareCommandAt(buf, cOff, d.getCommandMatchWidth(), cVal);
    }

}

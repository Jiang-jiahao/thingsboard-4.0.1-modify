/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package org.thingsboard.server.transport.tcp.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.device.profile.TcpHexChecksumDefinition;
import org.thingsboard.server.common.data.device.profile.TcpHexCommandProfile;
import org.thingsboard.server.common.data.device.profile.TcpHexFieldDefinition;
import org.thingsboard.server.common.data.device.profile.TcpHexLtvChunkOrder;
import org.thingsboard.server.common.data.device.profile.TcpHexLtvRepeatingConfig;
import org.thingsboard.server.common.data.device.profile.TcpHexLtvTagMapping;
import org.thingsboard.server.common.data.device.profile.TcpHexUnknownTagMode;
import org.thingsboard.server.common.data.device.profile.TcpHexValueType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 根据设备配置文件将 {@code {"hex":"..."}} 解析为扁平 JSON 遥测；支持命令匹配、LTV/TLV 重复段。
 */
@Slf4j
public final class TcpHexProtocolParser {

    private TcpHexProtocolParser() {
    }

    /**
     * 先匹配命令规则（含固定字段 + 可选 LTV），再否则使用默认字段 + 可选默认 LTV。
     */
    public static Optional<JsonObject> tryParseTelemetryFromHexPayload(JsonElement payload,
                                                                       List<TcpHexCommandProfile> commandProfiles,
                                                                       List<TcpHexFieldDefinition> defaultFields,
                                                                       TcpHexLtvRepeatingConfig defaultLtvRepeating,
                                                                       Boolean validateTotalLengthU32Le,
                                                                       TcpHexChecksumDefinition checksum,
                                                                       UUID sessionId) {
        byte[] frame = extractFrameBytes(payload);
        if (frame == null) {
            return Optional.empty();
        }
        if (Boolean.TRUE.equals(validateTotalLengthU32Le) && frame.length >= 4) {
            ByteBuffer bb = ByteBuffer.wrap(frame, 0, 4).order(ByteOrder.LITTLE_ENDIAN);
            long declared = Integer.toUnsignedLong(bb.getInt());
            if (declared != frame.length) {
                log.debug("[{}] Hex frame length mismatch: declared {} actual {}", sessionId, declared, frame.length);
                return Optional.empty();
            }
        }
        try {
            validateFrameChecksum(checksum, frame);
        } catch (IllegalArgumentException e) {
            log.debug("[{}] Hex frame checksum: {}", sessionId, e.getMessage());
            return Optional.empty();
        }
        if (commandProfiles != null) {
            for (TcpHexCommandProfile rule : commandProfiles) {
                if (rule == null) {
                    continue;
                }
                try {
                    rule.validate();
                } catch (IllegalArgumentException e) {
                    log.warn("[{}] Invalid hex command profile: {}", sessionId, e.getMessage());
                    continue;
                }
                if (matchesCommand(frame, rule, sessionId)) {
                    String tag = rule.getName();
                    JsonObject out = buildTelemetryForRule(frame, rule.getFields(), rule.getLtvRepeating(),
                            tag != null && !tag.isBlank() ? tag : null, sessionId);
                    if (out.size() > 0) {
                        return Optional.of(out);
                    }
                }
            }
        }
        JsonObject out = buildTelemetryForRule(frame, defaultFields, defaultLtvRepeating, null, sessionId);
        if (out.size() > 0) {
            return Optional.of(out);
        }
        return Optional.empty();
    }

    private static JsonObject buildTelemetryForRule(byte[] frame, List<TcpHexFieldDefinition> fields,
                                                    TcpHexLtvRepeatingConfig ltv, String profileNameForTag, UUID sessionId) {
        JsonObject out = new JsonObject();
        if (profileNameForTag != null) {
            out.addProperty("hexCmdProfile", profileNameForTag);
        }
        if (fields != null) {
            for (TcpHexFieldDefinition def : fields) {
                if (def == null) {
                    continue;
                }
                try {
                    def.validate();
                } catch (IllegalArgumentException e) {
                    log.warn("[{}] Invalid hex protocol field: {}", sessionId, e.getMessage());
                    continue;
                }
                try {
                    appendField(out, frame, def);
                } catch (Exception e) {
                    log.warn("[{}] Skip hex field [{}]: {}", sessionId, def.getKey(), e.getMessage());
                }
            }
        }
        if (ltv != null) {
            try {
                ltv.validate();
                appendLtvRepeating(out, frame, ltv, sessionId);
            } catch (Exception e) {
                log.warn("[{}] LTV section failed: {}", sessionId, e.getMessage());
            }
        }
        return out;
    }

    private static void appendLtvRepeating(JsonObject out, byte[] frame, TcpHexLtvRepeatingConfig cfg, UUID sessionId) {
        int pos = cfg.getStartByteOffset();
        int lenW = integralTypeWidth(cfg.getLengthFieldType());
        int tagW = integralTypeWidth(cfg.getTagFieldType());
        int maxItems = cfg.getEffectiveMaxItems();
        TcpHexLtvChunkOrder order = cfg.getChunkOrder();
        String prefix = cfg.getEffectiveKeyPrefix();
        for (int item = 0; item < maxItems && pos < frame.length; item++) {
            long lenVal;
            long tagVal;
            if (order == TcpHexLtvChunkOrder.LTV) {
                if (pos + lenW > frame.length) {
                    break;
                }
                lenVal = readIntegralAt(frame, pos, cfg.getLengthFieldType());
                pos += lenW;
                if (pos + tagW > frame.length) {
                    break;
                }
                tagVal = readIntegralAt(frame, pos, cfg.getTagFieldType());
                pos += tagW;
            } else {
                if (pos + tagW > frame.length) {
                    break;
                }
                tagVal = readIntegralAt(frame, pos, cfg.getTagFieldType());
                pos += tagW;
                if (pos + lenW > frame.length) {
                    break;
                }
                lenVal = readIntegralAt(frame, pos, cfg.getLengthFieldType());
                pos += lenW;
            }
            int vLen = (int) lenVal;
            if (vLen < 0 || pos + vLen > frame.length) {
                log.warn("[{}] LTV invalid value length {} at offset {}", sessionId, vLen, pos);
                break;
            }
            byte[] v = Arrays.copyOfRange(frame, pos, pos + vLen);
            pos += vLen;
            emitLtvItem(out, cfg, tagVal, v, item, prefix, sessionId);
        }
    }

    private static void emitLtvItem(JsonObject out, TcpHexLtvRepeatingConfig cfg, long tagVal, byte[] v,
                                    int item, String prefix, UUID sessionId) {
        TcpHexLtvTagMapping mapping = findTagMapping(cfg.getTagMappings(), tagVal);
        String fullKey = prefix + "_" + item + "_";
        if (mapping != null) {
            try {
                mapping.validate();
            } catch (IllegalArgumentException e) {
                log.warn("[{}] Invalid LTV mapping: {}", sessionId, e.getMessage());
                return;
            }
            fullKey += mapping.getTelemetryKey();
            TcpHexFieldDefinition synthetic = ltvMappingToField(mapping, fullKey);
            try {
                appendField(out, v, synthetic);
            } catch (Exception e) {
                log.warn("[{}] LTV value decode failed for tag {}: {}", sessionId, tagVal, e.getMessage());
            }
        } else if (cfg.getUnknownTagMode() == TcpHexUnknownTagMode.EMIT_HEX) {
            out.addProperty(prefix + "_unk_" + item + "_t" + tagVal, HexFormat.of().formatHex(v));
        }
    }

    private static TcpHexLtvTagMapping findTagMapping(List<TcpHexLtvTagMapping> list, long tagVal) {
        if (list == null) {
            return null;
        }
        for (TcpHexLtvTagMapping m : list) {
            if (m != null && m.getTagValue() == tagVal) {
                return m;
            }
        }
        return null;
    }

    private static TcpHexFieldDefinition ltvMappingToField(TcpHexLtvTagMapping m, String key) {
        TcpHexFieldDefinition d = new TcpHexFieldDefinition();
        d.setKey(key);
        d.setByteOffset(0);
        d.setValueType(m.getValueType());
        d.setByteLength(m.getByteLength());
        d.setScale(m.getScale());
        d.setBitMask(m.getBitMask());
        return d;
    }

    private static int integralTypeWidth(TcpHexValueType vt) {
        return switch (vt) {
            case UINT8, INT8 -> 1;
            case UINT16_BE, UINT16_LE, INT16_BE, INT16_LE -> 2;
            case UINT32_BE, UINT32_LE, INT32_BE, INT32_LE -> 4;
            default -> throw new IllegalArgumentException("not an integral width: " + vt);
        };
    }

    private static boolean matchesCommand(byte[] frame, TcpHexCommandProfile rule, UUID sessionId) {
        try {
            long actual = readIntegralAt(frame, rule.getMatchByteOffset(), rule.getMatchValueType());
            if (actual != rule.getMatchValue()) {
                return false;
            }
            if (rule.getSecondaryMatchByteOffset() != null) {
                TcpHexValueType st = rule.getSecondaryMatchValueType() != null
                        ? rule.getSecondaryMatchValueType()
                        : TcpHexValueType.UINT8;
                long secActual = readIntegralAt(frame, rule.getSecondaryMatchByteOffset(), st);
                long secExp = rule.getSecondaryMatchValue();
                boolean secOk = secActual == secExp;
                if (log.isTraceEnabled()) {
                    log.trace("[{}] Hex cmd secondary offset={} type={} actual={} expected={} -> {}",
                            sessionId, rule.getSecondaryMatchByteOffset(), st, secActual, secExp, secOk);
                }
                return secOk;
            }
            if (log.isTraceEnabled()) {
                log.trace("[{}] Hex cmd match offset={} type={} actual={} expected={} -> true",
                        sessionId, rule.getMatchByteOffset(), rule.getMatchValueType(), actual, rule.getMatchValue());
            }
            return true;
        } catch (Exception e) {
            log.trace("[{}] Command match skipped: {}", sessionId, e.getMessage());
            return false;
        }
    }

    /**
     * 与 {@link #appendField} 一致的整型读取，用于命令匹配与 LTV 长度/Tag。
     */
    static long readIntegralAt(byte[] frame, int offset, TcpHexValueType vt) {
        int len = switch (vt) {
            case UINT8, INT8 -> 1;
            case UINT16_BE, UINT16_LE, INT16_BE, INT16_LE -> 2;
            case UINT32_BE, UINT32_LE, INT32_BE, INT32_LE -> 4;
            default -> throw new IllegalArgumentException("not an integral type: " + vt);
        };
        if (offset < 0 || offset + len > frame.length) {
            throw new IllegalArgumentException("integral read out of bounds: offset=" + offset + " len=" + len + " frame=" + frame.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(frame, offset, len);
        return switch (vt) {
            case UINT8 -> Byte.toUnsignedInt(buf.get());
            case INT8 -> buf.get();
            case UINT16_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                yield Short.toUnsignedInt(buf.getShort());
            }
            case UINT16_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                yield Short.toUnsignedInt(buf.getShort());
            }
            case INT16_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                yield buf.getShort();
            }
            case INT16_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                yield buf.getShort();
            }
            case UINT32_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                yield Integer.toUnsignedLong(buf.getInt());
            }
            case UINT32_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                yield Integer.toUnsignedLong(buf.getInt());
            }
            case INT32_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                yield buf.getInt();
            }
            case INT32_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                yield buf.getInt();
            }
            default -> throw new IllegalArgumentException("not an integral type: " + vt);
        };
    }

    private static byte[] extractFrameBytes(JsonElement payload) {
        if (payload == null || payload.isJsonNull()) {
            return null;
        }
        if (payload.isJsonObject()) {
            JsonObject o = payload.getAsJsonObject();
            if (!o.has(TcpPayloadUtil.TCP_HEX_FRAME_JSON_KEY)) {
                return null;
            }
            JsonElement hexEl = o.get(TcpPayloadUtil.TCP_HEX_FRAME_JSON_KEY);
            if (hexEl == null || !hexEl.isJsonPrimitive()) {
                return null;
            }
            return parseHexString(hexEl.getAsString());
        }
        if (payload.isJsonPrimitive() && payload.getAsJsonPrimitive().isString()) {
            return parseHexString(payload.getAsString());
        }
        return null;
    }

    private static byte[] parseHexString(String hex) {
        if (hex == null) {
            return null;
        }
        String clean = hex.replaceAll("\\s+", "");
        if (clean.isEmpty()) {
            return new byte[0];
        }
        if ((clean.length() & 1) == 1) {
            return null;
        }
        try {
            return HexFormat.of().parseHex(clean);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static void appendField(JsonObject out, byte[] frame, TcpHexFieldDefinition def) {
        int len = resolveFieldByteLength(frame, def);
        if (def.getByteOffset() + len > frame.length) {
            throw new IllegalArgumentException("field out of bounds: offset=" + def.getByteOffset() + " len=" + len + " frame=" + frame.length);
        }
        validateFixedFieldWireValue(frame, def, len);
        ByteBuffer buf = ByteBuffer.wrap(frame, def.getByteOffset(), len);
        TcpHexValueType vt = def.getValueType();
        double scale = def.getEffectiveScale();

        switch (vt) {
            case UINT8 -> {
                long v = Byte.toUnsignedInt(buf.get());
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case INT8 -> {
                long v = buf.get();
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case UINT16_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                long v = Short.toUnsignedInt(buf.getShort());
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case UINT16_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                long v = Short.toUnsignedInt(buf.getShort());
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case INT16_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                long v = buf.getShort();
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case INT16_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                long v = buf.getShort();
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case UINT32_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                long v = Integer.toUnsignedLong(buf.getInt());
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case UINT32_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                long v = Integer.toUnsignedLong(buf.getInt());
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case INT32_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                long v = buf.getInt();
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case INT32_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                long v = buf.getInt();
                addScaledIntegral(out, def.getKey(), applyMask(v, def), scale);
            }
            case FLOAT_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                out.addProperty(def.getKey(), buf.getFloat() * scale);
            }
            case FLOAT_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                out.addProperty(def.getKey(), buf.getFloat() * scale);
            }
            case DOUBLE_BE -> {
                buf.order(ByteOrder.BIG_ENDIAN);
                out.addProperty(def.getKey(), buf.getDouble() * scale);
            }
            case DOUBLE_LE -> {
                buf.order(ByteOrder.LITTLE_ENDIAN);
                out.addProperty(def.getKey(), buf.getDouble() * scale);
            }
            case BYTES_AS_HEX -> {
                byte[] slice = new byte[len];
                buf.get(slice);
                out.addProperty(def.getKey(), HexFormat.of().formatHex(slice));
            }
            default -> throw new IllegalArgumentException("unsupported value type: " + vt);
        }
    }

    /**
     * 配置了 {@link TcpHexFieldDefinition#getFixedWireIntegralValue()} 或 {@link TcpHexFieldDefinition#getFixedBytesHex()} 时，
     * 校验帧内实际字节与固定值一致（与下行组帧写入语义对称）。
     */
    private static void validateFixedFieldWireValue(byte[] frame, TcpHexFieldDefinition def, int resolvedLen) {
        if (def.getFixedWireIntegralValue() != null) {
            TcpHexValueType vt = def.getValueType();
            long actual = readIntegralAt(frame, def.getByteOffset(), vt);
            byte[] tmp = new byte[8];
            writeIntegralAt(tmp, 0, vt, def.getFixedWireIntegralValue());
            long expected = readIntegralAt(tmp, 0, vt);
            if (actual != expected) {
                throw new IllegalArgumentException(
                        "fixed integral mismatch for [" + def.getKey() + "]: wire " + actual + " expected " + expected);
            }
        }
        if (def.getFixedBytesHex() != null && !def.getFixedBytesHex().isBlank()) {
            byte[] expected = parseHexString(def.getFixedBytesHex());
            if (expected == null || expected.length != resolvedLen) {
                throw new IllegalArgumentException(
                        "fixedBytesHex length mismatch for [" + def.getKey() + "]: need " + resolvedLen + " bytes");
            }
            for (int i = 0; i < resolvedLen; i++) {
                if (frame[def.getByteOffset() + i] != expected[i]) {
                    throw new IllegalArgumentException("fixed bytes mismatch for [" + def.getKey() + "]");
                }
            }
        }
    }

    /**
     * BYTES_AS_HEX：支持固定 {@link TcpHexFieldDefinition#getByteLength()} 或从帧内另一偏移读取长度。
     */
    static int resolveFieldByteLength(byte[] frame, TcpHexFieldDefinition def) {
        TcpHexValueType vt = def.getValueType();
        if (!vt.isBytesAsHex()) {
            return vt.getFixedByteLength();
        }
        if (def.getByteLength() != null && def.getByteLength() > 0) {
            return def.getByteLength();
        }
        if (def.getByteLengthFromByteOffset() == null) {
            throw new IllegalArgumentException("BYTES_AS_HEX requires byteLength or byteLengthFromByteOffset");
        }
        TcpHexValueType lenVt = def.getByteLengthFromValueType() != null ? def.getByteLengthFromValueType() : TcpHexValueType.UINT8;
        long lenLong = readIntegralAt(frame, def.getByteLengthFromByteOffset(), lenVt);
        if (lenLong < 0 || lenLong > frame.length) {
            throw new IllegalArgumentException("invalid dynamic byte length: " + lenLong);
        }
        return (int) lenLong;
    }

    private static long applyMask(long v, TcpHexFieldDefinition def) {
        Long m = def.getBitMask();
        if (m == null) {
            return v;
        }
        return v & m;
    }

    private static void addScaledIntegral(JsonObject out, String key, long raw, double scale) {
        double scaled = raw * scale;
        if (Double.isNaN(scaled) || Double.isInfinite(scaled)) {
            out.addProperty(key, 0);
            return;
        }
        double r = Math.rint(scaled);
        if (Math.abs(scaled - r) < 1e-9) {
            long lv = (long) r;
            if (lv >= Integer.MIN_VALUE && lv <= Integer.MAX_VALUE) {
                out.addProperty(key, (int) lv);
            } else {
                out.addProperty(key, lv);
            }
        } else {
            out.addProperty(key, scaled);
        }
    }

    private static int resolveIndex(byte[] buf, int idx) {
        if (idx >= 0) {
            return idx;
        }
        return buf.length + idx;
    }

    /**
     * 与规则引擎 {@code HexProtocolParser#validateChecksum} 行为一致。
     */
    private static void validateFrameChecksum(TcpHexChecksumDefinition cs, byte[] buf) {
        if (cs == null || cs.getType() == null || "NONE".equalsIgnoreCase(cs.getType().trim())) {
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
            case "SUM8": {
                int sum = 0;
                for (int i = from; i < toEx; i++) {
                    sum = (sum + (buf[i] & 0xFF)) & 0xFF;
                }
                if (sum != (buf[cksAt] & 0xFF)) {
                    throw new IllegalArgumentException("SUM8 mismatch");
                }
                break;
            }
            case "CRC16_MODBUS":
                if (cksAt + 1 >= buf.length) {
                    throw new IllegalArgumentException("CRC16_MODBUS: need 2 bytes at checksum index");
                }
                int crcM = crc16Modbus(buf, from, toEx);
                int lo = buf[cksAt] & 0xFF;
                int hi = buf[cksAt + 1] & 0xFF;
                int actualM = lo | (hi << 8);
                if (crcM != actualM) {
                    throw new IllegalArgumentException("CRC16_MODBUS mismatch expected " + crcM + " got " + actualM);
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
                long crc32 = crc32Ieee(buf, from, toEx);
                long a = readU32Le(buf, cksAt) & 0xFFFFFFFFL;
                if ((crc32 & 0xFFFFFFFFL) != a) {
                    throw new IllegalArgumentException("CRC32 mismatch");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown checksum type: " + cs.getType());
        }
    }

    private static int readU32Le(byte[] b, int i) {
        return (b[i] & 0xFF) | ((b[i + 1] & 0xFF) << 8) | ((b[i + 2] & 0xFF) << 16) | ((b[i + 3] & 0xFF) << 24);
    }

    private static long crc32Ieee(byte[] data, int from, int to) {
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

    private static int crc16Modbus(byte[] buf, int from, int to) {
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

    private static int crc16Ccitt(byte[] buf, int from, int to) {
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

    /**
     * 将整型按类型宽度写入帧（与 {@link #readIntegralAt} 对称）；用于下行组帧等场景。
     */
    public static void writeIntegralAt(byte[] buf, int offset, TcpHexValueType vt, long value) {
        int len = switch (vt) {
            case UINT8, INT8 -> 1;
            case UINT16_BE, UINT16_LE, INT16_BE, INT16_LE -> 2;
            case UINT32_BE, UINT32_LE, INT32_BE, INT32_LE -> 4;
            default -> throw new IllegalArgumentException("not an integral type: " + vt);
        };
        if (offset < 0 || offset + len > buf.length) {
            throw new IllegalArgumentException("integral write out of bounds: offset=" + offset + " len=" + len + " buf=" + buf.length);
        }
        ByteBuffer b = ByteBuffer.wrap(buf, offset, len);
        switch (vt) {
            case UINT8 -> b.put((byte) (value & 0xFF));
            case INT8 -> b.put((byte) value);
            case UINT16_BE -> {
                b.order(ByteOrder.BIG_ENDIAN);
                b.putShort((short) (value & 0xFFFF));
            }
            case UINT16_LE -> {
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putShort((short) (value & 0xFFFF));
            }
            case INT16_BE -> {
                b.order(ByteOrder.BIG_ENDIAN);
                b.putShort((short) value);
            }
            case INT16_LE -> {
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putShort((short) value);
            }
            case UINT32_BE -> {
                b.order(ByteOrder.BIG_ENDIAN);
                b.putInt((int) (value & 0xFFFFFFFFL));
            }
            case UINT32_LE -> {
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putInt((int) (value & 0xFFFFFFFFL));
            }
            case INT32_BE -> {
                b.order(ByteOrder.BIG_ENDIAN);
                b.putInt((int) value);
            }
            case INT32_LE -> {
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putInt((int) value);
            }
            default -> throw new IllegalArgumentException("not an integral type: " + vt);
        }
    }

    public static void writeFloatAt(byte[] buf, int offset, TcpHexValueType vt, float value) {
        if (offset < 0 || offset + 4 > buf.length) {
            throw new IllegalArgumentException("float write out of bounds");
        }
        ByteBuffer b = ByteBuffer.wrap(buf, offset, 4);
        switch (vt) {
            case FLOAT_BE -> b.order(ByteOrder.BIG_ENDIAN);
            case FLOAT_LE -> b.order(ByteOrder.LITTLE_ENDIAN);
            default -> throw new IllegalArgumentException("not a float type: " + vt);
        }
        b.putFloat(value);
    }

    public static void writeDoubleAt(byte[] buf, int offset, TcpHexValueType vt, double value) {
        if (offset < 0 || offset + 8 > buf.length) {
            throw new IllegalArgumentException("double write out of bounds");
        }
        ByteBuffer b = ByteBuffer.wrap(buf, offset, 8);
        switch (vt) {
            case DOUBLE_BE -> b.order(ByteOrder.BIG_ENDIAN);
            case DOUBLE_LE -> b.order(ByteOrder.LITTLE_ENDIAN);
            default -> throw new IllegalArgumentException("not a double type: " + vt);
        }
        b.putDouble(value);
    }

    /**
     * 按定义写入校验字节（与 {@link #validateFrameChecksum} 算法对称）。
     */
    public static void applyChecksumToFrame(TcpHexChecksumDefinition cs, byte[] buf) {
        if (cs == null || cs.getType() == null || "NONE".equalsIgnoreCase(cs.getType().trim())) {
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
            case "SUM8": {
                int sum = 0;
                for (int i = from; i < toEx; i++) {
                    sum = (sum + (buf[i] & 0xFF)) & 0xFF;
                }
                buf[cksAt] = (byte) sum;
                break;
            }
            case "CRC16_MODBUS":
                if (cksAt + 1 >= buf.length) {
                    throw new IllegalArgumentException("CRC16_MODBUS: need 2 bytes at checksum index");
                }
                int crcM = crc16Modbus(buf, from, toEx);
                buf[cksAt] = (byte) (crcM & 0xFF);
                buf[cksAt + 1] = (byte) ((crcM >> 8) & 0xFF);
                break;
            case "CRC16_CCITT":
                if (cksAt + 1 >= buf.length) {
                    throw new IllegalArgumentException("CRC16_CCITT: need 2 bytes at checksum index");
                }
                int cc = crc16Ccitt(buf, from, toEx);
                buf[cksAt] = (byte) ((cc >> 8) & 0xFF);
                buf[cksAt + 1] = (byte) (cc & 0xFF);
                break;
            case "CRC32":
                if (cksAt + 3 >= buf.length) {
                    throw new IllegalArgumentException("CRC32: need 4 bytes at checksum index");
                }
                long crc32 = crc32Ieee(buf, from, toEx) & 0xFFFFFFFFL;
                buf[cksAt] = (byte) (crc32 & 0xFF);
                buf[cksAt + 1] = (byte) ((crc32 >> 8) & 0xFF);
                buf[cksAt + 2] = (byte) ((crc32 >> 16) & 0xFF);
                buf[cksAt + 3] = (byte) ((crc32 >> 24) & 0xFF);
                break;
            default:
                throw new IllegalArgumentException("Unknown checksum type: " + cs.getType());
        }
    }
}

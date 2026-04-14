/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.service.protocoltemplate;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateBundle;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateCommandDefinition;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateCommandDirection;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateDefinition;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration;
import org.thingsboard.server.common.data.device.profile.TcpHexChecksumDefinition;
import org.thingsboard.server.common.data.device.profile.TcpHexCommandProfile;
import org.thingsboard.server.common.data.device.profile.TcpHexFieldDefinition;
import org.thingsboard.server.common.data.device.profile.TcpHexValueType;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.transport.tcp.util.TcpHexProtocolParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 按首帧模板 + 下行/双向命令合并字段组帧，并写入命令字与校验和。
 */
@Service
@RequiredArgsConstructor
public class ProtocolTemplateHexBuildService {

    private static final Pattern PA_WORD_FIELD_KEY = Pattern.compile("^pa(\\d+)_word$");

    private final DefaultProtocolTemplateBundleService bundleService;

    @Transactional(readOnly = true)
    public ProtocolTemplateHexBuildResult build(TenantId tenantId, ProtocolTemplateHexBuildRequest request) {
        ProtocolTemplateHexBuildResult out = new ProtocolTemplateHexBuildResult();
        if (request == null) {
            out.setErrorMessage("request is required");
            return out;
        }
        if (request.getBundleId() == null || request.getBundleId().isBlank()) {
            out.setErrorMessage("bundleId is required");
            return out;
        }
        if (request.getCommandValue() == null) {
            out.setErrorMessage("commandValue is required");
            return out;
        }
        UUID bundleId;
        try {
            bundleId = UUID.fromString(request.getBundleId().trim());
        } catch (IllegalArgumentException e) {
            out.setErrorMessage("Invalid bundleId (must be UUID)");
            return out;
        }

        Optional<ProtocolTemplateBundle> bundleOpt = bundleService.findById(tenantId, bundleId);
        if (bundleOpt.isEmpty()) {
            out.setErrorMessage("Protocol template bundle not found");
            return out;
        }
        ProtocolTemplateBundle bundle = bundleOpt.get();
        List<ProtocolTemplateCommandDefinition> cmds = bundle.getProtocolCommands() != null
                ? bundle.getProtocolCommands()
                : List.of();
        long cv = request.getCommandValue();
        String tidFilter = request.getTemplateId() != null ? request.getTemplateId().trim() : null;
        List<ProtocolTemplateCommandDefinition> matches = new ArrayList<>();
        for (ProtocolTemplateCommandDefinition c : cmds) {
            if (c == null) {
                continue;
            }
            if (c.getDirection() != ProtocolTemplateCommandDirection.DOWNLINK
                    && c.getDirection() != ProtocolTemplateCommandDirection.BOTH) {
                continue;
            }
            if (c.getCommandValue() != cv) {
                continue;
            }
            if (tidFilter != null && !tidFilter.equals(c.getTemplateId())) {
                continue;
            }
            matches.add(c);
        }
        if (matches.isEmpty()) {
            out.setErrorMessage("No DOWNLINK/BOTH command with this commandValue in the bundle");
            return out;
        }
        if (matches.size() > 1 && (tidFilter == null || tidFilter.isEmpty())) {
            out.setErrorMessage("Ambiguous commandValue: set templateId to pick one of: "
                    + matches.stream().map(ProtocolTemplateCommandDefinition::getTemplateId).distinct().toList());
            return out;
        }
        ProtocolTemplateCommandDefinition cmd = matches.get(0);

        ProtocolTemplateTransportTcpDataConfiguration pcfg = new ProtocolTemplateTransportTcpDataConfiguration();
        pcfg.setProtocolTemplates(bundle.getProtocolTemplates());
        pcfg.setProtocolCommands(cmds);
        ProtocolTemplateDefinition tpl = pcfg.findTemplate(cmd.getTemplateId());
        if (tpl == null) {
            out.setErrorMessage("Unknown templateId on command: " + cmd.getTemplateId());
            return out;
        }

        List<TcpHexFieldDefinition> merged;
        try {
            merged = ProtocolTemplateTransportTcpDataConfiguration.mergeTemplateAndCommandFields(
                    tpl.getHexProtocolFields(), cmd.getFields());
        } catch (RuntimeException e) {
            out.setErrorMessage(e.getMessage());
            return out;
        }
        if (merged == null || merged.isEmpty()) {
            out.setErrorMessage("No merged fields to build (template and command fields are empty)");
            return out;
        }

        TcpHexValueType matchVt = cmd.getMatchValueType() != null ? cmd.getMatchValueType() : TcpHexValueType.UINT32_LE;
        if (!TcpHexCommandProfile.isIntegralMatchType(matchVt)) {
            out.setErrorMessage("command matchValueType must be integral for HEX build");
            return out;
        }
        int cmdOff = tpl.getCommandByteOffset() != null ? tpl.getCommandByteOffset() : 12;
        int cmdW = integralTypeWidth(matchVt);

        TcpHexChecksumDefinition checksum = tpl.getChecksum();

        int frameLen;
        try {
            frameLen = computeFrameLength(merged, checksum, cmdOff, matchVt);
        } catch (IllegalArgumentException e) {
            out.setErrorMessage(e.getMessage());
            return out;
        }

        byte[] buf = new byte[frameLen];
        Map<String, Object> values = new HashMap<>();
        if (request.getValues() != null) {
            values.putAll(request.getValues());
        }
        expandPaWordHiLoAliases(values);

        boolean cmdLenAuto = commandLevelDownlinkPayloadLengthAuto(cmd);
        try {
            for (TcpHexFieldDefinition f : merged) {
                if (f == null) {
                    continue;
                }
                f.validate();
                if (cmdLenAuto && cmd.getDownlinkPayloadLengthFieldKey() != null
                        && cmd.getDownlinkPayloadLengthFieldKey().trim().equals(f.getKey())) {
                    continue;
                }
                if (writesAutoDownlinkPayloadLength(f)) {
                    continue;
                }
                if (fieldOverlapsCommandSpan(f, cmdOff, cmdW)) {
                    continue;
                }
                if (isRedundantPaWordField(f, merged)) {
                    continue;
                }
                writeFieldFromValues(buf, f, values);
            }
            if (cmd.getDirection() != ProtocolTemplateCommandDirection.DOWNLINK
                    && cmd.getSecondaryMatchByteOffset() != null) {
                TcpHexValueType st = cmd.getSecondaryMatchValueType() != null
                        ? cmd.getSecondaryMatchValueType()
                        : TcpHexValueType.UINT8;
                if (!TcpHexCommandProfile.isIntegralMatchType(st)) {
                    throw new IllegalArgumentException("secondaryMatchValueType must be integral");
                }
                int secOff = cmd.getSecondaryMatchByteOffset();
                Long secDef = cmd.getSecondaryMatchValue();
                long sec = secDef != null ? secDef : 0L;
                for (TcpHexFieldDefinition f : merged) {
                    if (f != null && f.getByteOffset() == secOff) {
                        Object v = values.get(f.getKey());
                        if (v != null) {
                            sec = toLongForIntegral(v, f.getKey());
                        }
                        break;
                    }
                }
                TcpHexProtocolParser.writeIntegralAt(buf, secOff, st, sec);
            }
            TcpHexProtocolParser.writeIntegralAt(buf, cmdOff, matchVt, cmd.getCommandValue());
            writeAutoDownlinkPayloadLengthFields(buf, merged, cmd, cmdOff, cmdW);
            TcpHexProtocolParser.applyChecksumToFrame(checksum, buf);
        } catch (IllegalArgumentException | ArithmeticException e) {
            out.setErrorMessage(e.getMessage());
            return out;
        }

        out.setSuccess(true);
        out.setHex(HexFormat.of().formatHex(buf));
        return out;
    }

    private static int computeFrameLength(List<TcpHexFieldDefinition> merged, TcpHexChecksumDefinition cs,
                                          int cmdOff, TcpHexValueType cmdVt) {
        int maxEnd = cmdOff + integralTypeWidth(cmdVt);
        for (TcpHexFieldDefinition f : merged) {
            if (f == null) {
                continue;
            }
            int w = fieldWidthForBuild(f);
            maxEnd = Math.max(maxEnd, f.getByteOffset() + w);
        }
        int cksN = checksumAlgBytes(cs);
        if (cksN == 0) {
            return maxEnd;
        }
        int cksIdx = cs.getChecksumByteIndex();
        if (cksIdx < 0) {
            return maxEnd + cksN;
        }
        return Math.max(maxEnd, cksIdx + cksN);
    }

    private static int checksumAlgBytes(TcpHexChecksumDefinition cs) {
        if (cs == null || cs.getType() == null || "NONE".equalsIgnoreCase(cs.getType().trim())) {
            return 0;
        }
        return switch (cs.getType().toUpperCase()) {
            case "SUM8" -> 1;
            case "CRC16_MODBUS", "CRC16_CCITT" -> 2;
            case "CRC32" -> 4;
            default -> throw new IllegalArgumentException("Unknown checksum type: " + cs.getType());
        };
    }

    private static int integralTypeWidth(TcpHexValueType vt) {
        return switch (vt) {
            case UINT8, INT8 -> 1;
            case UINT16_BE, UINT16_LE, INT16_BE, INT16_LE -> 2;
            case UINT32_BE, UINT32_LE, INT32_BE, INT32_LE -> 4;
            default -> throw new IllegalArgumentException("not an integral width: " + vt);
        };
    }

    private static boolean fieldOverlapsCommandSpan(TcpHexFieldDefinition f, int cmdOff, int cmdW) {
        int fw = fieldWidthForBuild(f);
        return spansOverlap(f.getByteOffset(), fw, cmdOff, cmdW);
    }

    /** 与 {@link ProtocolTemplateTransportTcpDataConfiguration#hexFieldSpansOverlap} 区间规则一致。 */
    private static boolean spansOverlap(int a0, int aLen, int b0, int bLen) {
        if (aLen <= 0 || bLen <= 0) {
            return a0 == b0;
        }
        int a1 = a0 + aLen;
        int b1 = b0 + bLen;
        return a0 < b1 && b0 < a1;
    }

    /** 本字段在下行组帧时由系统写入参长（非 JSON）。 */
    private static boolean writesAutoDownlinkPayloadLength(TcpHexFieldDefinition f) {
        if (f == null) {
            return false;
        }
        if (f.hasDownlinkPayloadLengthMemberKeys()) {
            return true;
        }
        return Boolean.TRUE.equals(f.getAutoDownlinkPayloadLength());
    }

    private static boolean commandLevelDownlinkPayloadLengthAuto(ProtocolTemplateCommandDefinition cmd) {
        if (cmd == null || !Boolean.TRUE.equals(cmd.getDownlinkPayloadLengthAuto())) {
            return false;
        }
        if (cmd.getDirection() != ProtocolTemplateCommandDirection.DOWNLINK
                && cmd.getDirection() != ProtocolTemplateCommandDirection.BOTH) {
            return false;
        }
        return cmd.getDownlinkPayloadLengthFieldKey() != null && !cmd.getDownlinkPayloadLengthFieldKey().isBlank();
    }

    private static Set<String> contributorKeysFromCommandFields(ProtocolTemplateCommandDefinition cmd) {
        Set<String> s = new HashSet<>();
        if (cmd.getFields() == null) {
            return s;
        }
        for (TcpHexFieldDefinition f : cmd.getFields()) {
            if (f != null && Boolean.TRUE.equals(f.getIncludeInDownlinkPayloadLength()) && f.getKey() != null) {
                String k = f.getKey().trim();
                if (!k.isEmpty()) {
                    s.add(k);
                }
            }
        }
        return s;
    }

    private static void writeCommandLevelDownlinkPayloadLength(byte[] buf, List<TcpHexFieldDefinition> merged,
                                                                 ProtocolTemplateCommandDefinition cmd) {
        cmd.validate();
        String lenKey = cmd.getDownlinkPayloadLengthFieldKey().trim();
        TcpHexFieldDefinition lengthField = null;
        for (TcpHexFieldDefinition m : merged) {
            if (m != null && lenKey.equals(m.getKey())) {
                lengthField = m;
                break;
            }
        }
        if (lengthField == null) {
            throw new IllegalArgumentException("downlinkPayloadLengthFieldKey: no merged field \"" + lenKey + "\"");
        }
        lengthField.validate();
        if (lengthField.getValueType() == null || lengthField.getValueType().isBytesAsHex()
                || !TcpHexCommandProfile.isIntegralMatchType(lengthField.getValueType())) {
            throw new IllegalArgumentException("Length field [" + lenKey + "] must use an integral valueType");
        }
        Set<String> want = contributorKeysFromCommandFields(cmd);
        for (String wk : want) {
            boolean found = false;
            for (TcpHexFieldDefinition g : merged) {
                if (g != null && wk.equals(g.getKey())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("includeInDownlinkPayloadLength: merged field not found for key \"" + wk + "\"");
            }
        }
        long len = 0;
        for (TcpHexFieldDefinition g : merged) {
            if (g == null || lenKey.equals(g.getKey())) {
                continue;
            }
            if (writesAutoDownlinkPayloadLength(g)) {
                continue;
            }
            if (!want.contains(g.getKey())) {
                continue;
            }
            if (isRedundantPaWordField(g, merged)) {
                continue;
            }
            len += fieldWidthForBuild(g);
        }
        long maxWire = maxUnsignedIntegralForType(lengthField.getValueType());
        if (len < 0 || len > maxWire) {
            throw new IllegalArgumentException(
                    "Field [" + lenKey + "]: auto payload length " + len + " out of range for " + lengthField.getValueType());
        }
        TcpHexProtocolParser.writeIntegralAt(buf, lengthField.getByteOffset(), lengthField.getValueType(), len);
    }

    private static void writeAutoDownlinkPayloadLengthFields(byte[] buf, List<TcpHexFieldDefinition> merged,
                                                             ProtocolTemplateCommandDefinition cmd,
                                                             int cmdOff, int cmdW) {
        if (commandLevelDownlinkPayloadLengthAuto(cmd)) {
            writeCommandLevelDownlinkPayloadLength(buf, merged, cmd);
        }
        for (TcpHexFieldDefinition f : merged) {
            if (f == null || !writesAutoDownlinkPayloadLength(f)) {
                continue;
            }
            if (commandLevelDownlinkPayloadLengthAuto(cmd) && cmd.getDownlinkPayloadLengthFieldKey() != null
                    && cmd.getDownlinkPayloadLengthFieldKey().trim().equals(f.getKey())) {
                continue;
            }
            f.validate();
            if (f.hasDownlinkPayloadLengthMemberKeys()) {
                validateDownlinkPayloadLengthMemberKeysResolve(merged, f);
            }
            int impliedStart = f.getByteOffset() + fieldWidthForBuild(f);
            int pStart = f.getDownlinkPayloadStartByteOffset() != null ? f.getDownlinkPayloadStartByteOffset() : impliedStart;
            if (pStart < 0 || pStart > buf.length) {
                throw new IllegalArgumentException("Field [" + f.getKey() + "]: invalid payload start offset " + pStart);
            }
            Integer pEndEx = f.getDownlinkPayloadEndExclusiveByteOffset();
            long len = computeDownlinkPayloadByteCount(merged, f, pStart, pEndEx, cmdOff, cmdW);
            long maxWire = maxUnsignedIntegralForType(f.getValueType());
            if (len < 0 || len > maxWire) {
                throw new IllegalArgumentException(
                        "Field [" + f.getKey() + "]: auto payload length " + len + " out of range for " + f.getValueType());
            }
            TcpHexProtocolParser.writeIntegralAt(buf, f.getByteOffset(), f.getValueType(), len);
        }
    }

    private static void validateDownlinkPayloadLengthMemberKeysResolve(List<TcpHexFieldDefinition> merged,
                                                                       TcpHexFieldDefinition lengthField) {
        for (String raw : lengthField.getDownlinkPayloadLengthMemberKeys()) {
            if (raw == null || raw.isBlank()) {
                continue;
            }
            String want = raw.trim();
            boolean found = false;
            for (TcpHexFieldDefinition g : merged) {
                if (g != null && want.equals(g.getKey())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException(
                        "Field [" + lengthField.getKey() + "]: downlinkPayloadLengthMemberKeys has no merged field \"" + want + "\"");
            }
        }
    }

    private static long computeDownlinkPayloadByteCount(List<TcpHexFieldDefinition> merged, TcpHexFieldDefinition lengthField,
                                                        int payloadStart, Integer payloadEndExclusive, int cmdOff, int cmdW) {
        if (lengthField.hasDownlinkPayloadLengthMemberKeys()) {
            Set<String> want = new HashSet<>();
            for (String k : lengthField.getDownlinkPayloadLengthMemberKeys()) {
                if (k != null && !k.isBlank()) {
                    want.add(k.trim());
                }
            }
            int maxExclusive = payloadStart;
            for (TcpHexFieldDefinition g : merged) {
                if (g == null || g == lengthField) {
                    continue;
                }
                if (writesAutoDownlinkPayloadLength(g)) {
                    continue;
                }
                if (!want.contains(g.getKey())) {
                    continue;
                }
                int go = g.getByteOffset();
                if (go < payloadStart) {
                    continue;
                }
                maxExclusive = Math.max(maxExclusive, go + fieldWidthForBuild(g));
            }
            return (long) maxExclusive - payloadStart;
        }
        int pEnd = payloadEndExclusive != null ? payloadEndExclusive : Integer.MAX_VALUE;
        int maxExclusive = payloadStart;
        for (TcpHexFieldDefinition g : merged) {
            if (g == null || g == lengthField) {
                continue;
            }
            if (writesAutoDownlinkPayloadLength(g)) {
                continue;
            }
            int go = g.getByteOffset();
            if (go < payloadStart || go >= pEnd) {
                continue;
            }
            int fieldEnd = go + fieldWidthForBuild(g);
            int cappedEnd = Math.min(fieldEnd, pEnd);
            maxExclusive = Math.max(maxExclusive, cappedEnd);
        }
        if (cmdOff >= payloadStart && cmdOff < pEnd) {
            maxExclusive = Math.max(maxExclusive, Math.min(cmdOff + cmdW, pEnd));
        }
        return (long) maxExclusive - payloadStart;
    }

    /** 按线格式写入的无符号上限（参长按字节计数，按无符号写入 wire）。 */
    private static long maxUnsignedIntegralForType(TcpHexValueType vt) {
        return switch (vt) {
            case UINT8, INT8 -> 0xFFL;
            case UINT16_BE, UINT16_LE, INT16_BE, INT16_LE -> 0xFFFFL;
            case UINT32_BE, UINT32_LE, INT32_BE, INT32_LE -> 0xFFFFFFFFL;
            default -> throw new IllegalArgumentException("unsupported integral type for auto payload length: " + vt);
        };
    }

    private static int fieldWidthForBuild(TcpHexFieldDefinition f) {
        TcpHexValueType vt = f.getValueType();
        if (vt.isLtvAutoWidthIntegral()) {
            throw new IllegalArgumentException(
                    "Field [" + f.getKey() + "]: LTV auto integral types are not supported for downlink hex build");
        }
        if (vt.isBytesAsHex()) {
            if (f.getByteLength() != null && f.getByteLength() > 0) {
                return f.getByteLength();
            }
            throw new IllegalArgumentException(
                    "Field [" + f.getKey() + "]: BYTES_AS_HEX for build requires fixed byteLength (dynamic length unsupported)");
        }
        return vt.getFixedByteLength();
    }

    /**
     * 合并列表里若已有 {@code paN_hi} 与 {@code paN_lo}，则 {@code paN_word} 与二者占用同一 2 字节；
     * 再按 word 写字会与 hi/lo 叠加，典型错位为多出一个 {@code 0x00}。此时 word 仅作 JSON 别名（见 {@link #expandPaWordHiLoAliases}），不应再作为独立字段写入。
     */
    static boolean isRedundantPaWordField(TcpHexFieldDefinition f, List<TcpHexFieldDefinition> merged) {
        if (f == null || f.getKey() == null || merged == null) {
            return false;
        }
        Matcher wm = PA_WORD_FIELD_KEY.matcher(f.getKey().trim());
        if (!wm.matches()) {
            return false;
        }
        String n = wm.group(1);
        String hiKey = "pa" + n + "_hi";
        String loKey = "pa" + n + "_lo";
        boolean hasHi = false;
        boolean hasLo = false;
        for (TcpHexFieldDefinition g : merged) {
            if (g == null || g.getKey() == null) {
                continue;
            }
            String k = g.getKey().trim();
            if (hiKey.equals(k)) {
                hasHi = true;
            } else if (loKey.equals(k)) {
                hasLo = true;
            }
            if (hasHi && hasLo) {
                return true;
            }
        }
        return false;
    }

    /**
     * 兼容「整格 UINT16」与「高/低字节 UINT8」两种下发键名：若存在 {@code paN_word} 且未单独提供 {@code paN_hi}/{@code paN_lo}，
     * 则按大端拆成 hi、lo（16 位线值掩码 0xFFFF）。已显式给出的 hi/lo 不会被覆盖。
     */
    static void expandPaWordHiLoAliases(Map<String, Object> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        Pattern p = PA_WORD_FIELD_KEY;
        for (String key : List.copyOf(values.keySet())) {
            if (key == null) {
                continue;
            }
            String trimmed = key.trim();
            Matcher m = p.matcher(trimmed);
            if (!m.matches()) {
                continue;
            }
            Object w = values.get(key);
            if (w == null) {
                continue;
            }
            long word;
            try {
                word = toLongForIntegral(w, trimmed);
            } catch (IllegalArgumentException | ArithmeticException e) {
                continue;
            }
            word &= 0xFFFFL;
            String n = m.group(1);
            String hk = "pa" + n + "_hi";
            String lk = "pa" + n + "_lo";
            if (!values.containsKey(hk)) {
                values.put(hk, (int) ((word >>> 8) & 0xFF));
            }
            if (!values.containsKey(lk)) {
                values.put(lk, (int) (word & 0xFF));
            }
        }
    }

    private static void writeFieldFromValues(byte[] buf, TcpHexFieldDefinition f, Map<String, Object> values) {
        TcpHexValueType vt = f.getValueType();
        int off = f.getByteOffset();
        double scale = f.getEffectiveScale();

        if (f.getFixedWireIntegralValue() != null) {
            if (vt == null || vt.isBytesAsHex() || !TcpHexCommandProfile.isIntegralMatchType(vt)) {
                throw new IllegalArgumentException("Field [" + f.getKey() + "]: fixedWireIntegralValue requires integral type");
            }
            TcpHexProtocolParser.writeIntegralAt(buf, off, vt, f.getFixedWireIntegralValue());
            return;
        }
        if (f.getFixedBytesHex() != null && !f.getFixedBytesHex().isBlank()) {
            if (vt == null || !vt.isBytesAsHex()) {
                throw new IllegalArgumentException("Field [" + f.getKey() + "]: fixedBytesHex requires BYTES_AS_HEX");
            }
            int len = fieldWidthForBuild(f);
            String clean = f.getFixedBytesHex().replaceAll("\\s+", "");
            if (clean.isEmpty() || (clean.length() & 1) == 1) {
                throw new IllegalArgumentException("Field [" + f.getKey() + "]: invalid fixedBytesHex");
            }
            byte[] parsed = HexFormat.of().parseHex(clean);
            if (parsed.length != len) {
                throw new IllegalArgumentException(
                        "Field [" + f.getKey() + "]: fixedBytesHex must be " + len + " bytes");
            }
            System.arraycopy(parsed, 0, buf, off, len);
            return;
        }

        Object raw = values.get(f.getKey());
        if (vt.isBytesAsHex()) {
            int len = fieldWidthForBuild(f);
            byte[] slice = new byte[len];
            if (raw != null) {
                String hex = raw.toString().replaceAll("\\s+", "");
                if ((hex.length() & 1) == 1) {
                    throw new IllegalArgumentException("Field [" + f.getKey() + "]: invalid hex string length");
                }
                byte[] parsed = HexFormat.of().parseHex(hex);
                if (parsed.length != len) {
                    throw new IllegalArgumentException("Field [" + f.getKey() + "]: hex length must be " + len + " bytes");
                }
                System.arraycopy(parsed, 0, slice, 0, len);
            }
            System.arraycopy(slice, 0, buf, off, len);
            return;
        }

        switch (vt) {
            case UINT8, INT8, UINT16_BE, UINT16_LE, INT16_BE, INT16_LE, UINT32_BE, UINT32_LE, INT32_BE, INT32_LE -> {
                long lv = raw != null ? toLongForIntegral(raw, f.getKey()) : 0L;
                long rawInt = scaleToRawLong(lv, scale, f.getKey());
                TcpHexProtocolParser.writeIntegralAt(buf, off, vt, rawInt);
            }
            case FLOAT_BE, FLOAT_LE -> {
                double dv = raw != null ? toDouble(raw, f.getKey()) : 0.0;
                float fv = (float) (dv / scale);
                TcpHexProtocolParser.writeFloatAt(buf, off, vt, fv);
            }
            case DOUBLE_BE, DOUBLE_LE -> {
                double dv = raw != null ? toDouble(raw, f.getKey()) : 0.0;
                double enc = dv / scale;
                TcpHexProtocolParser.writeDoubleAt(buf, off, vt, enc);
            }
            default -> throw new IllegalArgumentException("Unsupported value type for build: " + vt);
        }
    }

    private static long scaleToRawLong(long logicalScaled, double scale, String key) {
        if (scale == 0.0 || Double.isNaN(scale) || Double.isInfinite(scale)) {
            throw new IllegalArgumentException("Field [" + key + "]: invalid scale");
        }
        double raw = logicalScaled / scale;
        if (Double.isNaN(raw) || Double.isInfinite(raw)) {
            throw new IllegalArgumentException("Field [" + key + "]: value out of range");
        }
        return Math.round(raw);
    }

    private static long toLongForIntegral(Object o, String key) {
        if (o instanceof Number n) {
            return Math.round(n.doubleValue());
        }
        if (o instanceof String s) {
            return Math.round(Double.parseDouble(s.trim()));
        }
        throw new IllegalArgumentException("Field [" + key + "]: expected number for integral type");
    }

    private static double toDouble(Object o, String key) {
        if (o instanceof Number n) {
            return n.doubleValue();
        }
        if (o instanceof String s) {
            return Double.parseDouble(s.trim());
        }
        throw new IllegalArgumentException("Field [" + key + "]: expected number for float/double type");
    }
}

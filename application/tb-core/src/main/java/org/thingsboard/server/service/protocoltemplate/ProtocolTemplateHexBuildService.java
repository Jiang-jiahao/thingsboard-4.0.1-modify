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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 按首帧模板 + 下行/双向命令合并字段组帧，并写入命令字、总长度（若启用）、校验和。
 */
@Service
@RequiredArgsConstructor
public class ProtocolTemplateHexBuildService {

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

        if (cmd.getLtvRepeating() != null || tpl.getHexLtvRepeating() != null) {
            out.setErrorMessage("Downlink HEX build does not support LTV; clear hexLtvRepeating for this template/command");
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

        TcpHexChecksumDefinition checksum = tpl.getChecksum();
        boolean validateLen = Boolean.TRUE.equals(tpl.getValidateTotalLengthU32Le());

        int frameLen;
        try {
            frameLen = computeFrameLength(merged, checksum, validateLen, cmdOff, matchVt);
        } catch (IllegalArgumentException e) {
            out.setErrorMessage(e.getMessage());
            return out;
        }

        byte[] buf = new byte[frameLen];
        Map<String, Object> values = request.getValues() != null ? request.getValues() : Map.of();

        try {
            for (TcpHexFieldDefinition f : merged) {
                if (f == null) {
                    continue;
                }
                f.validate();
                writeFieldFromValues(buf, f, values);
            }
            if (cmd.getSecondaryMatchByteOffset() != null) {
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
            if (validateLen) {
                if (buf.length < 4) {
                    throw new IllegalArgumentException("frame too short for validateTotalLengthU32Le");
                }
                ByteBuffer lb = ByteBuffer.wrap(buf, 0, 4).order(ByteOrder.LITTLE_ENDIAN);
                lb.putInt(buf.length);
            }
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
                                          boolean validateLen32, int cmdOff, TcpHexValueType cmdVt) {
        int maxEnd = 0;
        if (validateLen32) {
            maxEnd = Math.max(maxEnd, 4);
        }
        maxEnd = Math.max(maxEnd, cmdOff + integralTypeWidth(cmdVt));
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

    private static int fieldWidthForBuild(TcpHexFieldDefinition f) {
        TcpHexValueType vt = f.getValueType();
        if (vt.isBytesAsHex()) {
            if (f.getByteLength() != null && f.getByteLength() > 0) {
                return f.getByteLength();
            }
            throw new IllegalArgumentException(
                    "Field [" + f.getKey() + "]: BYTES_AS_HEX for build requires fixed byteLength (dynamic length unsupported)");
        }
        return vt.getFixedByteLength();
    }

    private static void writeFieldFromValues(byte[] buf, TcpHexFieldDefinition f, Map<String, Object> values) {
        Object raw = values.get(f.getKey());
        TcpHexValueType vt = f.getValueType();
        int off = f.getByteOffset();
        double scale = f.getEffectiveScale();

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

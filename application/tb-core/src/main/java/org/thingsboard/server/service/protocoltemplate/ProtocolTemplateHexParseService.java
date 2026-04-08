/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.service.protocoltemplate;

import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.thingsboard.server.common.data.device.profile.HexTransportTcpDataConfiguration;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateBundle;
import org.thingsboard.server.common.data.device.profile.ProtocolTemplateTransportTcpDataConfiguration;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.transport.tcp.util.TcpHexProtocolParser;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 使用租户协议模板包配置对 HEX 帧做与 TCP 上行相同的解析测试。
 */
@Service
@RequiredArgsConstructor
public class ProtocolTemplateHexParseService {

    private static final Pattern HEX_CLEAN = Pattern.compile("[^0-9a-fA-F]");

    private final DefaultProtocolTemplateBundleService bundleService;

    @Transactional(readOnly = true)
    public ProtocolTemplateHexParseResult parse(TenantId tenantId, String bundleIdStr, String rawHex) {
        ProtocolTemplateHexParseResult out = new ProtocolTemplateHexParseResult();
        if (bundleIdStr == null || bundleIdStr.isBlank()) {
            out.setErrorMessage("bundleId is required");
            return out;
        }
        final UUID bundleId;
        try {
            bundleId = UUID.fromString(bundleIdStr.trim());
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
        if (bundle.getProtocolTemplates() == null || bundle.getProtocolTemplates().isEmpty()) {
            out.setErrorMessage("Bundle has no frame templates");
            return out;
        }

        String hex = normalizeHex(rawHex);
        if (hex.isEmpty()) {
            out.setErrorMessage("hex is empty");
            return out;
        }
        if ((hex.length() & 1) == 1) {
            out.setErrorMessage("hex must have an even number of digits");
            return out;
        }

        ProtocolTemplateTransportTcpDataConfiguration pcfg = new ProtocolTemplateTransportTcpDataConfiguration();
        pcfg.setProtocolTemplates(bundle.getProtocolTemplates());
        pcfg.setProtocolCommands(bundle.getProtocolCommands() != null ? bundle.getProtocolCommands() : java.util.List.of());
        HexTransportTcpDataConfiguration hexCfg;
        try {
            hexCfg = pcfg.expandToHexTransportTcpDataConfiguration();
        } catch (RuntimeException e) {
            out.setErrorMessage("Invalid template configuration: " + e.getMessage());
            return out;
        }

        JsonObject payload = new JsonObject();
        payload.addProperty("hex", hex);

        Optional<JsonObject> parsed = TcpHexProtocolParser.tryParseTelemetryFromHexPayload(
                payload,
                hexCfg.getHexCommandProfiles(),
                hexCfg.getHexProtocolFields(),
                hexCfg.getHexLtvRepeating(),
                hexCfg.getValidateTotalLengthU32Le(),
                hexCfg.getChecksum(),
                UUID.randomUUID());

        if (parsed.isEmpty()) {
            out.setErrorMessage("No telemetry produced (checksum mismatch, length check failed, no matching command, or no parseable fields)");
            return out;
        }

        JsonObject jo = parsed.get();
        String matched = null;
        if (jo.has("hexCmdProfile") && !jo.get("hexCmdProfile").isJsonNull()) {
            matched = jo.get("hexCmdProfile").getAsString();
        }
        out.setMatchedHexCommandProfile(matched);
        out.setTelemetry(jsonObjectToFlatMap(jo));
        out.setSuccess(true);
        return out;
    }

    static String normalizeHex(String raw) {
        if (raw == null) {
            return "";
        }
        return HEX_CLEAN.matcher(raw.trim()).replaceAll("").toLowerCase();
    }

    private static Map<String, Object> jsonObjectToFlatMap(JsonObject o) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (String k : o.keySet()) {
            var el = o.get(k);
            if (el == null || el.isJsonNull()) {
                map.put(k, null);
            } else if (el.isJsonPrimitive()) {
                var p = el.getAsJsonPrimitive();
                if (p.isBoolean()) {
                    map.put(k, p.getAsBoolean());
                } else if (p.isNumber()) {
                    Number n = p.getAsNumber();
                    if (n.doubleValue() == Math.rint(n.doubleValue()) && n.longValue() >= Integer.MIN_VALUE && n.longValue() <= Integer.MAX_VALUE) {
                        map.put(k, (int) n.longValue());
                    } else {
                        map.put(k, n.doubleValue());
                    }
                } else {
                    map.put(k, p.getAsString());
                }
            } else {
                map.put(k, el.toString());
            }
        }
        return map;
    }
}

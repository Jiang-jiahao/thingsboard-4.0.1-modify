/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.DefaultDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HTTP 出站 URL / body / header 模板占位符，能力与
 * {@code MqttRpcTemplateResolver} 对齐：{@code ${params}}、{@code ${params.xxx}}、
 * {@code ${device.name}}、{@code ${device.label}}、{@code ${device.externalDeviceId}}、
 * {@code ${requestId}}、{@code ${method}}（后两者也接受 {@code rpc.} 前缀）。
 */
@Slf4j
final class HttpPullTemplateResolver {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    private HttpPullTemplateResolver() {
    }

    static String resolve(String template, Device device, HttpPullDeviceTransportConfiguration deviceCfg,
                          String paramsJson, int requestId, String method) {
        if (StringUtils.isBlank(template)) {
            return template;
        }
        JsonObject params = parseParams(paramsJson);
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String key = matcher.group(1).trim();
            String replacement = resolveKey(key, device, params, requestId, method);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    static Map<String, String> resolveHeaders(Map<String, String> headers, Device device,
                                               HttpPullDeviceTransportConfiguration deviceCfg, String paramsJson,
                                               int requestId, String method) {
        if (headers == null || headers.isEmpty()) {
            return headers;
        }
        Map<String, String> resolved = new HashMap<>();
        headers.forEach((k, v) -> resolved.put(
                resolve(k, device, deviceCfg, paramsJson, requestId, method),
                resolve(v, device, deviceCfg, paramsJson, requestId, method)));
        return resolved;
    }

    private static JsonObject parseParams(String paramsJson) {
        if (StringUtils.isBlank(paramsJson)) {
            return new JsonObject();
        }
        try {
            JsonElement el = JsonParser.parseString(paramsJson);
            return el.isJsonObject() ? el.getAsJsonObject() : new JsonObject();
        } catch (Exception e) {
            return new JsonObject();
        }
    }

    private static String resolveKey(String key, Device device, JsonObject params, int requestId, String method) {
        if ("params".equals(key)) {
            return params.toString();
        }
        if (key.startsWith("params.")) {
            String paramKey = key.substring("params.".length());
            JsonElement el = params.get(paramKey);
            if (el == null || el.isJsonNull()) {
                return "";
            }
            if (el.isJsonPrimitive()) {
                var primitive = el.getAsJsonPrimitive();
                return primitive.isNumber() ? primitive.getAsNumber().toString() : primitive.getAsString();
            }
            return el.toString();
        }
        if (key.startsWith("device.")) {
            String deviceKey = key.substring("device.".length());
            return switch (deviceKey) {
                case "name" -> deviceName(device);
                case "label" -> device != null ? StringUtils.defaultString(device.getLabel(), "") : "";
                case "externalDeviceId" -> externalDeviceId(device);
                default -> unknown(key);
            };
        }
        return switch (key) {
            case "deviceName" -> deviceName(device);
            case "deviceLabel" -> device != null ? StringUtils.defaultString(device.getLabel(), "") : "";
            case "externalDeviceId" -> externalDeviceId(device);
            case "requestId", "rpc.requestId" -> Integer.toString(requestId);
            case "method", "rpc.method" -> method != null ? method : "";
            default -> unknown(key);
        };
    }

    private static String deviceName(Device device) {
        return device != null ? StringUtils.defaultString(device.getName(), "") : "";
    }

    private static String externalDeviceId(Device device) {
        if (device == null || device.getDeviceData() == null) {
            return "";
        }
        DeviceTransportConfiguration cfg = device.getDeviceData().getTransportConfiguration();
        if (cfg instanceof DefaultDeviceTransportConfiguration def
                && StringUtils.isNotBlank(def.getExternalDeviceId())) {
            return def.getExternalDeviceId().trim();
        }
        return "";
    }

    /** 未知占位符保持原样替换为空串的老行为，但要留下日志，避免静默匹配失败难以排查。 */
    private static String unknown(String key) {
        log.warn("Unknown HTTP outbound template placeholder [{}], replaced with empty string", key);
        return "";
    }
}

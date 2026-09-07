/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class HttpPullTemplateResolver {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    private HttpPullTemplateResolver() {
    }

    static String resolve(String template, Device device, HttpPullDeviceTransportConfiguration deviceCfg, String paramsJson) {
        if (StringUtils.isBlank(template)) {
            return template;
        }
        JsonObject params = parseParams(paramsJson);
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String key = matcher.group(1).trim();
            String replacement = resolveKey(key, device, deviceCfg, params);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    static Map<String, String> resolveHeaders(Map<String, String> headers, Device device,
                                              HttpPullDeviceTransportConfiguration deviceCfg, String paramsJson) {
        if (headers == null || headers.isEmpty()) {
            return headers;
        }
        Map<String, String> resolved = new java.util.HashMap<>();
        headers.forEach((k, v) -> resolved.put(
                resolve(k, device, deviceCfg, paramsJson),
                resolve(v, device, deviceCfg, paramsJson)));
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

    private static String resolveKey(String key, Device device, HttpPullDeviceTransportConfiguration deviceCfg,
                                     JsonObject params) {
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
                return el.getAsString();
            }
            return el.toString();
        }
        if (key.startsWith("device.")) {
            String deviceKey = key.substring("device.".length());
            return switch (deviceKey) {
                case "name" -> device != null && device.getName() != null ? device.getName() : "";
                case "label" -> device != null && device.getLabel() != null ? device.getLabel() : "";
                case "externalDeviceId" -> deviceCfg != null && StringUtils.isNotBlank(deviceCfg.getExternalDeviceId())
                        ? deviceCfg.getExternalDeviceId() : "";
                default -> "";
            };
        }
        return "";
    }
}

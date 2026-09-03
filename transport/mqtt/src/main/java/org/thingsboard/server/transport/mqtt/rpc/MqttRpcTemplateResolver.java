/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.rpc;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.device.data.DefaultDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.MqttPullDeviceTransportConfiguration;
import org.thingsboard.server.common.transport.auth.TransportDeviceInfo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MQTT RPC 主题/载荷模板占位符：{@code ${params}}、{@code ${params.xxx}}、
 * {@code ${device.name}}、{@code ${device.label}}、{@code ${device.externalDeviceId}}、
 * {@code ${requestId}}、{@code ${method}}。
 */
public final class MqttRpcTemplateResolver {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");

    private MqttRpcTemplateResolver() {
    }

    public static String resolve(String template, Device device, TransportDeviceInfo deviceInfo,
                                 String paramsJson, int requestId, String method) {
        if (StringUtils.isBlank(template)) {
            return template;
        }
        JsonObject params = parseParams(paramsJson);
        String deviceName = deviceName(device, deviceInfo);
        String deviceLabel = device != null && device.getLabel() != null ? device.getLabel() : "";
        String externalDeviceId = externalDeviceId(device);
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String key = matcher.group(1).trim();
            String replacement = resolveKey(key, deviceName, deviceLabel, externalDeviceId, params, requestId, method);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * 仅解析设备级占位符，其余占位符变为 MQTT {@code +}，用于设备订阅匹配。
     */
    public static String toRequestSubscribeFilter(String template, Device device, TransportDeviceInfo deviceInfo) {
        if (StringUtils.isBlank(template)) {
            return template;
        }
        String deviceName = deviceName(device, deviceInfo);
        String deviceLabel = device != null && device.getLabel() != null ? device.getLabel() : "";
        String externalDeviceId = externalDeviceId(device);
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String key = matcher.group(1).trim();
            String replacement = switch (key) {
                case "device.name", "deviceName" -> deviceName;
                case "device.label", "deviceLabel" -> deviceLabel;
                case "device.externalDeviceId", "externalDeviceId", "deviceId" -> externalDeviceId;
                default -> "+";
            };
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String resolveKey(String key, String deviceName, String deviceLabel, String externalDeviceId,
                                     JsonObject params, int requestId, String method) {
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
                case "name" -> deviceName != null ? deviceName : "";
                case "label" -> deviceLabel != null ? deviceLabel : "";
                case "externalDeviceId" -> externalDeviceId != null ? externalDeviceId : "";
                default -> "";
            };
        }
        return switch (key) {
            case "deviceName" -> deviceName != null ? deviceName : "";
            case "deviceLabel" -> deviceLabel != null ? deviceLabel : "";
            case "externalDeviceId", "deviceId" -> externalDeviceId != null ? externalDeviceId : "";
            case "requestId", "rpc.requestId" -> Integer.toString(requestId);
            case "method", "rpc.method" -> method != null ? method : "";
            default -> "";
        };
    }

    private static String externalDeviceId(Device device) {
        if (device == null || device.getDeviceData() == null) {
            return "";
        }
        DeviceTransportConfiguration cfg = device.getDeviceData().getTransportConfiguration();
        if (cfg instanceof MqttPullDeviceTransportConfiguration mqtt
                && StringUtils.isNotBlank(mqtt.getExternalDeviceId())) {
            return mqtt.getExternalDeviceId().trim();
        }
        if (cfg instanceof HttpPullDeviceTransportConfiguration http
                && StringUtils.isNotBlank(http.getExternalDeviceId())) {
            return http.getExternalDeviceId().trim();
        }
        if (cfg instanceof DefaultDeviceTransportConfiguration def
                && StringUtils.isNotBlank(def.getExternalDeviceId())) {
            return def.getExternalDeviceId().trim();
        }
        return "";
    }

    private static String deviceName(Device device, TransportDeviceInfo deviceInfo) {
        if (device != null && StringUtils.isNotBlank(device.getName())) {
            return device.getName();
        }
        if (deviceInfo != null && StringUtils.isNotBlank(deviceInfo.getDeviceName())) {
            return deviceInfo.getDeviceName();
        }
        return "";
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
}

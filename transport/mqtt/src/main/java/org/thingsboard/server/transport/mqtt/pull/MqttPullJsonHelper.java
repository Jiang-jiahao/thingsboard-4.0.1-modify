/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.server.common.data.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
public final class MqttPullJsonHelper {

    private static final Configuration JSON_PATH_CONF = Configuration.defaultConfiguration();

    private MqttPullJsonHelper() {
    }

    public static List<Object> readArrayElements(String jsonBody, String arrayJsonPath) {
        if (StringUtils.isBlank(jsonBody)) {
            return Collections.emptyList();
        }
        var ctx = JsonPath.using(JSON_PATH_CONF).parse(jsonBody);
        if (StringUtils.isBlank(arrayJsonPath)) {
            Object root = ctx.read("$");
            if (root instanceof List<?> list) {
                return new ArrayList<>(list);
            }
            return List.of(root);
        }
        try {
            Object value = ctx.read(arrayJsonPath);
            if (value instanceof List<?> list) {
                return new ArrayList<>(list);
            }
            if (value != null) {
                return List.of(value);
            }
        } catch (PathNotFoundException e) {
            log.debug("Array path not found: {}", arrayJsonPath);
        }
        return Collections.emptyList();
    }

    public static String readDeviceId(Object element, String deviceIdJsonPath) {
        if (element == null || StringUtils.isBlank(deviceIdJsonPath)) {
            return null;
        }
        String json = element instanceof String s ? s : JsonPath.using(JSON_PATH_CONF).parse(element).jsonString();
        try {
            Object value = JsonPath.using(JSON_PATH_CONF).parse(json).read(deviceIdJsonPath.startsWith("$") ? deviceIdJsonPath : "$." + deviceIdJsonPath);
            return value != null ? String.valueOf(value) : null;
        } catch (Exception e) {
            log.debug("Device id path {} not found in element", deviceIdJsonPath);
            return null;
        }
    }

    public static String elementToJsonString(Object element) {
        if (element == null) {
            return "{}";
        }
        if (element instanceof String s) {
            return s;
        }
        return JsonPath.using(JSON_PATH_CONF).parse(element).jsonString();
    }

    public static String readDeviceIdFromBody(String jsonBody, String deviceIdJsonPath) {
        if (StringUtils.isBlank(jsonBody) || StringUtils.isBlank(deviceIdJsonPath)) {
            return null;
        }
        try {
            String path = deviceIdJsonPath.startsWith("$") ? deviceIdJsonPath : "$." + deviceIdJsonPath;
            Object value = JsonPath.using(JSON_PATH_CONF).parse(jsonBody).read(path);
            return value != null ? String.valueOf(value) : null;
        } catch (Exception e) {
            log.debug("Device id path {} not found in message body", deviceIdJsonPath);
            return null;
        }
    }
}

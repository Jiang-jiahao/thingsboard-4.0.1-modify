/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.mqtt;

import org.thingsboard.server.common.data.StringUtils;

/**
 * 将设备级主题前缀与档案主题拼接，并替换设备占位符。
 * <p>无人机监管：档案写相对路径 {@code api/locate}，设备填写 Server Topic 前缀 {@code server/chan}，
 * 实际订阅 {@code server/chan/api/locate}。前缀为空则原样使用档案主题。
 * <p>大公博创无 Server Topic 前缀：档案写 {@code dgb/${device.externalDeviceId}/status/detect_report}，
 * 设备传输配置填写对方 {@code deviceid}。
 */
public final class MqttPullTopicPrefix {

    private MqttPullTopicPrefix() {
    }

    public static String resolve(String prefix, String topic) {
        return resolve(prefix, topic, null, null, null);
    }

    public static String resolve(String prefix, String topic, String deviceName, String deviceLabel) {
        return resolve(prefix, topic, deviceName, deviceLabel, null);
    }

    public static String resolve(String prefix, String topic, String deviceName, String deviceLabel, String externalDeviceId) {
        if (StringUtils.isBlank(topic)) {
            return topic;
        }
        String t = topic.trim();
        while (t.startsWith("/")) {
            t = t.substring(1);
        }
        if (!StringUtils.isBlank(prefix)) {
            String p = prefix.trim();
            while (p.endsWith("/")) {
                p = p.substring(0, p.length() - 1);
            }
            if (!StringUtils.isBlank(p) && !t.equals(p) && !t.startsWith(p + "/")) {
                t = p + "/" + t;
            }
        }
        return resolveDevicePlaceholders(t, deviceName, deviceLabel, externalDeviceId);
    }

    public static String resolveDevicePlaceholders(String topic, String deviceName, String deviceLabel, String externalDeviceId) {
        if (StringUtils.isBlank(topic) || !topic.contains("${")) {
            return topic;
        }
        String name = deviceName != null ? deviceName : "";
        String label = deviceLabel != null ? deviceLabel : "";
        String id = externalDeviceId != null ? externalDeviceId : "";
        return topic
                .replace("${device.externalDeviceId}", id)
                .replace("${externalDeviceId}", id)
                .replace("${deviceId}", id)
                .replace("${device.name}", name)
                .replace("${deviceName}", name)
                .replace("${device.label}", label)
                .replace("${deviceLabel}", label);
    }
}

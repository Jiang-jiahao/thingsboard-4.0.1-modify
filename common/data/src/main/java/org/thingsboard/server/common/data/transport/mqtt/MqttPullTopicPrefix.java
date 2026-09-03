/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.mqtt;

import org.thingsboard.server.common.data.StringUtils;

/**
 * 将设备级主题前缀与档案主题拼接；前缀为空时使用档案主题原文。
 */
public final class MqttPullTopicPrefix {

    private MqttPullTopicPrefix() {
    }

    public static String resolve(String prefix, String topic) {
        if (StringUtils.isBlank(topic)) {
            return topic;
        }
        String t = topic.trim();
        while (t.startsWith("/")) {
            t = t.substring(1);
        }
        if (StringUtils.isBlank(prefix)) {
            return t;
        }
        String p = prefix.trim();
        while (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        if (StringUtils.isBlank(p) || t.equals(p) || t.startsWith(p + "/")) {
            return t;
        }
        return p + "/" + t;
    }

    /**
     * 发布不能带 MQTT 通配符。将路径中的 {@code +} 段替换为已观测到的实际值。
     */
    public static String fillPlusSegments(String topic, String replacement) {
        if (StringUtils.isBlank(topic) || StringUtils.isBlank(replacement) || !topic.contains("+")) {
            return topic;
        }
        String[] parts = topic.split("/", -1);
        for (int i = 0; i < parts.length; i++) {
            if ("+".equals(parts[i])) {
                parts[i] = replacement;
            }
        }
        return String.join("/", parts);
    }

    public static String firstPlusSegment(String filter, String topic) {
        if (StringUtils.isBlank(filter) || StringUtils.isBlank(topic) || !filter.contains("+")) {
            return null;
        }
        String[] filterParts = filter.split("/", -1);
        String[] topicParts = topic.split("/", -1);
        if (filterParts.length != topicParts.length) {
            return null;
        }
        for (int i = 0; i < filterParts.length; i++) {
            if ("+".equals(filterParts[i]) && StringUtils.isNotBlank(topicParts[i])) {
                return topicParts[i];
            }
        }
        return null;
    }
}

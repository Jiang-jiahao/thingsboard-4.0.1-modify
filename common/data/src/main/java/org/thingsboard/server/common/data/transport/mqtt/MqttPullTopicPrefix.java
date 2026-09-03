/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.mqtt;

import org.thingsboard.server.common.data.StringUtils;

/**
 * 将设备级主题前缀与档案相对主题拼接。档案可写 {@code api/locate}，设备填写 {@code server/chan}
 *（不同防区可改），实际订阅 {@code server/chan/api/locate}。
 * 若档案主题已包含该前缀，则不再重复拼接。
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
        if (StringUtils.isBlank(p)) {
            return t;
        }
        if (t.equals(p) || t.startsWith(p + "/")) {
            return t;
        }
        return p + "/" + t;
    }
}

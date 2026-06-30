/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull;

import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;

final class MqttPullDeviceIdResolver {

    private MqttPullDeviceIdResolver() {
    }

    static String resolve(String topic, String body, HttpPullDeviceRoutingConfiguration routing) {
        if (routing == null) {
            return null;
        }
        if (StringUtils.isNotBlank(routing.getDeviceIdJsonPath()) && StringUtils.isNotBlank(body)) {
            String fromPayload = MqttPullJsonHelper.readDeviceIdFromBody(body, routing.getDeviceIdJsonPath());
            if (StringUtils.isNotBlank(fromPayload)) {
                return fromPayload.trim();
            }
        }
        if (routing.getDeviceIdTopicSegmentIndex() != null && StringUtils.isNotBlank(topic)) {
            String fromTopic = extractTopicSegment(topic, routing.getDeviceIdTopicSegmentIndex());
            if (StringUtils.isNotBlank(fromTopic)) {
                return fromTopic.trim();
            }
        }
        return null;
    }

    static String extractTopicSegment(String topic, int segmentIndex) {
        String[] parts = topic.split("/");
        if (parts.length == 0) {
            return null;
        }
        int idx = segmentIndex < 0 ? parts.length + segmentIndex : segmentIndex;
        if (idx >= 0 && idx < parts.length) {
            String segment = parts[idx];
            return StringUtils.isNotBlank(segment) ? segment : null;
        }
        return null;
    }
}

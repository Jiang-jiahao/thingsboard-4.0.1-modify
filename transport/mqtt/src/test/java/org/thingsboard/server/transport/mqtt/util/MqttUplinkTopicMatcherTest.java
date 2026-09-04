/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.util;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.mqtt.MqttUplinkTopicMapping;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MqttUplinkTopicMatcherTest {

    @Test
    void firstMatchingMappingWins() {
        MqttUplinkTopicMapping detect = mapping("detect", "dgb/+/status/detect_report", HttpPullPollDataType.TELEMETRY);
        MqttUplinkTopicMapping wildcard = mapping("all-status", "dgb/+/status/#", HttpPullPollDataType.TELEMETRY);
        MqttUplinkTopicMatcher matcher = new MqttUplinkTopicMatcher(List.of(detect, wildcard));

        assertThat(matcher.find("dgb/1/status/detect_report").getName()).isEqualTo("detect");
        assertThat(matcher.find("dgb/1/status/aoa_location").getName()).isEqualTo("all-status");
        assertThat(matcher.find("v1/devices/me/telemetry")).isNull();
    }

    @Test
    void emptyMatcherMatchesNothing() {
        assertThat(new MqttUplinkTopicMatcher(null).find("v1/devices/me/telemetry")).isNull();
        assertThat(new MqttUplinkTopicMatcher(List.of()).isEmpty()).isTrue();
    }

    private static MqttUplinkTopicMapping mapping(String name, String topic, HttpPullPollDataType dataType) {
        MqttUplinkTopicMapping mapping = new MqttUplinkTopicMapping();
        mapping.setName(name);
        mapping.setTopic(topic);
        mapping.setDataType(dataType);
        mapping.setEnabled(true);
        return mapping;
    }
}

/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.mqtt;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MqttPullTopicPrefixTest {

    @Test
    public void uavRelativeTopicIsPrefixed() {
        assertThat(MqttPullTopicPrefix.resolve("server/chan", "api/locate"))
                .isEqualTo("server/chan/api/locate");
    }

    @Test
    public void blankPrefixKeepsProfileTopic() {
        assertThat(MqttPullTopicPrefix.resolve(null, "dgb/${device.externalDeviceId}/status/detect_report",
                "ignored-name", null, "W1014_01"))
                .isEqualTo("dgb/W1014_01/status/detect_report");
        assertThat(MqttPullTopicPrefix.resolve("  ", "dgb/${deviceId}/request/detect_open",
                null, "lab", "W1014_01"))
                .isEqualTo("dgb/W1014_01/request/detect_open");
    }

    @Test
    public void doesNotDoublePrefix() {
        assertThat(MqttPullTopicPrefix.resolve("server/chan", "server/chan/api/locate"))
                .isEqualTo("server/chan/api/locate");
    }
}

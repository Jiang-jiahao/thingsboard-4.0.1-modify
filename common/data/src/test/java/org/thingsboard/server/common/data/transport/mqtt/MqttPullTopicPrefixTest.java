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
    public void relativeTopicIsPrefixed() {
        assertThat(MqttPullTopicPrefix.resolve("site/a", "api/data"))
                .isEqualTo("site/a/api/data");
    }

    @Test
    public void blankPrefixKeepsProfileTopic() {
        assertThat(MqttPullTopicPrefix.resolve(null, "peer/+/status"))
                .isEqualTo("peer/+/status");
    }

    @Test
    public void doesNotDoublePrefix() {
        assertThat(MqttPullTopicPrefix.resolve("site/a", "site/a/api/data"))
                .isEqualTo("site/a/api/data");
    }

    @Test
    public void fillPlusForPublish() {
        assertThat(MqttPullTopicPrefix.fillPlusSegments("peer/+/cmd", "dev-1"))
                .isEqualTo("peer/dev-1/cmd");
        assertThat(MqttPullTopicPrefix.firstPlusSegment("peer/+/status", "peer/dev-1/status"))
                .isEqualTo("dev-1");
    }
}

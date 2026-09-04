/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.mqtt.MqttUplinkTopicMapping;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttDeviceProfileTransportConfigurationTest {

    @Test
    void emptyMappingsFallBackToLegacyTopics() {
        MqttDeviceProfileTransportConfiguration cfg = new MqttDeviceProfileTransportConfiguration();
        cfg.setDeviceTelemetryTopic("custom/telemetry");
        cfg.setDeviceAttributesTopic("custom/attributes");

        List<MqttUplinkTopicMapping> mappings = cfg.effectiveUplinkMappings();
        assertThat(mappings).hasSize(2);
        assertThat(mappings.get(0).getTopic()).isEqualTo("custom/telemetry");
        assertThat(mappings.get(0).getDataType()).isEqualTo(HttpPullPollDataType.TELEMETRY);
        assertThat(mappings.get(0).hasTelemetryPayloadKey()).isFalse();
        assertThat(mappings.get(1).getTopic()).isEqualTo("custom/attributes");
        assertThat(mappings.get(1).getDataType()).isEqualTo(HttpPullPollDataType.CLIENT_ATTRIBUTES);
    }

    @Test
    void storedMappingsTakePrecedenceAndSkipDisabled() {
        MqttDeviceProfileTransportConfiguration cfg = new MqttDeviceProfileTransportConfiguration();
        cfg.setDeviceTelemetryTopic("v1/devices/me/telemetry");
        MqttUplinkTopicMapping detect = mapping("detect", "dgb/+/status/detect", HttpPullPollDataType.TELEMETRY, "detect");
        MqttUplinkTopicMapping disabled = mapping("old", "old/topic", HttpPullPollDataType.TELEMETRY, "old");
        disabled.setEnabled(false);
        MqttUplinkTopicMapping shared = mapping("meta", "dgb/+/status/meta", HttpPullPollDataType.SHARED_ATTRIBUTES, null);
        cfg.setUplinkTopicMappings(List.of(detect, disabled, shared));

        List<MqttUplinkTopicMapping> mappings = cfg.effectiveUplinkMappings();
        assertThat(mappings).hasSize(2);
        assertThat(mappings.get(0).getName()).isEqualTo("detect");
        assertThat(mappings.get(1).getName()).isEqualTo("meta");
    }

    @Test
    void validateSyncsLegacyTopicsFromFirstEnabledMappings() {
        MqttDeviceProfileTransportConfiguration cfg = new MqttDeviceProfileTransportConfiguration();
        cfg.setUplinkTopicMappings(List.of(
                mapping("detect", "dgb/+/status/detect", HttpPullPollDataType.TELEMETRY, "detect"),
                mapping("attr", "dgb/+/status/attr", HttpPullPollDataType.CLIENT_ATTRIBUTES, null)
        ));
        cfg.validate();
        assertThat(cfg.getDeviceTelemetryTopic()).isEqualTo("dgb/+/status/detect");
        assertThat(cfg.getDeviceAttributesTopic()).isEqualTo("dgb/+/status/attr");
    }

    @Test
    void validateRejectsDuplicateEnabledTopics() {
        MqttDeviceProfileTransportConfiguration cfg = new MqttDeviceProfileTransportConfiguration();
        cfg.setUplinkTopicMappings(List.of(
                mapping("a", "same/topic", HttpPullPollDataType.TELEMETRY, "a"),
                mapping("b", "same/topic", HttpPullPollDataType.CLIENT_ATTRIBUTES, null)
        ));
        assertThatThrownBy(cfg::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unique");
    }

    @Test
    void validateAllowsDuplicateWhenOneDisabled() {
        MqttUplinkTopicMapping disabled = mapping("a", "same/topic", HttpPullPollDataType.TELEMETRY, "a");
        disabled.setEnabled(false);
        MqttDeviceProfileTransportConfiguration cfg = new MqttDeviceProfileTransportConfiguration();
        cfg.setUplinkTopicMappings(List.of(
                disabled,
                mapping("b", "same/topic", HttpPullPollDataType.CLIENT_ATTRIBUTES, null)
        ));
        cfg.validate();
    }

    private static MqttUplinkTopicMapping mapping(String name, String topic, HttpPullPollDataType dataType, String key) {
        MqttUplinkTopicMapping mapping = new MqttUplinkTopicMapping();
        mapping.setName(name);
        mapping.setTopic(topic);
        mapping.setDataType(dataType);
        mapping.setTelemetryPayloadKey(key);
        mapping.setEnabled(true);
        return mapping;
    }
}

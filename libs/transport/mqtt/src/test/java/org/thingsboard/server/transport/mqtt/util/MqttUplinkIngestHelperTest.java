/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.util;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.adaptor.AdaptorException;
import org.thingsboard.server.gen.transport.TransportProtos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MqttUplinkIngestHelperTest {

    @Test
    void wrapTelemetryStoresPayloadUnderKey() {
        TransportProtos.PostTelemetryMsg msg = MqttUplinkIngestHelper.toWrappedTelemetry(
                "{\"freq\":2400}", "detect");
        assertThat(msg.getTsKvListCount()).isEqualTo(1);
        TransportProtos.TsKvListProto tsKv = msg.getTsKvList(0);
        assertThat(tsKv.getKvList()).extracting(TransportProtos.KeyValueProto::getKey).containsExactly("detect");
        assertThat(tsKv.getKv(0).getJsonV()).contains("freq");
    }

    @Test
    void wrapTelemetryKeepsNonJsonAsString() {
        TransportProtos.PostTelemetryMsg msg = MqttUplinkIngestHelper.toWrappedTelemetry("not-json", "raw");
        assertThat(msg.getTsKvList(0).getKv(0).getKey()).isEqualTo("raw");
        assertThat(msg.getTsKvList(0).getKv(0).getStringV()).isEqualTo("not-json");
    }

    @Test
    void attributesCanBeMarkedShared() throws AdaptorException {
        TransportProtos.PostAttributeMsg msg = MqttUplinkIngestHelper.toAttributes("{\"fw\":\"v1.2\"}", true);
        assertThat(msg.getShared()).isTrue();
        assertThat(msg.getKvList()).extracting(TransportProtos.KeyValueProto::getKey).containsExactly("fw");
        assertThat(msg.getKv(0).getStringV()).isEqualTo("v1.2");
    }

    @Test
    void attributesRejectNonObjectJson() {
        assertThatThrownBy(() -> MqttUplinkIngestHelper.toAttributes("[1,2]", false))
                .isInstanceOf(AdaptorException.class)
                .hasMessageContaining("JSON object");
    }
}

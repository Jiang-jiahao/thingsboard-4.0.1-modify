/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.transport.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpPullPollRequestTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void resolveTelemetryPayloadKeyFallsBackToLegacyRouting() {
        HttpPullPollRequest request = new HttpPullPollRequest();
        HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();
        routing.setTelemetryPayloadKey("detect");
        request.setRouting(routing);

        assertThat(request.resolveTelemetryPayloadKey()).isEqualTo("detect");
    }

    @Test
    void resolveTelemetryPayloadKeyPrefersExplicitField() {
        HttpPullPollRequest request = new HttpPullPollRequest();
        request.setTelemetryPayloadKey("explicit");
        HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();
        routing.setTelemetryPayloadKey("fromRouting");
        request.setRouting(routing);

        assertThat(request.resolveTelemetryPayloadKey()).isEqualTo("explicit");
    }

    @Test
    void jacksonOldRoutingOnlyJsonKeepsCustomTelemetryKey() throws Exception {
        String json = """
                {
                  "pollUrl": "http://example/data",
                  "pollMethod": "GET",
                  "routing": {
                    "routingMode": "MULTI_DEVICE",
                    "deviceIdJsonPath": "deviceId",
                    "telemetryPayloadKey": "detect"
                  }
                }
                """;
        HttpPullPollRequest request = mapper.readValue(json, HttpPullPollRequest.class);
        assertThat(request.resolveTelemetryPayloadKey()).isEqualTo("detect");
        request.validate();
        assertThat(request.getId()).isNotBlank();
    }

    @Test
    void validateRejectsBlankPollUrl() {
        HttpPullPollRequest request = new HttpPullPollRequest();
        assertThatThrownBy(request::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pollUrl");
    }

    @Test
    void defaultTelemetryKeyWhenNothingConfigured() {
        HttpPullPollRequest request = new HttpPullPollRequest();
        request.setPollUrl("http://example/data");
        request.validate();
        assertThat(request.resolveTelemetryPayloadKey()).isEqualTo("httpPullPayload");
    }
}

/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullDeviceRoutingConfiguration;
import org.thingsboard.server.common.data.transport.http.HttpPullPollDataType;
import org.thingsboard.server.common.data.transport.http.HttpPullPollRequest;
import org.thingsboard.server.common.data.transport.http.HttpPullRoutingMode;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpPullDeviceProfileTransportConfigurationTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void effectivePollRequestsSkipDisabled() {
        HttpPullDeviceProfileTransportConfiguration cfg = profileWithRequests(
                request("on", "http://example/on", true),
                request("off", "http://example/off", false)
        );
        assertThat(cfg.effectivePollRequests()).extracting(HttpPullPollRequest::getName).containsExactly("on");
    }

    @Test
    void legacyPollUrlBecomesSingleRequest() {
        HttpPullDeviceProfileTransportConfiguration cfg = new HttpPullDeviceProfileTransportConfiguration();
        cfg.setPollUrl("http://legacy/api");
        cfg.setPollMethod("POST");
        HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();
        routing.setTelemetryPayloadKey("legacyPayload");
        cfg.setRouting(routing);

        List<HttpPullPollRequest> requests = cfg.effectivePollRequests();
        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).getPollUrl()).isEqualTo("http://legacy/api");
        assertThat(requests.get(0).getPollMethod()).isEqualTo("POST");
        assertThat(requests.get(0).resolveTelemetryPayloadKey()).isEqualTo("legacyPayload");
    }

    @Test
    void validateAcceptsOldMultiDeviceRoutingWithoutSplittingRequirement() {
        HttpPullPollRequest request = request("poll-1", "http://example/data", true);
        HttpPullDeviceRoutingConfiguration routing = new HttpPullDeviceRoutingConfiguration();
        routing.setRoutingMode(HttpPullRoutingMode.MULTI_DEVICE);
        routing.setDeviceIdJsonPath(null);
        request.setRouting(routing);
        HttpPullDeviceProfileTransportConfiguration cfg = profileWithRequests(request);

        assertThatCode(cfg::validate).doesNotThrowAnyException();
    }

    @Test
    void validateRejectsMissingPollUrl() {
        HttpPullDeviceProfileTransportConfiguration cfg = new HttpPullDeviceProfileTransportConfiguration();
        assertThatThrownBy(cfg::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void jacksonReadsOldProfileWithRoutingBlock() throws Exception {
        String json = """
                {
                  "type": "HTTP_PULL",
                  "queryingFrequencyMs": 15000,
                  "timeoutMs": 10000,
                  "readTimeoutMs": 10000,
                  "pollRequests": [{
                    "name": "poll-1",
                    "enabled": true,
                    "pollUrl": "http://192.168.1.10/api/list",
                    "pollMethod": "GET",
                    "dataType": "TELEMETRY",
                    "routing": {
                      "routingMode": "MULTI_DEVICE",
                      "deviceIdJsonPath": "deviceId",
                      "telemetryPayloadKey": "detect"
                    }
                  }]
                }
                """;
        DeviceProfileTransportConfiguration raw = mapper.readValue(json, DeviceProfileTransportConfiguration.class);
        assertThat(raw).isInstanceOf(HttpPullDeviceProfileTransportConfiguration.class);
        HttpPullDeviceProfileTransportConfiguration cfg = (HttpPullDeviceProfileTransportConfiguration) raw;
        cfg.validate();
        HttpPullPollRequest request = cfg.effectivePollRequests().get(0);
        assertThat(request.resolveTelemetryPayloadKey()).isEqualTo("detect");
    }

    @Test
    void jacksonReadsOldDeviceConfigAndKeepsPollUrlOverride() throws Exception {
        String json = """
                {
                  "type": "HTTP_PULL",
                  "collector": false,
                  "externalDeviceId": "ext-1",
                  "collectorDeviceId": "11111111-1111-1111-1111-111111111111",
                  "pollUrlOverride": "10.0.0.8:8080"
                }
                """;
        DeviceTransportConfiguration raw = mapper.readValue(json, DeviceTransportConfiguration.class);
        assertThat(raw).isInstanceOf(HttpPullDeviceTransportConfiguration.class);
        HttpPullDeviceTransportConfiguration cfg = (HttpPullDeviceTransportConfiguration) raw;
        assertThat(cfg.getPollUrlOverride()).isEqualTo("10.0.0.8:8080");
        assertThatCode(cfg::validate).doesNotThrowAnyException();
    }

    private static HttpPullDeviceProfileTransportConfiguration profileWithRequests(HttpPullPollRequest... requests) {
        HttpPullDeviceProfileTransportConfiguration cfg = new HttpPullDeviceProfileTransportConfiguration();
        cfg.setPollRequests(List.of(requests));
        return cfg;
    }

    private static HttpPullPollRequest request(String name, String url, boolean enabled) {
        HttpPullPollRequest request = new HttpPullPollRequest();
        request.setName(name);
        request.setPollUrl(url);
        request.setEnabled(enabled);
        request.setDataType(HttpPullPollDataType.TELEMETRY);
        return request;
    }
}

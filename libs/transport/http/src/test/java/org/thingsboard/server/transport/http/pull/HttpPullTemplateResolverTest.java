/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpPullTemplateResolverTest {

    @Test
    void resolvesDeviceNameAndParams() {
        Device device = new Device();
        device.setName("cam-01");
        device.setLabel("front");
        String resolved = HttpPullTemplateResolver.resolve(
                "http://host/cmd/${device.name}?cmd=${params.command}",
                device,
                new HttpPullDeviceTransportConfiguration(),
                "{\"command\":\"reboot\"}", 0, null);
        assertThat(resolved).isEqualTo("http://host/cmd/cam-01?cmd=reboot");
    }

    @Test
    void resolvesRequestIdAndMethod() {
        Device device = new Device();
        device.setName("cam-01");
        String resolved = HttpPullTemplateResolver.resolve(
                "/cmd/${device.name}/${requestId}/${method}",
                device,
                new HttpPullDeviceTransportConfiguration(),
                "{}", 17, "reboot");
        assertThat(resolved).isEqualTo("/cmd/cam-01/17/reboot");
    }

    @Test
    void resolvesDeviceLabel() {
        Device device = new Device();
        device.setName("cam-01");
        device.setLabel("front");
        String resolved = HttpPullTemplateResolver.resolve(
                "${device.label}",
                device,
                new HttpPullDeviceTransportConfiguration(),
                "{}", 0, null);
        assertThat(resolved).isEqualTo("front");
    }

    @Test
    void externalDeviceIdPlaceholderIsEmptyWithoutRouting() {
        Device device = new Device();
        device.setName("cam-01");
        String resolved = HttpPullTemplateResolver.resolve(
                "${device.externalDeviceId}",
                device,
                new HttpPullDeviceTransportConfiguration(),
                "{}", 0, null);
        assertThat(resolved).isEmpty();
    }

    @Test
    void resolveHeaders() {
        Device device = new Device();
        device.setName("cam-01");
        Map<String, String> headers = HttpPullTemplateResolver.resolveHeaders(
                Map.of("X-Device", "${device.name}", "X-Rid", "${requestId}"),
                device,
                new HttpPullDeviceTransportConfiguration(),
                "{}", 3, "cmd");
        assertThat(headers).containsEntry("X-Device", "cam-01");
        assertThat(headers).containsEntry("X-Rid", "3");
    }
}

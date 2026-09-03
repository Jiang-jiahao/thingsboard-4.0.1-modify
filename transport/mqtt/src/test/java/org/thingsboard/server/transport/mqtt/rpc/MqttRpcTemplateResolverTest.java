/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.rpc;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.Device;

import static org.assertj.core.api.Assertions.assertThat;

public class MqttRpcTemplateResolverTest {

    @Test
    public void requestAndResponseTopicsUseParamsDeviceId() {
        Device device = new Device();
        device.setName("device-a");
        assertThat(MqttRpcTemplateResolver.resolve(
                "peer/${params.deviceId}/request/cmd", device, null, "{\"deviceId\":1}", 7, "detectOpen"))
                .isEqualTo("peer/1/request/cmd");
        assertThat(MqttRpcTemplateResolver.resolve(
                "peer/${params.deviceId}/response/cmd", device, null, "{\"deviceId\":\"2\"}", 7, "detectOpen"))
                .isEqualTo("peer/2/response/cmd");
    }

    @Test
    public void missingParamsDeviceIdLeavesEmptySegment() {
        assertThat(MqttRpcTemplateResolver.resolve(
                "peer/${params.deviceId}/request/cmd", new Device(), null, "{}", 1, "detectOpen"))
                .isEqualTo("peer//request/cmd");
    }

    @Test
    public void requestAndResponseTopicsUseParamsPrefix() {
        assertThat(MqttRpcTemplateResolver.resolve(
                "${params.prefix}/api/jammer", new Device(), null,
                "{\"prefix\":\"server/chan\"}", 1, "jammer"))
                .isEqualTo("server/chan/api/jammer");
        assertThat(MqttRpcTemplateResolver.resolve(
                "${params.prefix}/api/jammerresult", new Device(), null,
                "{\"prefix\":\"site/b\"}", 1, "jammer"))
                .isEqualTo("site/b/api/jammerresult");
    }
}

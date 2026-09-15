/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.common.data.device.profile;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceProfileRpcMethodScheduleTest {

    @Test
    void scheduleActiveOnlyForHttpOutboundWithValidInterval() {
        DeviceProfileRpcMethod method = baseHttpOutbound();
        method.setScheduleEnabled(true);
        method.setScheduleIntervalMs(5000L);
        assertThat(method.isScheduleActive()).isTrue();
        method.validate(null);
    }

    @Test
    void scheduleRequiresMinInterval() {
        DeviceProfileRpcMethod method = baseHttpOutbound();
        method.setScheduleEnabled(true);
        method.setScheduleIntervalMs(500L);
        assertThat(method.isScheduleActive()).isFalse();
        assertThatThrownBy(() -> method.validate(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheduleIntervalMs");
    }

    @Test
    void scheduleAllowedForNativeBinding() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("nativePing");
        method.setBindingType(DeviceProfileRpcBindingType.NATIVE);
        method.setDeviceMethod("ping");
        method.setScheduleEnabled(true);
        method.setScheduleIntervalMs(5000L);
        assertThat(method.isScheduleActive()).isTrue();
        method.validate(null);
    }

    private static DeviceProfileRpcMethod baseHttpOutbound() {
        DeviceProfileRpcMethod method = new DeviceProfileRpcMethod();
        method.setId("httpSetValue");
        method.setBindingType(DeviceProfileRpcBindingType.HTTP_OUTBOUND);
        method.setHttpUrl("http://127.0.0.1/rpc");
        method.setHttpMethod("POST");
        return method;
    }
}

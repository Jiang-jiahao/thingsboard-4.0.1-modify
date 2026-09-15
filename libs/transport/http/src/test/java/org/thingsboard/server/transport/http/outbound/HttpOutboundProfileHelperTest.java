/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.outbound;

import org.junit.jupiter.api.Test;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.device.profile.DeviceProfileData;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcBindingType;
import org.thingsboard.server.common.data.device.profile.DeviceProfileRpcMethod;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HttpOutboundProfileHelperTest {

    @Test
    void profileHasHttpOutboundDetectsBinding() {
        DeviceProfileRpcMethod outbound = new DeviceProfileRpcMethod();
        outbound.setId("set");
        outbound.setBindingType(DeviceProfileRpcBindingType.HTTP_OUTBOUND);
        DeviceProfileRpcMethod nativeRpc = new DeviceProfileRpcMethod();
        nativeRpc.setId("poll");
        nativeRpc.setBindingType(DeviceProfileRpcBindingType.NATIVE);

        DeviceProfileData data = new DeviceProfileData();
        data.setRpcMethods(List.of(nativeRpc, outbound));
        DeviceProfile profile = new DeviceProfile();
        profile.setProfileData(data);

        assertThat(HttpOutboundTransportContext.profileHasHttpOutbound(profile)).isTrue();
    }

    @Test
    void profileHasHttpOutboundFalseWhenOnlyNative() {
        DeviceProfileRpcMethod nativeRpc = new DeviceProfileRpcMethod();
        nativeRpc.setId("poll");
        nativeRpc.setBindingType(DeviceProfileRpcBindingType.NATIVE);
        DeviceProfileData data = new DeviceProfileData();
        data.setRpcMethods(List.of(nativeRpc));
        DeviceProfile profile = new DeviceProfile();
        profile.setProfileData(data);

        assertThat(HttpOutboundTransportContext.profileHasHttpOutbound(profile)).isFalse();
        assertThat(HttpOutboundTransportContext.profileHasHttpOutbound(null)).isFalse();
    }
}

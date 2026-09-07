/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.push.session;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.device.data.DefaultDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.DefaultDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.transport.http.push.HttpPushRoutingService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
public class HttpPushGatewaySessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private SessionInfoProto gatewaySessionInfo;
    private DefaultDeviceProfileTransportConfiguration profileTransportConfiguration;
    private DefaultDeviceTransportConfiguration deviceTransportConfiguration;
    private HttpPushRoutingService routingService;

    @Builder.Default
    private final Map<String, HttpPushTargetSession> activeTargets = new ConcurrentHashMap<>();

    public DeviceId getDeviceId() {
        return device.getId();
    }

    public void clearTargets() {
        activeTargets.clear();
    }
}

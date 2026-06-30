/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.mqtt.pull.session;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.device.data.MqttPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.MqttPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.transport.mqtt.pull.MqttPullTransportContext;
import org.thingsboard.mqtt.MqttClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Data
@Builder
public class MqttPullCollectorSessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private String token;
    private SessionInfoProto sessionInfo;
    private MqttPullDeviceProfileTransportConfiguration profileTransportConfiguration;
    private MqttPullDeviceTransportConfiguration deviceTransportConfiguration;
    private MqttPullTransportContext transportContext;
    private MqttClient mqttClient;
    private ScheduledFuture<?> reconnectTask;
    private volatile boolean brokerLinkActive;

    @Builder.Default
    private final Map<String, MqttPullTargetSession> activeTargets = new ConcurrentHashMap<>();

    public DeviceId getDeviceId() {
        return device.getId();
    }

    public void close() {
        if (reconnectTask != null) {
            reconnectTask.cancel(false);
            reconnectTask = null;
        }
        if (mqttClient != null) {
            try {
                mqttClient.disconnect();
            } catch (Exception ignored) {
            }
            mqttClient = null;
        }
        activeTargets.clear();
    }
}

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
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.mqtt.MqttClient;
import org.thingsboard.server.transport.mqtt.pull.MqttPullTransportContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    private SessionMsgListener rpcSessionListener;
    private ScheduledFuture<?> reconnectTask;
    private ScheduledFuture<?> activityHeartbeatTask;
    @Builder.Default
    private volatile boolean brokerLinkActive = false;
    @Builder.Default
    private volatile boolean destroyed = false;
    @Builder.Default
    private final Map<String, ConcurrentLinkedQueue<PendingMqttPullRpc>> pendingRpcByResponseTopic = new ConcurrentHashMap<>();
    @Builder.Default
    private final Set<String> rpcResponseSubscriptions = ConcurrentHashMap.newKeySet();

    public DeviceId getDeviceId() {
        return device.getId();
    }

    public void markDestroyed() {
        this.destroyed = true;
    }

    /**
     * Broker 断开或换新客户端后必须清空，否则会跳过对响应主题的重新订阅。
     */
    public void resetRpcState() {
        pendingRpcByResponseTopic.clear();
        rpcResponseSubscriptions.clear();
    }

    public void cancelActivityHeartbeat() {
        if (activityHeartbeatTask != null) {
            activityHeartbeatTask.cancel(false);
            activityHeartbeatTask = null;
        }
    }

    public void close() {
        markDestroyed();
        resetRpcState();
        if (reconnectTask != null) {
            reconnectTask.cancel(false);
            reconnectTask = null;
        }
        cancelActivityHeartbeat();
        if (mqttClient != null) {
            try {
                mqttClient.disconnect();
            } catch (Exception ignored) {
            }
            mqttClient = null;
        }
    }
}

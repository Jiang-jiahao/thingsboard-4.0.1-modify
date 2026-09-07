/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull.session;

import lombok.RequiredArgsConstructor;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.transport.SessionMsgListener;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.transport.http.pull.HttpPullRpcService;
import org.thingsboard.server.transport.http.pull.service.HttpPullProtoEntityService;

import java.util.UUID;

@RequiredArgsConstructor
public class HttpPullRpcSessionListener implements SessionMsgListener {

    private final HttpPullRpcService rpcService;
    private final HttpPullProtoEntityService protoEntityService;
    private final HttpPullCollectorSessionContext collectorCtx;
    private final DeviceId targetDeviceId;
    private final TransportProtos.SessionInfoProto sessionInfo;

    @Override
    public void onGetAttributesResponse(TransportProtos.GetAttributeResponseMsg getAttributesResponse) {
    }

    @Override
    public void onAttributeUpdate(UUID sessionId, TransportProtos.AttributeUpdateNotificationMsg attributeUpdateNotification) {
    }

    @Override
    public void onRemoteSessionCloseCommand(UUID sessionId, TransportProtos.SessionCloseNotificationProto sessionCloseNotification) {
    }

    @Override
    public void onToDeviceRpcRequest(UUID sessionId, TransportProtos.ToDeviceRpcRequestMsg toDeviceRequest) {
        Device targetDevice = resolveTargetDevice();
        HttpPullDeviceTransportConfiguration targetCfg = resolveTargetDeviceConfig(targetDevice);
        rpcService.onToDeviceRpcRequest(collectorCtx, targetDevice, targetCfg, sessionInfo, toDeviceRequest);
    }

    @Override
    public void onToServerRpcResponse(TransportProtos.ToServerRpcResponseMsg toServerResponse) {
    }

    @Override
    public void onDeviceDeleted(DeviceId deviceId) {
    }

    private Device resolveTargetDevice() {
        if (targetDeviceId == null || targetDeviceId.equals(collectorCtx.getDeviceId())) {
            return collectorCtx.getDevice();
        }
        return protoEntityService.getDeviceById(targetDeviceId);
    }

    private HttpPullDeviceTransportConfiguration resolveTargetDeviceConfig(Device device) {
        if (device == null || device.getDeviceData() == null
                || !(device.getDeviceData().getTransportConfiguration() instanceof HttpPullDeviceTransportConfiguration cfg)) {
            return new HttpPullDeviceTransportConfiguration();
        }
        return cfg;
    }
}

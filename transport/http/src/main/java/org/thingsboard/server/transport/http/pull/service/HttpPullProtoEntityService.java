/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.device.data.DeviceData;
import org.thingsboard.server.common.data.device.data.DeviceTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.DeviceProfileId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.security.DeviceCredentials;
import org.thingsboard.server.common.transport.TransportService;
import org.thingsboard.server.common.util.ProtoUtils;
import org.thingsboard.server.gen.transport.TransportProtos;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class HttpPullProtoEntityService {

    private final TransportService transportService;

    public Device getDeviceById(DeviceId id) {
        TransportProtos.GetDeviceResponseMsg deviceProto = transportService.getDevice(TransportProtos.GetDeviceRequestMsg.newBuilder()
                .setDeviceIdMSB(id.getId().getMostSignificantBits())
                .setDeviceIdLSB(id.getId().getLeastSignificantBits())
                .build());
        if (deviceProto == null || deviceProto.getDeviceProfileIdMSB() == 0 && deviceProto.getDeviceProfileIdLSB() == 0) {
            return null;
        }
        DeviceProfileId deviceProfileId = new DeviceProfileId(new UUID(
                deviceProto.getDeviceProfileIdMSB(), deviceProto.getDeviceProfileIdLSB()));
        Device device = new Device();
        device.setId(id);
        device.setDeviceProfileId(deviceProfileId);
        DeviceTransportConfiguration deviceTransportConfiguration = JacksonUtil.fromBytes(
                deviceProto.getDeviceTransportConfiguration().toByteArray(), DeviceTransportConfiguration.class);
        DeviceData deviceData = new DeviceData();
        deviceData.setTransportConfiguration(deviceTransportConfiguration);
        device.setDeviceData(deviceData);
        return device;
    }

    public DeviceCredentials getDeviceCredentialsByDeviceId(DeviceId deviceId) {
        TransportProtos.GetDeviceCredentialsResponseMsg response = transportService.getDeviceCredentials(
                TransportProtos.GetDeviceCredentialsRequestMsg.newBuilder()
                        .setDeviceIdMSB(deviceId.getId().getMostSignificantBits())
                        .setDeviceIdLSB(deviceId.getId().getLeastSignificantBits())
                        .build());
        if (response.hasDeviceCredentialsData()) {
            return ProtoUtils.fromProto(response.getDeviceCredentialsData());
        }
        throw new IllegalArgumentException("Device credentials not found for " + deviceId);
    }

    public TransportProtos.GetHttpPullDevicesResponseMsg getHttpPullDevicesIds(int page, int pageSize) {
        return transportService.getHttpPullDevicesIds(TransportProtos.GetHttpPullDevicesRequestMsg.newBuilder()
                .setPage(page)
                .setPageSize(pageSize)
                .build());
    }

    public TransportProtos.GetHttpPullRoutingTargetsResponseMsg getRoutingTargets(TenantId tenantId, DeviceProfileId profileId, int page, int pageSize) {
        return transportService.getHttpPullRoutingTargets(TransportProtos.GetHttpPullRoutingTargetsRequestMsg.newBuilder()
                .setTenantIdMSB(tenantId.getId().getMostSignificantBits())
                .setTenantIdLSB(tenantId.getId().getLeastSignificantBits())
                .setDeviceProfileIdMSB(profileId.getId().getMostSignificantBits())
                .setDeviceProfileIdLSB(profileId.getId().getLeastSignificantBits())
                .setPage(page)
                .setPageSize(pageSize)
                .build());
    }
}

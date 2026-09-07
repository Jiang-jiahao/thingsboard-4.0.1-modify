/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.thingsboard.server.transport.http.pull.session;

import lombok.Builder;
import lombok.Data;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.DeviceProfile;
import org.thingsboard.server.common.data.device.data.HttpPullDeviceTransportConfiguration;
import org.thingsboard.server.common.data.device.profile.HttpPullDeviceProfileTransportConfiguration;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.gen.transport.TransportProtos.SessionInfoProto;
import org.thingsboard.server.transport.http.pull.HttpPullTransportContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
public class HttpPullCollectorSessionContext {

    private TenantId tenantId;
    private Device device;
    private DeviceProfile deviceProfile;
    private String token;
    private SessionInfoProto sessionInfo;
    private HttpPullDeviceProfileTransportConfiguration profileTransportConfiguration;
    private HttpPullDeviceTransportConfiguration deviceTransportConfiguration;
    private HttpPullTransportContext transportContext;

    @Builder.Default
    private final List<ScheduledTask> queryingTasks = new ArrayList<>();

    /** 活跃目标设备：matchKey -> 目标会话 */
    @Builder.Default
    private final Map<String, HttpPullTargetSession> activeTargets = new ConcurrentHashMap<>();

    public DeviceId getDeviceId() {
        return device.getId();
    }

    public void close() {
        queryingTasks.forEach(ScheduledTask::cancel);
        queryingTasks.clear();
        activeTargets.clear();
    }
}

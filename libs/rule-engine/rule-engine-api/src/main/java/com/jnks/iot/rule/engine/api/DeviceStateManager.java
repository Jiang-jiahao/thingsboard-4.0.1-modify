package com.jnks.iot.rule.engine.api;

import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.JnksIotCallback;

public interface DeviceStateManager {

    void onDeviceConnect(TenantId tenantId, DeviceId deviceId, long connectTime, JnksIotCallback callback);

    void onDeviceActivity(TenantId tenantId, DeviceId deviceId, long activityTime, JnksIotCallback callback);

    void onDeviceDisconnect(TenantId tenantId, DeviceId deviceId, long disconnectTime, JnksIotCallback callback);

    void onDeviceInactivity(TenantId tenantId, DeviceId deviceId, long inactivityTime, JnksIotCallback callback);

    void onDeviceInactivityTimeoutUpdate(TenantId tenantId, DeviceId deviceId, long inactivityTimeout, JnksIotCallback callback);

}

package com.jnks.iot.rule.engine.api;

import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.TenantId;
import com.jnks.iot.server.common.msg.queue.TbCallback;

public interface DeviceStateManager {

    void onDeviceConnect(TenantId tenantId, DeviceId deviceId, long connectTime, TbCallback callback);

    void onDeviceActivity(TenantId tenantId, DeviceId deviceId, long activityTime, TbCallback callback);

    void onDeviceDisconnect(TenantId tenantId, DeviceId deviceId, long disconnectTime, TbCallback callback);

    void onDeviceInactivity(TenantId tenantId, DeviceId deviceId, long inactivityTime, TbCallback callback);

    void onDeviceInactivityTimeoutUpdate(TenantId tenantId, DeviceId deviceId, long inactivityTimeout, TbCallback callback);

}
